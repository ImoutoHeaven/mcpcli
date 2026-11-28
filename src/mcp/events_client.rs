//! Cloneable MCP Events client handle (list/poll/stream).
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use rmcp::{
    RoleClient,
    model::{ClientRequest, CustomRequest, CustomResult, ErrorCode, ServerResult},
    service::{Peer, PeerRequestOptions, ServiceError},
};
use serde_json::{Map, Value};
use tokio::time::timeout;

use super::{
    events::{
        self, EventNotification, EventRouter, EventSchemaCache, EventStreamHandle,
        EventStreamReceiver, MAX_EVENT_PAGES, PollEventsParams, StreamEventsParams,
    },
    ingress::{Ingress, UntrackOnDrop, service_failure},
    stdio::{StderrCapture, attach_stderr_tail},
    types::{FailureKind, McpFailure, McpLimits, TransportTermination, failure},
};
use crate::budget::Leased;

/// Cloneable handle for MCP Events operations that can run without holding `Mutex<ServerState>`.
#[derive(Clone)]
pub struct McpEventsClient {
    pub(super) peer: Peer<RoleClient>,
    pub(super) service_cancel:
        Arc<std::sync::Mutex<Option<rmcp::service::RunningServiceCancellationToken>>>,
    pub(super) ingress: Ingress,
    pub(super) event_router: EventRouter,
    pub(super) event_schemas: EventSchemaCache,
    pub(super) termination: Arc<TransportTermination>,
    pub(super) stderr_capture: Option<Arc<StderrCapture>>,
    pub(super) limits: McpLimits,
    pub(super) http: bool,
}

impl McpEventsClient {
    pub(super) fn service_failure(&self, error: ServiceError) -> anyhow::Error {
        let ServiceError::McpError(error) = error else {
            return service_failure(error, false, &self.ingress, &self.termination);
        };
        let Ok(bytes) = crate::json::serialized_len(&error) else {
            return failure(
                FailureKind::Protocol,
                "MCP event error could not be represented",
            );
        };
        let Some(mut lease) = self.limits.budget.try_acquire(bytes) else {
            return failure(
                FailureKind::Transport,
                "MCP event error exceeds the active memory limit",
            );
        };
        let code = error.code.0;
        let code_bytes = code.unsigned_abs().checked_ilog10().unwrap_or(0) as usize
            + 1
            + usize::from(code.is_negative());
        let message_bytes = error
            .message
            .len()
            .saturating_add(code_bytes + "MCP error : ".len());
        let mut message = String::new();
        if !lease.reserve_string(&mut message, message_bytes, self.limits.budget.limit()) {
            return failure(
                FailureKind::Transport,
                "MCP event error exceeds the active memory limit",
            );
        }
        use std::fmt::Write as _;
        write!(&mut message, "MCP error {code}: {}", error.message)
            .expect("formatting into a String cannot fail");
        let mut value = Map::new();
        value.insert("code".into(), Value::Number(code.into()));
        value.insert("message".into(), Value::String(error.message.into_owned()));
        if let Some(data) = error.data {
            value.insert("data".into(), data);
        }
        McpFailure {
            kind: FailureKind::Protocol,
            message,
            outcome_unknown: false,
            rpc_error: Some(Arc::new(Value::Object(value))),
            auth: None,
            stderr_tail: None,
            lease: Some(Arc::new(lease)),
        }
        .into()
    }

    pub(super) fn event_request(
        &self,
        method: &'static str,
        params: &impl serde::Serialize,
    ) -> Result<Leased<ClientRequest>> {
        let bytes = crate::json::serialized_len(params)?.saturating_add(method.len());
        let lease = self.limits.budget.try_acquire(bytes).ok_or_else(|| {
            failure(
                FailureKind::Transport,
                "MCP event request exceeds the active memory limit",
            )
        })?;
        let request = ClientRequest::CustomRequest(CustomRequest::new(
            method,
            Some(serde_json::to_value(params)?),
        ));
        Ok(Leased::new(request, lease))
    }

    fn abort_service(&self) {
        if let Some(cancel) = self
            .service_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            cancel.cancel();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.termination.is_closed() || self.peer.is_transport_closed()
    }

    async fn attach_stderr_tail(&self, error: anyhow::Error) -> anyhow::Error {
        attach_stderr_tail(error, self.stderr_capture.as_ref(), self.is_closed()).await
    }

    pub(crate) async fn closed_error(&self) -> anyhow::Error {
        self.attach_stderr_tail(self.termination.failure(false))
            .await
    }

    pub fn supports_events(&self) -> bool {
        self.check_events_support().is_ok()
    }

    pub fn check_events_support(&self) -> Result<()> {
        events::check_events_capability(self.peer.peer_info().as_deref())
    }

    pub async fn list_events(&self, budget: Duration) -> Result<Leased<Vec<Value>>> {
        self.check_events_support()?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        self.termination.clear_transient();
        let listing = async {
            let mut lease = self.limits.budget.empty_lease();
            let mut events = Vec::new();
            let mut schema_lease = self.limits.budget.empty_lease();
            let mut schemas = HashMap::new();
            let mut pagination_lease = self.limits.budget.empty_lease();
            let mut cursors = HashSet::new();
            let mut cursor: Option<String> = None;
            let mut cursor_bytes = 0;

            for _ in 0..MAX_EVENT_PAGES {
                let params = cursor.take().map(|c| {
                    let mut map = Map::new();
                    map.insert("cursor".into(), Value::String(c));
                    Value::Object(map)
                });
                let request =
                    ClientRequest::CustomRequest(CustomRequest::new("events/list", params));
                let mut handle = self
                    .peer
                    .send_cancellable_request(request, PeerRequestOptions::no_options())
                    .await
                    .map_err(|e| self.service_failure(e))?;
                pagination_lease.shrink_by(cursor_bytes);
                let mut page_lease = self.limits.budget.empty_lease();
                let mut guard = UntrackOnDrop::new(self.ingress.clone(), handle.id.clone());
                let result = tokio::select! {
                    biased;
                    rpc = &mut handle.rx => match rpc {
                        Ok(res) => res.map_err(|e| self.service_failure(e))?,
                        Err(_) => return Err(self.termination.failure(false)),
                    },
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        return Err(self.termination.failure(false));
                    }
                };
                guard.disarm();
                let ServerResult::CustomResult(CustomResult(mut page)) = result else {
                    return Err(failure(
                        FailureKind::Protocol,
                        "MCP server returned an unexpected events/list response",
                    ));
                };
                let page_bytes = crate::json::serialized_len(&page)?;
                if !page_lease.try_grow(page_bytes) {
                    return Err(failure(
                        FailureKind::Transport,
                        "Event catalog exceeds the active memory limit",
                    ));
                }
                let next_cursor = match page.get("nextCursor").and_then(Value::as_str) {
                    Some(next) => {
                        cursor_bytes = crate::json::serialized_len(&next)?;
                        if !pagination_lease.try_grow(cursor_bytes.saturating_mul(2)) {
                            return Err(failure(
                                FailureKind::Transport,
                                "Event catalog cursors exceed the active memory limit",
                            ));
                        }
                        Some(next.to_owned())
                    }
                    None => None,
                };
                let page_events = page
                    .get_mut("events")
                    .and_then(Value::as_array_mut)
                    .map(std::mem::take)
                    .unwrap_or_default();
                for event in page_events {
                    if self.http
                        && let Some(schema) = event.get("inputSchema").and_then(Value::as_object)
                    {
                        let event_name = event
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("<unnamed>");
                        if let Err(reason) = events::validate_param_header_annotations(schema) {
                            eprintln!(
                                "mcpcli: ignoring HTTP event {:?} with invalid x-mcp-header annotation: {}",
                                crate::discovery::safe_text(event_name),
                                crate::discovery::safe_text(&reason)
                            );
                            continue;
                        }
                        if let Some(name) = event.get("name").and_then(Value::as_str) {
                            let schema_bytes =
                                crate::json::serialized_len(schema)?.saturating_add(name.len());
                            if !schema_lease.try_grow(schema_bytes) {
                                return Err(failure(
                                    FailureKind::Transport,
                                    "Event schema cache exceeds the active memory limit",
                                ));
                            }
                            schemas.insert(name.to_owned(), schema.clone());
                        }
                    }
                    let item_bytes = crate::json::serialized_len(&event)?;
                    lease.merge(
                        page_lease
                            .split(item_bytes)
                            .expect("event belongs to catalog page"),
                    );
                    events.push(event);
                }
                match next_cursor {
                    None => {
                        if self.http
                            && let Ok(mut guard) = self.event_schemas.write()
                        {
                            *guard = Some(Leased::new(schemas, schema_lease));
                        }
                        return Ok(Leased::new(events, lease));
                    }
                    Some(next) if !cursors.insert(next.clone()) => {
                        return Err(failure(
                            FailureKind::Protocol,
                            "Event catalog repeated a pagination cursor",
                        ));
                    }
                    Some(next) => cursor = Some(next),
                }
            }
            Err(failure(
                FailureKind::Protocol,
                "Event catalog exceeds the 100-page pagination limit",
            ))
        };
        let result = timeout(budget, listing)
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Event discovery timed out"))?;
        match result {
            Ok(events) => Ok(events),
            Err(error) => Err(self.attach_stderr_tail(error).await),
        }
    }

    pub async fn poll_events(
        &self,
        params: PollEventsParams,
        budget: Duration,
    ) -> Result<Leased<Value>> {
        self.check_events_support()?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        let started = tokio::time::Instant::now();
        self.termination.clear_transient();
        for attempt in 0..2 {
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(failure(FailureKind::Timeout, "Event poll timed out"));
            }
            let (request, request_lease) = self.event_request("events/poll", &params)?.into_parts();
            let res = timeout(remaining, async {
                let mut handle = self
                    .peer
                    .send_cancellable_request(request, PeerRequestOptions::no_options())
                    .await?;
                drop(request_lease);
                let mut guard = UntrackOnDrop::new(self.ingress.clone(), handle.id.clone());
                let res = tokio::select! {
                    biased;
                    rpc = &mut handle.rx => match rpc {
                        Ok(res) => res,
                        Err(_) => Err(ServiceError::TransportClosed),
                    },
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        Err(ServiceError::TransportClosed)
                    }
                };
                guard.disarm();
                res
            })
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Event poll timed out"))?;
            match res {
                Ok(ServerResult::CustomResult(CustomResult(value))) => {
                    let bytes = crate::json::serialized_len(&value)?;
                    let lease = self.limits.budget.try_acquire(bytes).ok_or_else(|| {
                        failure(
                            FailureKind::Transport,
                            "Event poll response exceeds the active memory limit",
                        )
                    })?;
                    return Ok(Leased::new(value, lease));
                }
                Ok(_) => {
                    return Err(failure(
                        FailureKind::Protocol,
                        "MCP server returned an unexpected events/poll response",
                    ));
                }
                Err(ServiceError::McpError(ref err))
                    if attempt == 0 && err.code == ErrorCode::HEADER_MISMATCH =>
                {
                    let refresh_budget = budget.saturating_sub(started.elapsed());
                    let _ = self.list_events(refresh_budget).await?;
                    continue;
                }
                Err(error) => {
                    return Err(self.attach_stderr_tail(self.service_failure(error)).await);
                }
            }
        }
        unreachable!("poll_events retry loop always returns")
    }

    pub async fn open_event_stream(
        &self,
        params: StreamEventsParams,
        budget: Duration,
    ) -> Result<(EventStreamHandle, EventStreamReceiver)> {
        enum EitherStreamStart {
            Notification(Leased<EventNotification>),
            Rpc(
                Box<
                    std::result::Result<
                        std::result::Result<ServerResult, ServiceError>,
                        tokio::sync::oneshot::error::RecvError,
                    >,
                >,
            ),
        }
        self.check_events_support()?;
        if self.termination.is_terminated() {
            self.abort_service();
            return Err(self
                .attach_stderr_tail(self.termination.failure(false))
                .await);
        }
        let started = tokio::time::Instant::now();
        self.termination.clear_transient();
        for attempt in 0..2 {
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(failure(FailureKind::Timeout, "Event stream open timed out"));
            }
            let (request, request_lease) =
                self.event_request("events/stream", &params)?.into_parts();
            let mut handle = match timeout(
                remaining,
                self.peer
                    .send_cancellable_request(request, PeerRequestOptions::no_options()),
            )
            .await
            .map_err(|_| failure(FailureKind::Timeout, "Event stream open timed out"))?
            {
                Ok(handle) => handle,
                Err(e) => return Err(self.attach_stderr_tail(self.service_failure(e)).await),
            };
            drop(request_lease);
            let id = handle.id.clone();
            let (_, empty_response) = tokio::sync::oneshot::channel();
            let mut response = std::mem::replace(&mut handle.rx, empty_response);
            let mut rx = self.event_router.take_receiver(&id);
            // Own cancellation before awaiting the first notification or RPC response.
            let stream_handle =
                EventStreamHandle::new(handle, self.event_router.clone(), self.ingress.clone());
            let wait_budget = budget.saturating_sub(started.elapsed());
            let first_outcome = timeout(wait_budget, async {
                tokio::select! {
                    biased;
                    Some(notif) = rx.recv() => Ok(EitherStreamStart::Notification(notif)),
                    rpc = &mut response => Ok(EitherStreamStart::Rpc(Box::new(rpc))),
                    () = self.termination.cancelled() => {
                        self.abort_service();
                        Err(self.termination.failure(false))
                    }
                }
            })
            .await;

            match first_outcome {
                Err(_) => {
                    stream_handle.cancel().await?;
                    return Err(failure(
                        FailureKind::Timeout,
                        "Timed out waiting for event stream start",
                    ));
                }
                Ok(Err(err)) => return Err(self.attach_stderr_tail(err).await),
                Ok(Ok(EitherStreamStart::Notification(first))) => {
                    return Ok((stream_handle, EventStreamReceiver::new(Some(first), rx)));
                }
                Ok(Ok(EitherStreamStart::Rpc(rpc_res))) => match *rpc_res {
                    Ok(Err(ServiceError::McpError(ref err)))
                        if attempt == 0 && err.code == ErrorCode::HEADER_MISMATCH =>
                    {
                        drop(stream_handle);
                        let refresh_budget = budget.saturating_sub(started.elapsed());
                        let _ = self.list_events(refresh_budget).await?;
                        continue;
                    }
                    Ok(Err(err)) => {
                        return Err(self.attach_stderr_tail(self.service_failure(err)).await);
                    }
                    Ok(Ok(_)) => {
                        return Ok((stream_handle, EventStreamReceiver::new(None, rx)));
                    }
                    Err(_) => {
                        return Err(self
                            .attach_stderr_tail(self.termination.failure(false))
                            .await);
                    }
                },
            }
        }
        unreachable!("open_event_stream retry loop always returns")
    }
}
