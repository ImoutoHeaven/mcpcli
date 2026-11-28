//! Bounded Streamable HTTP client transport.
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use futures_util::stream::BoxStream;
use http::{HeaderName, HeaderValue};
use rmcp::{
    model::{ClientJsonRpcMessage, ClientRequest, ErrorData, ServerJsonRpcMessage},
    transport::streamable_http_client::{
        AuthRequiredError, HttpStatusError, SseError, StreamableHttpClient, StreamableHttpError,
        StreamableHttpPostResponse,
    },
};
use sse_stream::Sse;
use tokio::time::timeout;

use super::{
    events::{self, EventSchemaCache},
    ingress::{self, Ingress, ObserveOutcome, UntrackOnDrop},
    sse,
    types::{
        CleanupState, FailureKind, IngressError, McpFailure, TransportTermination,
        encode_mcp_message, record_cleanup_failure, strip_modern_extensions_for_legacy_initialize,
    },
};
use crate::budget::{ByteBudget, ByteLease, Leased, format_bytes};

pub(super) const MAX_ERROR_BYTES: usize = 64 * 1024;

pub(super) struct OutgoingHttpBody {
    bytes: Leased<Vec<u8>>,
    _message: ClientJsonRpcMessage,
}

impl AsRef<[u8]> for OutgoingHttpBody {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

pub(super) fn has_media_type(content_type: &str, expected: &str) -> bool {
    let essence = content_type
        .split_once(';')
        .map_or(content_type, |(essence, _)| essence)
        .trim();
    essence.eq_ignore_ascii_case(expected)
}

#[derive(Clone)]
pub(super) struct BoundedHttp {
    pub(super) client: reqwest::Client,
    pub(super) auth_manager: Option<Arc<crate::auth::HttpAuthManager>>,
    pub(super) cleanup: CleanupState,
    pub(super) ingress: Ingress,
    pub(super) event_schemas: EventSchemaCache,
    pub(super) termination: Arc<TransportTermination>,
    pub(super) max_response_bytes: usize,
    pub(super) budget: Arc<ByteBudget>,
}
pub(super) type HttpError = StreamableHttpError<reqwest::Error>;

impl BoundedHttp {
    fn classify_auth_failure(
        &self,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        record_transient: bool,
    ) -> HttpError {
        let info = crate::auth::AuthChallengeInfo::from_response(status, headers);
        let summary = info.sanitized_summary();
        let required_scope =
            (!info.required_scopes.is_empty()).then(|| info.required_scopes.join(" "));
        let kind = if status == http::StatusCode::FORBIDDEN {
            FailureKind::InsufficientScope
        } else {
            FailureKind::AuthRequired
        };
        if record_transient {
            self.termination.set_transient_failure(McpFailure {
                kind,
                message: info.format_message(None),
                outcome_unknown: false,
                rpc_error: None,
                auth: Some(info),
                stderr_tail: None,
                lease: None,
            });
        }
        if status == http::StatusCode::FORBIDDEN {
            HttpError::InsufficientScope(
                rmcp::transport::streamable_http_client::InsufficientScopeError::new(
                    summary,
                    required_scope,
                ),
            )
        } else {
            HttpError::AuthRequired(AuthRequiredError::new(summary))
        }
    }
    pub(super) async fn bounded_body(
        &self,
        mut response: reqwest::Response,
        limit: usize,
    ) -> Result<(Vec<u8>, ByteLease), HttpError> {
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            let msg = format!("HTTP response exceeds the {} limit", format_bytes(limit));
            self.termination
                .set_transient(FailureKind::Transport, msg.clone());
            return Err(HttpError::ResponseBodyTooLarge { max_size: limit });
        }
        let mut lease = self.budget.empty_lease();
        let mut bytes = Vec::new();
        if let Some(length) = response.content_length()
            && !lease.reserve_vec(&mut bytes, length as usize, limit)
        {
            let msg = format!(
                "HTTP response exceeded the {} active memory limit",
                format_bytes(self.budget.limit())
            );
            self.termination
                .set_transient(FailureKind::Transport, msg.clone());
            return Err(HttpError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                msg,
            )));
        }
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| HttpError::Client(error.without_url()))?
        {
            let next_len = bytes.len().saturating_add(chunk.len());
            if next_len > limit {
                drop(bytes);
                lease.release();
                let msg = format!("HTTP response exceeds the {} limit", format_bytes(limit));
                self.termination.set_transient(FailureKind::Transport, msg);
                return Err(HttpError::ResponseBodyTooLarge { max_size: limit });
            }
            if !lease.reserve_vec(&mut bytes, next_len, limit) {
                drop(bytes);
                lease.release();
                let msg = format!(
                    "HTTP response exceeded the {} active memory limit",
                    format_bytes(self.budget.limit())
                );
                self.termination
                    .set_transient(FailureKind::Transport, msg.clone());
                return Err(HttpError::Io(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    msg,
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        lease.shrink_to(bytes.capacity());
        Ok((bytes, lease))
    }

    pub(super) async fn post(
        &self,
        uri: Arc<str>,
        mut message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        limit: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        if self.termination.is_terminated() {
            return Err(HttpError::TransportChannelClosed);
        }
        strip_modern_extensions_for_legacy_initialize(&mut message);
        self.ingress.track(&message);
        let active_limit = self.ingress.active_response_limit(limit);
        let mut passthrough_guard = match &message {
            ClientJsonRpcMessage::Request(request)
                if matches!(
                    request.request.method(),
                    "events/list" | "events/poll" | "events/stream"
                ) =>
            {
                Some(UntrackOnDrop::new(self.ingress.clone(), request.id.clone()))
            }
            _ => None,
        };
        let had_session = session_id.is_some();
        let notification = !matches!(&message, ClientJsonRpcMessage::Request(_));
        let is_events_stream = matches!(
            &message,
            ClientJsonRpcMessage::Request(request) if request.request.method() == "events/stream"
        );
        let event_headers = if let ClientJsonRpcMessage::Request(request) = &message
            && let ClientRequest::CustomRequest(custom) = &request.request
        {
            events::event_request_headers(
                &custom.method,
                custom.params.as_ref(),
                &self.event_schemas,
                &self.budget,
            )
        } else {
            Some(Leased::new(Vec::new(), self.budget.empty_lease()))
        }
        .ok_or_else(|| {
            let message = format!(
                "MCP event headers exceeded the {} active memory limit",
                format_bytes(self.budget.limit())
            );
            self.termination
                .terminate(FailureKind::Transport, message.clone());
            HttpError::Io(std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                message,
            ))
        })?;
        let discover_id = match &message {
            ClientJsonRpcMessage::Request(request)
                if !had_session && matches!(request.request, ClientRequest::DiscoverRequest(_)) =>
            {
                Some(request.id.clone())
            }
            _ => None,
        };
        let body = encode_mcp_message(
            &message,
            self.max_response_bytes,
            &self.budget,
            &self.termination,
        )
        .map_err(HttpError::Io)?;
        let body = bytes::Bytes::from_owner(OutgoingHttpBody {
            bytes: body,
            _message: message,
        });
        let (mut dynamic_token, attempted_refresh) = match (&self.auth_manager, &auth) {
            (Some(mgr), None) => mgr.prepare_token(&self.client).await,
            _ => (None, false),
        };
        let send_post = |bearer: Option<&str>| {
            let mut request = self
                .client
                .post(uri.as_ref())
                .header("accept", "application/json, text/event-stream");
            for (name, value) in &headers {
                request = request.header(name, value);
            }
            for (name, value) in event_headers.iter() {
                request = request.header(name, value);
            }
            if let Some(session) = &session_id {
                request = request.header("Mcp-Session-Id", session.as_ref());
            }
            if let Some(token) = auth.as_deref().or(bearer) {
                request = request.bearer_auth(token);
            }
            let built = request
                .body(body.clone())
                .build()
                .map_err(|error| HttpError::Client(error.without_url()));
            let client = self.client.clone();
            async move {
                let mut req = built?;
                req.headers_mut()
                    .entry(http::header::CONTENT_TYPE)
                    .or_insert(HeaderValue::from_static("application/json"));
                client
                    .execute(req)
                    .await
                    .map_err(|error| HttpError::Client(error.without_url()))
            }
        };
        let mut response = send_post(dynamic_token.as_deref()).await?;
        if response.status() == http::StatusCode::UNAUTHORIZED
            && !attempted_refresh
            && auth.is_none()
            && let Some(mgr) = &self.auth_manager
            && let Some(refreshed) = mgr
                .refresh_after_401(&self.client, dynamic_token.as_deref())
                .await
        {
            dynamic_token = Some(refreshed);
            response = send_post(dynamic_token.as_deref()).await?;
        }
        drop(event_headers);
        drop(body);
        let status = response.status();
        if status == http::StatusCode::UNAUTHORIZED || status == http::StatusCode::FORBIDDEN {
            return Err(self.classify_auth_failure(status, response.headers(), true));
        }
        if status == http::StatusCode::NOT_FOUND && had_session {
            self.termination.terminate(
                FailureKind::Transport,
                "MCP HTTP session expired; run reset before opening a new connection".into(),
            );
            return Err(HttpError::SessionExpired);
        }
        if status == http::StatusCode::ACCEPTED || status == http::StatusCode::NO_CONTENT {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status.is_success() && notification && response.content_length() == Some(0) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let session = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        if status.is_success() && has_media_type(&content_type, "text/event-stream") {
            if let Some(guard) = passthrough_guard.as_mut() {
                guard.disarm();
            }
            return Ok(StreamableHttpPostResponse::Sse(
                sse::bounded_stream(
                    response,
                    active_limit,
                    self.budget.clone(),
                    self.ingress.clone(),
                    self.termination.clone(),
                    is_events_stream,
                ),
                session,
            ));
        }
        let retry_after = response
            .headers()
            .get("retry-after")
            .filter(|v| v.as_bytes().len() <= 128)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let is_json = has_media_type(&content_type, "application/json");
        if !status.is_success() {
            // Legacy middleware may reject discover before JSON-RPC dispatch.
            // Correlate only this read-only probe, never an ordinary tool call.
            let discover_request_id = discover_id.filter(|_| {
                status.is_client_error() && status != http::StatusCode::TOO_MANY_REQUESTS
            });
            if status == http::StatusCode::TOO_MANY_REQUESTS
                || (discover_request_id.is_none() && !is_json)
            {
                return Err(HttpError::HttpStatus(HttpStatusError::new(
                    status.as_u16(),
                    retry_after,
                )));
            }
            let (body, lease) = match self
                .bounded_body(response, limit.min(MAX_ERROR_BYTES))
                .await
            {
                Ok(pair) => pair,
                Err(_) if discover_request_id.is_some() => {
                    self.termination.clear_transient();
                    (Vec::new(), self.budget.empty_lease())
                }
                Err(_) => {
                    return Err(HttpError::HttpStatus(HttpStatusError::new(
                        status.as_u16(),
                        retry_after,
                    )));
                }
            };
            let raw = Leased::new(body, lease);
            let observed = if is_json && !raw.is_empty() {
                Some(self.observe_response(raw, limit, true)?)
            } else {
                drop(raw);
                None
            };
            if let Some(request_id) = discover_request_id {
                if let Some(ObserveOutcome::Captured(message)) = observed
                    && let ServerJsonRpcMessage::Error(ref error) = **message
                    && error.id.as_ref() == Some(&request_id)
                {
                    return Ok(StreamableHttpPostResponse::Json(
                        ingress::into_sdk(message),
                        None,
                    ));
                }
                return Ok(StreamableHttpPostResponse::Json(
                    ServerJsonRpcMessage::error(
                        ErrorData::invalid_request("server/discover is unavailable", None),
                        Some(request_id),
                    ),
                    None,
                ));
            }
            if let Some(ObserveOutcome::Captured(message)) = observed
                && matches!(**message, ServerJsonRpcMessage::Error(_))
            {
                return Ok(StreamableHttpPostResponse::Json(
                    ingress::into_sdk(message),
                    session,
                ));
            }
            return Err(HttpError::HttpStatus(HttpStatusError::new(
                status.as_u16(),
                retry_after,
            )));
        }
        let (body, lease) = self.bounded_body(response, active_limit).await?;
        let raw = Leased::new(body, lease);
        if notification && raw.is_empty() {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !is_json {
            return Err(HttpError::UnexpectedContentType(None));
        }
        match self.observe_response(raw, active_limit, false)? {
            ObserveOutcome::Captured(message) => Ok(StreamableHttpPostResponse::Json(
                ingress::into_sdk(message),
                session,
            )),
            ObserveOutcome::NotMatched => Ok(StreamableHttpPostResponse::Accepted),
            ObserveOutcome::InvalidMessage(..) if notification => {
                Ok(StreamableHttpPostResponse::Accepted)
            }
            ObserveOutcome::InvalidMessage(..) => Err(HttpError::UnexpectedServerResponse(
                Cow::Borrowed("Invalid JSON-RPC response"),
            )),
        }
    }

    fn observe_response(
        &self,
        raw: Leased<Vec<u8>>,
        limit: usize,
        error_only: bool,
    ) -> Result<ObserveOutcome, HttpError> {
        self.ingress
            .observe_owned(raw, limit, &self.budget, error_only)
            .map_err(|error| {
                let (kind, io_kind, message) = match error {
                    IngressError::MemoryExhausted => (
                        FailureKind::Transport,
                        std::io::ErrorKind::OutOfMemory,
                        format!(
                            "HTTP response exceeded the {} active memory limit",
                            format_bytes(self.budget.limit())
                        ),
                    ),
                    IngressError::InvalidMetadata => (
                        FailureKind::Protocol,
                        std::io::ErrorKind::InvalidData,
                        "MCP response JSON cannot be represented losslessly".into(),
                    ),
                    IngressError::FrameTooLarge { limit } => (
                        FailureKind::Transport,
                        std::io::ErrorKind::InvalidData,
                        format!("HTTP response exceeds the {} limit", format_bytes(limit)),
                    ),
                };
                self.termination.terminate(kind, message.clone());
                HttpError::Io(std::io::Error::new(io_kind, message))
            })
    }
}

impl StreamableHttpClient for BoundedHttp {
    type Error = reqwest::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post(
            uri,
            message,
            session,
            auth,
            headers,
            self.max_response_bytes,
        )
        .await
    }

    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        limit: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post(
            uri,
            message,
            session,
            auth,
            headers,
            limit.min(self.max_response_bytes),
        )
        .await
    }

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session,
            last_id,
            auth,
            headers,
            self.max_response_bytes,
        )
        .await
    }

    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        last_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        limit: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        // Local SSE hard rejections terminate immediately without reconnecting via GET.
        if self.termination.is_terminated() {
            return Err(HttpError::ServerDoesNotSupportSse);
        }
        let had_session = session.is_some();
        let resumed = last_id.is_some();
        let (mut dynamic_token, attempted_refresh) = match (&self.auth_manager, &auth) {
            (Some(mgr), None) => mgr.prepare_token(&self.client).await,
            _ => (None, false),
        };
        let send_get = |bearer: Option<&str>| {
            let mut request = self
                .client
                .get(uri.as_ref())
                .header("accept", "text/event-stream");
            for (name, value) in &headers {
                request = request.header(name, value);
            }
            if let Some(session) = &session {
                request = request.header("Mcp-Session-Id", session.as_ref());
            }
            if let Some(last_id) = &last_id {
                request = request.header("Last-Event-Id", last_id.as_str());
            }
            if let Some(token) = auth.as_deref().or(bearer) {
                request = request.bearer_auth(token);
            }
            async move {
                request
                    .send()
                    .await
                    .map_err(|error| HttpError::Client(error.without_url()))
            }
        };
        let mut response = send_get(dynamic_token.as_deref()).await?;
        if response.status() == http::StatusCode::UNAUTHORIZED
            && !attempted_refresh
            && auth.is_none()
            && let Some(mgr) = &self.auth_manager
            && let Some(refreshed) = mgr
                .refresh_after_401(&self.client, dynamic_token.as_deref())
                .await
        {
            dynamic_token = Some(refreshed);
            response = send_get(dynamic_token.as_deref()).await?;
        }
        let status = response.status();
        if status == http::StatusCode::METHOD_NOT_ALLOWED {
            return Err(HttpError::ServerDoesNotSupportSse);
        }
        if matches!(
            status,
            http::StatusCode::UNAUTHORIZED | http::StatusCode::FORBIDDEN
        ) {
            return Err(self.classify_auth_failure(status, response.headers(), resumed));
        }
        if status == http::StatusCode::NOT_FOUND && had_session {
            self.termination.terminate(
                FailureKind::Transport,
                "MCP HTTP session expired; run reset before opening a new connection".into(),
            );
            return Err(HttpError::SessionExpired);
        }
        if !status.is_success() {
            return Err(HttpError::HttpStatus(HttpStatusError::new(
                status.as_u16(),
                None,
            )));
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !has_media_type(content_type, "text/event-stream") {
            return Err(HttpError::UnexpectedContentType(None));
        }
        Ok(sse::bounded_stream(
            response,
            limit.min(self.max_response_bytes),
            self.budget.clone(),
            self.ingress.clone(),
            self.termination.clone(),
            false,
        ))
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), HttpError> {
        let deletion = async {
            let dynamic_token = match (&self.auth_manager, &auth) {
                (Some(mgr), None) => mgr.prepare_token(&self.client).await.0,
                _ => None,
            };
            let mut request = self.client.delete(uri.as_ref());
            for (name, value) in headers {
                request = request.header(name, value);
            }
            request = request.header("Mcp-Session-Id", session.as_ref());
            if let Some(token) = auth.or(dynamic_token) {
                request = request.bearer_auth(token);
            }
            request.send().await
        };
        match timeout(Duration::from_secs(2), deletion).await {
            Ok(Ok(response)) => {
                let status = response.status();
                if status.is_success()
                    || matches!(
                        status,
                        http::StatusCode::NOT_FOUND | http::StatusCode::METHOD_NOT_ALLOWED
                    )
                {
                    // Legacy sessions may already be gone or disallow explicit deletion.
                    Ok(())
                } else {
                    record_cleanup_failure(
                        &self.cleanup,
                        FailureKind::Transport,
                        "Local MCP client stopped; remote session deletion was not confirmed",
                        true,
                    );
                    Err(HttpError::HttpStatus(HttpStatusError::new(
                        status.as_u16(),
                        None,
                    )))
                }
            }
            Ok(Err(_)) => {
                record_cleanup_failure(
                    &self.cleanup,
                    FailureKind::Transport,
                    "Local MCP client stopped; remote session deletion was not confirmed",
                    true,
                );
                Err(HttpError::Io(std::io::Error::other(
                    "Session deletion failed",
                )))
            }
            Err(_) => {
                record_cleanup_failure(
                    &self.cleanup,
                    FailureKind::Timeout,
                    "Local MCP client stopped; remote session deletion timed out",
                    true,
                );
                Err(HttpError::Io(std::io::Error::other(
                    "Session deletion timed out",
                )))
            }
        }
    }
}
