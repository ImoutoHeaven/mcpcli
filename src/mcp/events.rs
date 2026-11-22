//! MCP Events (SEP-3415) capability checks, notification routing, and HTTP routing headers.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Result;
use http::{HeaderName, HeaderValue};
use rmcp::{
    RoleClient,
    model::{
        ClientJsonRpcMessage, ClientRequest, CustomNotification, GetMeta, ProtocolVersion,
        RequestId, ServerJsonRpcMessage, ServerNotification, ServerPeerInfo,
    },
    service::RequestHandle,
    transport::Transport,
};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use super::{FailureKind, TransportTermination, failure, ingress::Ingress};
use crate::budget::{ByteBudget, Leased, format_bytes};

pub const EVENTS_EXTENSION_ID: &str = "io.modelcontextprotocol/events";
pub const MAX_EVENT_NOTIFICATION_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_EVENT_PAGES: usize = 100;
const EVENT_CHANNEL_CAPACITY: usize = 128;

const BASE64_HEADER_PREFIX: &str = "=?base64?";
const BASE64_HEADER_SUFFIX: &str = "?=";
const HEADER_MCP_NAME: &str = "Mcp-Name";
const HEADER_MCP_PARAM_PREFIX: &str = "Mcp-Param-";

pub(super) type EventSchemaCache = Arc<RwLock<HashMap<String, Map<String, Value>>>>;

#[derive(Clone, Debug)]
pub struct PollEventsParams {
    pub name: String,
    pub arguments: Option<Map<String, Value>>,
    pub cursor: Option<String>,
    pub max_age_ms: Option<u64>,
    pub max_events: Option<u64>,
}

impl PollEventsParams {
    pub(super) fn to_json(&self) -> Value {
        let mut map = Map::new();
        map.insert("name".into(), Value::String(self.name.clone()));
        if let Some(arguments) = &self.arguments {
            map.insert("arguments".into(), Value::Object(arguments.clone()));
        }
        if let Some(cursor) = &self.cursor {
            map.insert("cursor".into(), Value::String(cursor.clone()));
        }
        if let Some(max_age_ms) = self.max_age_ms {
            map.insert("maxAgeMs".into(), Value::Number(max_age_ms.into()));
        }
        if let Some(max_events) = self.max_events {
            map.insert("maxEvents".into(), Value::Number(max_events.into()));
        }
        Value::Object(map)
    }
}

#[derive(Clone, Debug)]
pub struct StreamEventsParams {
    pub name: String,
    pub arguments: Option<Map<String, Value>>,
    pub cursor: Option<String>,
    pub max_age_ms: Option<u64>,
    pub heartbeat_ms: Option<u64>,
}

impl StreamEventsParams {
    pub(super) fn to_json(&self) -> Value {
        let mut map = Map::new();
        map.insert("name".into(), Value::String(self.name.clone()));
        if let Some(arguments) = &self.arguments {
            map.insert("arguments".into(), Value::Object(arguments.clone()));
        }
        if let Some(cursor) = &self.cursor {
            map.insert("cursor".into(), Value::String(cursor.clone()));
        }
        if let Some(max_age_ms) = self.max_age_ms {
            map.insert("maxAgeMs".into(), Value::Number(max_age_ms.into()));
        }
        if let Some(heartbeat_ms) = self.heartbeat_ms {
            map.insert("heartbeatMs".into(), Value::Number(heartbeat_ms.into()));
        }
        Value::Object(map)
    }
}

#[derive(Debug)]
pub struct EventNotification {
    pub method: String,
    pub params: Option<Value>,
    pub subscription_id: RequestId,
}

type SharedRequestHandle = Arc<tokio::sync::Mutex<Option<RequestHandle<RoleClient>>>>;

struct SubscriptionSlot {
    sender: mpsc::Sender<Leased<EventNotification>>,
    pending_receiver: Option<mpsc::Receiver<Leased<EventNotification>>>,
    request_handle: Option<SharedRequestHandle>,
    terminated: bool,
}

#[derive(Clone)]
pub(super) struct EventRouter {
    slots: Arc<Mutex<HashMap<RequestId, SubscriptionSlot>>>,
    budget: Arc<ByteBudget>,
    max_notification_bytes: usize,
    channel_capacity: usize,
}

impl EventRouter {
    pub(super) fn new(budget: Arc<ByteBudget>) -> Self {
        Self::with_limits(budget, MAX_EVENT_NOTIFICATION_BYTES, EVENT_CHANNEL_CAPACITY)
    }

    fn with_limits(
        budget: Arc<ByteBudget>,
        max_notification_bytes: usize,
        channel_capacity: usize,
    ) -> Self {
        Self {
            slots: Arc::new(Mutex::new(HashMap::new())),
            budget,
            max_notification_bytes,
            channel_capacity,
        }
    }

    /// Pre-register before writing `events/stream` on the wire so early
    /// `notifications/events/active` frames are never dropped.
    pub(super) fn pre_register(&self, id: RequestId) {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        slots.entry(id).or_insert_with(|| {
            let (sender, receiver) = mpsc::channel(self.channel_capacity);
            SubscriptionSlot {
                sender,
                pending_receiver: Some(receiver),
                request_handle: None,
                terminated: false,
            }
        });
    }

    pub(super) fn take_receiver(&self, id: &RequestId) -> mpsc::Receiver<Leased<EventNotification>> {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(slot) = slots.get_mut(id)
            && let Some(receiver) = slot.pending_receiver.take()
        {
            return receiver;
        }
        let (sender, receiver) = mpsc::channel(self.channel_capacity);
        slots.insert(
            id.clone(),
            SubscriptionSlot {
                sender,
                pending_receiver: None,
                request_handle: None,
                terminated: false,
            },
        );
        receiver
    }

    pub(super) fn attach_handle(&self, id: &RequestId, handle: SharedRequestHandle) {
        let cancel_now = {
            let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(slot) = slots.get_mut(id) {
                if slot.terminated {
                    true
                } else {
                    slot.request_handle = Some(handle.clone());
                    false
                }
            } else {
                false
            }
        };
        if cancel_now {
            spawn_cancel_handle(handle, Some("stream terminated".into()));
        }
    }

    pub(super) fn unregister(&self, id: &RequestId) {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        slots.remove(id);
    }

    pub(super) fn route(
        &self,
        notification: CustomNotification,
        termination: &TransportTermination,
    ) {
        let Some(subscription_id) = notification.get_meta().subscription_id() else {
            return;
        };
        let params_bytes = match notification.params.as_ref() {
            Some(params) => match crate::json::serialized_len(params) {
                Ok(len) => len,
                Err(_) => return,
            },
            None => 0,
        };
        let payload_bytes = notification.method.len().saturating_add(params_bytes);
        if payload_bytes > self.max_notification_bytes {
            termination.terminate(
                FailureKind::Transport,
                format!(
                    "MCP event notification exceeds the {} limit",
                    format_bytes(self.max_notification_bytes)
                ),
            );
            return;
        }
        let Some(lease) = self.budget.try_acquire(payload_bytes) else {
            termination.terminate(
                FailureKind::Transport,
                format!(
                    "MCP event notification exceeded the {} active memory limit",
                    format_bytes(self.budget.limit())
                ),
            );
            return;
        };
        let is_terminated = notification.method == "notifications/events/terminated";
        let leased = Leased::new(
            EventNotification {
                method: notification.method,
                params: notification.params,
                subscription_id: subscription_id.clone(),
            },
            lease,
        );
        let (sender, handle_to_cancel) = {
            let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
            let Some(slot) = slots.get_mut(&subscription_id) else {
                return;
            };
            let handle = if is_terminated {
                slot.terminated = true;
                slot.request_handle.take()
            } else {
                None
            };
            (slot.sender.clone(), handle)
        };
        if let Err(error) = sender.try_send(leased) {
            drop(error);
            // Close the subscription so slow or dropped consumers reconnect from cursor.
            self.unregister(&subscription_id);
        }
        if let Some(handle) = handle_to_cancel {
            spawn_cancel_handle(handle, Some("stream terminated".into()));
        }
    }
}

fn spawn_cancel_handle(handle: SharedRequestHandle, reason: Option<String>) {
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
        rt.spawn(async move {
            let taken = handle.lock().await.take();
            if let Some(request_handle) = taken {
                let _ = request_handle.cancel(reason).await;
            }
        });
    }
}

pub struct EventStreamHandle {
    id: RequestId,
    handle: SharedRequestHandle,
    router: EventRouter,
    ingress: Ingress,
}

impl EventStreamHandle {
    pub(super) fn new(
        handle: RequestHandle<RoleClient>,
        router: EventRouter,
        ingress: Ingress,
    ) -> Self {
        let id = handle.id.clone();
        let shared = Arc::new(tokio::sync::Mutex::new(Some(handle)));
        router.attach_handle(&id, shared.clone());
        Self {
            id,
            handle: shared,
            router,
            ingress,
        }
    }

    pub fn id(&self) -> &RequestId {
        &self.id
    }

    pub async fn is_cancelled(&self) -> bool {
        self.handle.lock().await.is_none()
    }

    pub async fn cancel(&self) -> Result<()> {
        self.router.unregister(&self.id);
        self.ingress.untrack(&self.id);
        let taken = self.handle.lock().await.take();
        if let Some(handle) = taken {
            let _ = handle.cancel(Some("client cancelled".into())).await;
        }
        Ok(())
    }
}

impl Drop for EventStreamHandle {
    fn drop(&mut self) {
        self.router.unregister(&self.id);
        self.ingress.untrack(&self.id);
        spawn_cancel_handle(self.handle.clone(), None);
    }
}

pub struct EventStreamReceiver {
    buffered: Option<Leased<EventNotification>>,
    rx: mpsc::Receiver<Leased<EventNotification>>,
}

impl EventStreamReceiver {
    pub(super) fn new(
        buffered: Option<Leased<EventNotification>>,
        rx: mpsc::Receiver<Leased<EventNotification>>,
    ) -> Self {
        Self { buffered, rx }
    }

    pub async fn recv(&mut self) -> Option<Leased<EventNotification>> {
        if let Some(first) = self.buffered.take() {
            return Some(first);
        }
        self.rx.recv().await
    }

    pub fn try_recv(
        &mut self,
    ) -> std::result::Result<Leased<EventNotification>, mpsc::error::TryRecvError> {
        if let Some(first) = self.buffered.take() {
            return Ok(first);
        }
        self.rx.try_recv()
    }
}

/// Wraps an underlying MCP client transport to strip legacy `events` extensions
/// on `<= 2025-11-25` `initialize` requests and route `notifications/events/*`
/// to per-subscription bounded channels.
pub(super) struct EventNotificationTransport<T> {
    inner: T,
    router: EventRouter,
    termination: Arc<TransportTermination>,
}

impl<T> EventNotificationTransport<T> {
    pub(super) fn new(
        inner: T,
        router: EventRouter,
        termination: Arc<TransportTermination>,
    ) -> Self {
        Self {
            inner,
            router,
            termination,
        }
    }
}

impl<T> Transport<RoleClient> for EventNotificationTransport<T>
where
    T: Transport<RoleClient> + Send,
{
    type Error = T::Error;

    fn send(
        &mut self,
        mut item: ClientJsonRpcMessage,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send + 'static {
        strip_legacy_events_extension(&mut item);
        if let ClientJsonRpcMessage::Request(ref request) = item
            && request.request.method() == "events/stream"
        {
            self.router.pre_register(request.id.clone());
        }
        self.inner.send(item)
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        loop {
            let message = self.inner.receive().await?;
            if let ServerJsonRpcMessage::Notification(mut notif) = message {
                if let ServerNotification::CustomNotification(custom) = notif.notification {
                    if custom.method.starts_with("notifications/events/") {
                        self.router.route(custom, &self.termination);
                        continue;
                    }
                    notif.notification = ServerNotification::CustomNotification(custom);
                }
                return Some(ServerJsonRpcMessage::Notification(notif));
            }
            return Some(message);
        }
    }

    async fn close(&mut self) -> std::result::Result<(), Self::Error> {
        self.inner.close().await
    }
}

pub(super) fn strip_legacy_events_extension(message: &mut ClientJsonRpcMessage) {
    if let ClientJsonRpcMessage::Request(request) = message
        && let ClientRequest::InitializeRequest(init) = &mut request.request
        && init.params.protocol_version <= ProtocolVersion::V_2025_11_25
        && let Some(extensions) = init.params.capabilities.extensions.as_mut()
    {
        extensions.remove(EVENTS_EXTENSION_ID);
        if extensions.is_empty() {
            init.params.capabilities.extensions = None;
        }
    }
}

pub(super) fn check_events_capability(info: Option<&ServerPeerInfo>) -> Result<()> {
    let Some(info) = info else {
        return Err(failure(
            FailureKind::Protocol,
            "MCP server info is missing",
        ));
    };
    if info.protocol_version < ProtocolVersion::V_2026_07_28 {
        return Err(failure(
            FailureKind::Protocol,
            format!(
                "MCP server protocol version {} does not support events (requires >= 2026-07-28)",
                info.protocol_version
            ),
        ));
    }
    let Some(settings) = info
        .capabilities
        .extensions
        .as_ref()
        .and_then(|ext| ext.get(EVENTS_EXTENSION_ID))
    else {
        return Err(failure(
            FailureKind::Protocol,
            "MCP server does not advertise the io.modelcontextprotocol/events extension",
        ));
    };
    if let Some(revision) = settings.get("revision") {
        return Err(failure(
            FailureKind::Protocol,
            format!("MCP server advertises unsupported events extension revision: {revision}"),
        ));
    }
    Ok(())
}

// --- SEP-2243 HTTP routing headers (`Mcp-Name`, `Mcp-Param-*`) ---

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn requires_base64(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let bytes = value.as_bytes();
    if matches!(bytes.first(), Some(b' ' | b'\t')) || matches!(bytes.last(), Some(b' ' | b'\t')) {
        return true;
    }
    if value
        .chars()
        .any(|c| (c as u32) < 0x20 || (c as u32) > 0x7E)
    {
        return true;
    }
    value.starts_with(BASE64_HEADER_PREFIX) && value.ends_with(BASE64_HEADER_SUFFIX)
}

pub(super) fn encode_header_value(value: &str) -> String {
    if requires_base64(value) {
        format!(
            "{BASE64_HEADER_PREFIX}{}{BASE64_HEADER_SUFFIX}",
            base64_encode(value.as_bytes())
        )
    } else {
        value.to_owned()
    }
}

fn is_tchar(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(
            c,
            '!' | '#'
                | '$'
                | '%'
                | '&'
                | '\''
                | '*'
                | '+'
                | '-'
                | '.'
                | '^'
                | '_'
                | '`'
                | '|'
                | '~'
        )
}

struct PropertyAnnotation<'a> {
    path: Vec<&'a str>,
    schema: &'a Value,
    header: &'a Value,
}

fn collect_property_annotations<'a>(
    properties: Option<&'a Value>,
    path: &mut Vec<&'a str>,
    out: &mut Vec<PropertyAnnotation<'a>>,
) {
    let Some(Value::Object(properties)) = properties else {
        return;
    };
    for (name, schema) in properties {
        path.push(name);
        if let Some(header) = schema.get("x-mcp-header") {
            out.push(PropertyAnnotation {
                path: path.clone(),
                schema,
                header,
            });
        }
        collect_property_annotations(schema.get("properties"), path, out);
        path.pop();
    }
}

fn property_annotations(input_schema: &Map<String, Value>) -> Vec<PropertyAnnotation<'_>> {
    let mut out = Vec::new();
    collect_property_annotations(input_schema.get("properties"), &mut Vec::new(), &mut out);
    out
}

fn has_off_chain_annotation(schema: &Map<String, Value>, on_chain: bool) -> bool {
    schema.iter().any(|(key, value)| match key.as_str() {
        "x-mcp-header" => !on_chain,
        "properties" => schema_map_has_off_chain_annotation(value, on_chain),
        "$defs" | "definitions" | "dependentSchemas" | "patternProperties" => {
            schema_map_has_off_chain_annotation(value, false)
        }
        "default" | "const" | "enum" | "examples" => false,
        _ => subschema_has_off_chain_annotation(value, false),
    })
}

fn schema_map_has_off_chain_annotation(map: &Value, on_chain: bool) -> bool {
    map.as_object().is_some_and(|map| {
        map.values()
            .any(|schema| subschema_has_off_chain_annotation(schema, on_chain))
    })
}

fn subschema_has_off_chain_annotation(value: &Value, on_chain: bool) -> bool {
    match value {
        Value::Object(schema) => has_off_chain_annotation(schema, on_chain),
        Value::Array(items) => items
            .iter()
            .any(|item| subschema_has_off_chain_annotation(item, false)),
        _ => false,
    }
}

/// Validates SEP-2243 `x-mcp-header` annotations in an event `inputSchema`.
pub(super) fn validate_param_header_annotations(input_schema: &Map<String, Value>) -> Result<(), String> {
    if input_schema.contains_key("x-mcp-header") {
        return Err("x-mcp-header must annotate a property, not the schema root".into());
    }
    if has_off_chain_annotation(input_schema, true) {
        return Err("x-mcp-header must sit on a `properties`-only path from the root".into());
    }
    let mut seen = HashSet::new();
    for annotation in property_annotations(input_schema) {
        let prop = annotation.path.join(".");
        let Value::String(header) = annotation.header else {
            return Err(format!("property `{prop}`: x-mcp-header must be a string"));
        };
        if header.is_empty() {
            return Err(format!("property `{prop}`: x-mcp-header must not be empty"));
        }
        if !header.chars().all(is_tchar) {
            return Err(format!(
                "property `{prop}`: x-mcp-header `{header}` is not a valid HTTP token"
            ));
        }
        if !seen.insert(header.to_ascii_lowercase()) {
            return Err(format!(
                "property `{prop}`: duplicate x-mcp-header `{header}`"
            ));
        }
        match annotation.schema.get("type").and_then(Value::as_str) {
            Some("string" | "integer" | "boolean") => {}
            other => {
                return Err(format!(
                    "property `{prop}`: x-mcp-header requires primitive type, got {other:?}"
                ));
            }
        }
    }
    Ok(())
}

fn value_at<'a>(arguments: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(arguments, |value, key| value.get(key))
}

fn primitive_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Builds `Mcp-Name` and `Mcp-Param-*` headers for `events/poll` and `events/stream`.
pub(super) fn event_request_headers(
    method: &str,
    params: Option<&Value>,
    schemas: &EventSchemaCache,
) -> Vec<(HeaderName, HeaderValue)> {
    if !matches!(method, "events/poll" | "events/stream") {
        return Vec::new();
    }
    let Some(params) = params else {
        return Vec::new();
    };
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut push = |header_name: &str, value: &str| {
        if let (Ok(k), Ok(v)) = (
            HeaderName::from_bytes(header_name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            out.push((k, v));
        }
    };
    push(HEADER_MCP_NAME, &encode_header_value(name));

    if let Some(arguments) = params.get("arguments")
        && let Ok(guard) = schemas.read()
        && let Some(schema) = guard.get(name)
    {
        for annotation in property_annotations(schema) {
            let Value::String(header) = annotation.header else {
                continue;
            };
            if header.is_empty() {
                continue;
            }
            let Some(arg) = value_at(arguments, &annotation.path) else {
                continue;
            };
            if arg.is_null() {
                continue;
            }
            let Some(encoded) = primitive_to_string(arg).map(|s| encode_header_value(&s)) else {
                continue;
            };
            push(&format!("{HEADER_MCP_PARAM_PREFIX}{header}"), &encoded);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::{ClientCapabilities, ExtensionCapabilities, InitializeRequestParams, Implementation, ServerCapabilities};
    use serde_json::json;
    use std::collections::VecDeque;

    struct MockTransport {
        sent: Arc<Mutex<Vec<ClientJsonRpcMessage>>>,
        incoming: VecDeque<ServerJsonRpcMessage>,
    }

    impl Transport<RoleClient> for MockTransport {
        type Error = std::io::Error;

        fn send(
            &mut self,
            item: ClientJsonRpcMessage,
        ) -> impl Future<Output = std::io::Result<()>> + Send + 'static {
            let sent = self.sent.clone();
            async move {
                sent.lock().unwrap().push(item);
                Ok(())
            }
        }

        async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
            self.incoming.pop_front()
        }

        async fn close(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn capability_check_enforces_version_extension_and_no_unknown_revision() {
        let mut ok_ext = ExtensionCapabilities::new();
        ok_ext.insert(EVENTS_EXTENSION_ID.into(), Map::new());
        let mut ok_caps = ServerCapabilities::default();
        ok_caps.extensions = Some(ok_ext);
        let mut ok_info = ServerPeerInfo::from(rmcp::model::InitializeResult::new(ok_caps));
        ok_info.protocol_version = ProtocolVersion::V_2026_07_28;
        assert!(check_events_capability(Some(&ok_info)).is_ok());

        // Old protocol version rejected
        let mut old_info = ok_info.clone();
        old_info.protocol_version = ProtocolVersion::V_2025_11_25;
        assert!(check_events_capability(Some(&old_info)).is_err());

        // Missing extension rejected
        let mut no_ext_info = ok_info.clone();
        no_ext_info.capabilities.extensions = None;
        assert!(check_events_capability(Some(&no_ext_info)).is_err());

        // Unknown revision rejected
        let mut rev_ext = ExtensionCapabilities::new();
        let mut rev_settings = Map::new();
        rev_settings.insert("revision".into(), json!("2027-01-01"));
        rev_ext.insert(EVENTS_EXTENSION_ID.into(), rev_settings);
        let mut rev_info = ok_info;
        rev_info.capabilities.extensions = Some(rev_ext);
        assert!(check_events_capability(Some(&rev_info)).is_err());
    }

    #[test]
    fn strip_legacy_events_extension_only_for_legacy_initialize() {
        let make_init = |version: ProtocolVersion| {
            let mut ext = ExtensionCapabilities::new();
            ext.insert(EVENTS_EXTENSION_ID.into(), Map::new());
            let mut caps = ClientCapabilities::default();
            caps.extensions = Some(ext);
            let params = InitializeRequestParams::new(caps, Implementation::new("mcpcli", "0.4.0"))
                .with_protocol_version(version);
            ClientJsonRpcMessage::request(
                ClientRequest::InitializeRequest(rmcp::model::InitializeRequest::new(params)),
                RequestId::Number(1),
            )
        };

        let mut legacy = make_init(ProtocolVersion::V_2025_11_25);
        strip_legacy_events_extension(&mut legacy);
        let ClientJsonRpcMessage::Request(req) = legacy else {
            panic!("expected request");
        };
        let ClientRequest::InitializeRequest(init) = req.request else {
            panic!("expected initialize");
        };
        assert!(init.params.capabilities.extensions.is_none());

        let mut modern = make_init(ProtocolVersion::V_2026_07_28);
        strip_legacy_events_extension(&mut modern);
        let ClientJsonRpcMessage::Request(req) = modern else {
            panic!("expected request");
        };
        let ClientRequest::InitializeRequest(init) = req.request else {
            panic!("expected initialize");
        };
        assert!(
            init.params
                .capabilities
                .extensions
                .as_ref()
                .is_some_and(|e| e.contains_key(EVENTS_EXTENSION_ID))
        );
    }

    #[test]
    fn mcp_name_and_param_header_encoding_and_validation() {
        assert_eq!(encode_header_value("task.exited"), "task.exited");
        assert_eq!(
            encode_header_value(" leading"),
            "=?base64?IGxlYWRpbmc=?="
        );
        assert_eq!(
            encode_header_value("事件"),
            "=?base64?5LqL5Lu2?="
        );
        assert_eq!(
            encode_header_value("=?base64?abc?="),
            "=?base64?PT9iYXNlNjQ/YWJjPz0=?="
        );

        let valid_schema: Map<String, Value> = serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "repo": { "type": "string", "x-mcp-header": "Repo" },
                "nested": {
                    "type": "object",
                    "properties": {
                        "count": { "type": "integer", "x-mcp-header": "Count" },
                        "active": { "type": "boolean", "x-mcp-header": "Active" }
                    }
                }
            }
        }))
        .unwrap();
        assert!(validate_param_header_annotations(&valid_schema).is_ok());

        // Invalid annotations: root, duplicate (case-insensitive), non-primitive, bad token, off-chain
        for bad in [
            json!({ "type": "object", "x-mcp-header": "Root" }),
            json!({ "type": "object", "properties": { "a": { "type": "string", "x-mcp-header": "" } } }),
            json!({ "type": "object", "properties": { "a": { "type": "string", "x-mcp-header": "Bad Header" } } }),
            json!({ "type": "object", "properties": { "a": { "type": "object", "x-mcp-header": "Obj" } } }),
            json!({ "type": "object", "properties": {
                "a": { "type": "string", "x-mcp-header": "Dup" },
                "b": { "type": "string", "x-mcp-header": "dup" }
            }}),
            json!({ "type": "object", "properties": {
                "arr": { "type": "array", "items": { "type": "string", "x-mcp-header": "Item" } }
            }}),
        ] {
            let map = bad.as_object().unwrap().clone();
            assert!(validate_param_header_annotations(&map).is_err(), "expected invalid: {bad}");
        }

        let cache: EventSchemaCache = Arc::new(RwLock::new(HashMap::from([(
            "pr.merged".to_owned(),
            valid_schema,
        )])));
        let params = json!({
            "name": "pr.merged",
            "arguments": {
                "repo": "org/app",
                "nested": { "count": 42, "active": true }
            }
        });
        let headers: HashMap<String, String> = event_request_headers("events/stream", Some(&params), &cache)
            .into_iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap().to_owned()))
            .collect();
        assert_eq!(headers.get("mcp-name").map(String::as_str), Some("pr.merged"));
        assert_eq!(headers.get("mcp-param-repo").map(String::as_str), Some("org/app"));
        assert_eq!(headers.get("mcp-param-count").map(String::as_str), Some("42"));
        assert_eq!(headers.get("mcp-param-active").map(String::as_str), Some("true"));
    }

    #[tokio::test]
    async fn event_notification_transport_routes_by_typed_subscription_id_and_enforces_budget() {
        let budget = ByteBudget::new(64 * 1024);
        let router = EventRouter::with_limits(budget.clone(), 512, 8);
        let termination = Arc::new(TransportTermination::default());

        let num_id = RequestId::Number(1);
        let str_id = RequestId::String("1".into());
        let mut rx_num = router.take_receiver(&num_id);
        let mut rx_str = router.take_receiver(&str_id);

        let notif_num: ServerJsonRpcMessage = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "notifications/events/event",
            "params": {
                "eventId": "e1",
                "name": "task.exited",
                "data": {"ok": true},
                "_meta": { "io.modelcontextprotocol/subscriptionId": 1 }
            }
        }))
        .unwrap();
        let notif_str: ServerJsonRpcMessage = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "method": "notifications/events/event",
            "params": {
                "eventId": "e2",
                "name": "task.exited",
                "data": {"ok": false},
                "_meta": { "io.modelcontextprotocol/subscriptionId": "1" }
            }
        }))
        .unwrap();
        let normal_resp: ServerJsonRpcMessage = serde_json::from_value(json!({
            "jsonrpc": "2.0",
            "id": 9,
            "result": {}
        }))
        .unwrap();

        let mock = MockTransport {
            sent: Arc::new(Mutex::new(Vec::new())),
            incoming: VecDeque::from([notif_num, notif_str, normal_resp]),
        };
        let mut transport = EventNotificationTransport::new(mock, router.clone(), termination.clone());

        // receive() consumes both event notifications and returns the non-event response.
        let received = transport.receive().await.unwrap();
        assert!(matches!(received, ServerJsonRpcMessage::Response(_)));
        assert!(budget.used() > 0);

        let ev_num = rx_num.recv().await.unwrap();
        assert_eq!(ev_num.subscription_id, num_id);
        assert_eq!(ev_num.params.as_ref().unwrap()["eventId"], "e1");
        let ev_str = rx_str.recv().await.unwrap();
        assert_eq!(ev_str.subscription_id, str_id);
        assert_eq!(ev_str.params.as_ref().unwrap()["eventId"], "e2");

        drop(ev_num);
        drop(ev_str);
        assert_eq!(budget.used(), 0);
        assert!(!termination.is_terminated());

        // Oversized notification (> max_notification_bytes) terminates transport cleanly.
        let big: CustomNotification = serde_json::from_value(json!({
            "method": "notifications/events/event",
            "params": {
                "data": "x".repeat(600),
                "_meta": { "io.modelcontextprotocol/subscriptionId": 1 }
            }
        }))
        .unwrap();
        router.route(big, &termination);
        assert!(termination.is_terminated());
        assert_eq!(budget.used(), 0);
    }
}
