//! JSON entry points reject duplicate keys, excessive depth, and numeric precision loss.
use crate::budget::{ByteBudget, ByteLease};
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;
use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;

const MAX_DEPTH: usize = 64;

struct Unique(Value);

impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct JsonVisitor;
        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON with unique object keys")
            }
            fn visit_bool<E>(self, value: bool) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Bool(value)))
            }
            fn visit_i64<E>(self, value: i64) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Number(value.into())))
            }
            fn visit_u64<E>(self, value: u64) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Number(value.into())))
            }
            fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Unique, E> {
                Number::from_f64(value)
                    .map(|number| Unique(Value::Number(number)))
                    .ok_or_else(|| E::custom("non-finite number"))
            }
            fn visit_str<E>(self, value: &str) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::String(value.into())))
            }
            fn visit_string<E>(self, value: String) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::String(value)))
            }
            fn visit_unit<E>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_none<E>(self) -> std::result::Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut out = Vec::new();
                while let Some(Unique(value)) = sequence.next_element()? {
                    out.push(value);
                }
                Ok(Unique(Value::Array(out)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Unique, A::Error> {
                let mut out = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if out.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate JSON object key"));
                    }
                    out.insert(key, map.next_value::<Unique>()?.0);
                }
                Ok(Unique(Value::Object(out)))
            }
        }
        deserializer.deserialize_any(JsonVisitor)
    }
}

// serde_json uses one scratch Vec for escaped strings and, with float_roundtrip,
// mantissas that overflow u64. Keep its largest capacity through the entire parse.
pub fn scratch_bytes(bytes: &[u8]) -> usize {
    let mut index = 0;
    let mut longest = 0;
    while index < bytes.len() {
        if matches!(bytes[index], b'-' | b'0'..=b'9') {
            let start = index;
            while index < bytes.len()
                && matches!(bytes[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
            {
                index += 1;
            }
            let mut significand = Some(0u64);
            for &byte in bytes[start..index]
                .iter()
                .take_while(|&&byte| !matches!(byte, b'e' | b'E'))
            {
                if byte.is_ascii_digit() {
                    significand = significand.and_then(|value| {
                        value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))
                    });
                }
            }
            if significand.is_none() {
                longest = longest.max(index - start);
            }
            continue;
        }
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        index += 1;
        let start = index;
        let mut escaped = false;
        while index < bytes.len() && bytes[index] != b'"' {
            if bytes[index] == b'\\' {
                escaped = true;
                index = index.saturating_add(2).min(bytes.len());
            } else {
                index += 1;
            }
        }
        if escaped {
            longest = longest.max(index - start);
        }
        index += 1;
    }
    if longest == 0 {
        0
    } else {
        longest.saturating_mul(2).max(8)
    }
}

pub fn serialized_len(value: &impl serde::Serialize) -> serde_json::Result<usize> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("JSON length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

pub fn parse(bytes: &[u8]) -> Result<Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let Unique(value) = Unique::deserialize(&mut deserializer).context("invalid JSON")?;
    deserializer.end().context("trailing data after JSON")?;
    drop(deserializer);
    validate_tokens(bytes)?;
    Ok(value)
}

// Syntax is already validated. Inspect tokens outside strings without building another JSON tree.
fn validate_tokens(bytes: &[u8]) -> Result<()> {
    let mut index = 0;
    let mut depth = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'"' {
                    index += if bytes[index] == b'\\' { 2 } else { 1 };
                }
                index += 1;
            }
            b'[' | b'{' => {
                depth += 1;
                ensure!(
                    depth <= MAX_DEPTH,
                    "JSON nesting exceeds {MAX_DEPTH} containers"
                );
                index += 1;
            }
            b']' | b'}' => {
                depth -= 1;
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && matches!(bytes[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                {
                    index += 1;
                }
                let text = std::str::from_utf8(&bytes[start..index])?;
                if !text.contains(['.', 'e', 'E']) {
                    let in_range = if text.starts_with('-') {
                        text.parse::<i64>().is_ok()
                    } else {
                        text.parse::<u64>().is_ok()
                    };
                    ensure!(
                        in_range,
                        "integer is outside the supported i64/u64 range; use a string"
                    );
                }
                let number: Number = serde_json::from_str(text)?;
                ensure!(
                    decimal(text)? == decimal(&number.to_string())?,
                    "number would lose precision when forwarded; use a string"
                );
            }
            _ => index += 1,
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Decimal<'a> {
    negative: bool,
    digits: &'a str,
    exponent: i64,
}

impl PartialEq for Decimal<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.negative == other.negative
            && self.exponent == other.exponent
            && self
                .digits
                .bytes()
                .filter(|byte| *byte != b'.')
                .eq(other.digits.bytes().filter(|byte| *byte != b'.'))
    }
}

// Canonical decimal view borrows the significant digits, skipping the decimal
// point during comparison. Long finite decimal spellings need no String copies.
fn decimal(text: &str) -> Result<Decimal<'_>> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = if let Some(index) = unsigned.find(['e', 'E']) {
        (
            &unsigned[..index],
            unsigned[index + 1..]
                .parse::<i64>()
                .context("number exponent exceeds the supported range")?,
        )
    } else {
        (unsigned, 0)
    };
    let fraction = mantissa
        .find('.')
        .map_or(0, |index| mantissa.len() - index - 1);
    let Some(start) = mantissa
        .bytes()
        .position(|byte| matches!(byte, b'1'..=b'9'))
    else {
        return Ok(Decimal {
            negative,
            digits: "0",
            exponent: 0,
        });
    };
    let end = mantissa
        .bytes()
        .rposition(|byte| matches!(byte, b'1'..=b'9'))
        .expect("nonzero digit")
        + 1;
    let trailing = mantissa[end..].bytes().filter(u8::is_ascii_digit).count();
    let exponent = exponent
        .checked_sub(i64::try_from(fraction)?)
        .and_then(|exponent| exponent.checked_add(i64::try_from(trailing).ok()?))
        .context("number exponent exceeds the supported range")?;
    Ok(Decimal {
        negative,
        digits: &mantissa[start..end],
        exponent,
    })
}

pub fn read_limited(reader: impl Read, max: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    reader.take(max as u64 + 1).read_to_end(&mut out)?;
    if out.len() > max {
        bail!("input exceeds the {max} byte limit");
    }
    Ok(out)
}

pub fn read_file(path: &Path, max: usize) -> Result<Vec<u8>> {
    let file = crate::platform::open_input(path)
        .with_context(|| format!("cannot read {}", path.display()))?;
    if !file.metadata()?.is_file() {
        bail!("input must be a regular file");
    }
    read_limited(file, max)
}

struct LimitedVecWriter {
    buf: Vec<u8>,
    max: usize,
    prefix: usize,
    lease: ByteLease,
    abort: Option<io::Error>,
}

impl LimitedVecWriter {
    fn new(max: usize, budget: &Arc<ByteBudget>, prefix: usize) -> io::Result<Self> {
        let mut lease = budget.empty_lease();
        if !lease.try_grow(prefix) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "IPC frame exceeded active memory limit",
            ));
        }
        Ok(Self {
            buf: vec![0u8; prefix],
            max,
            prefix,
            lease,
            abort: None,
        })
    }

    fn finish(mut self) -> io::Result<(Vec<u8>, ByteLease)> {
        if let Some(error) = self.abort {
            return Err(error);
        }
        let payload_len = self.buf.len().saturating_sub(self.prefix);
        if payload_len == 0 || payload_len > self.max || payload_len > u32::MAX as usize {
            self.buf = Vec::new();
            self.lease.release();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
        }
        self.lease.shrink_to(self.buf.capacity());
        if self.prefix == 4 {
            self.buf[..4].copy_from_slice(&(payload_len as u32).to_le_bytes());
        }
        Ok((self.buf, self.lease))
    }
}

impl io::Write for LimitedVecWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(payload_len) = (self.buf.len() - self.prefix).checked_add(bytes.len()) else {
            self.buf = Vec::new();
            self.lease.release();
            self.abort = Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
        };
        if payload_len > self.max || payload_len > u32::MAX as usize {
            self.buf = Vec::new();
            self.lease.release();
            self.abort = Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IPC response exceeds frame limit",
            ));
        }
        let needed = self.prefix + payload_len;
        if !self
            .lease
            .reserve_vec(&mut self.buf, needed, self.max.saturating_add(self.prefix))
        {
            self.buf = Vec::new();
            self.lease.release();
            self.abort = Some(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "IPC frame exceeded active memory limit",
            ));
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "IPC frame exceeded active memory limit",
            ));
        }
        self.buf.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode_json_leased<T: Serialize>(
    value: &T,
    max: usize,
    budget: &Arc<ByteBudget>,
) -> io::Result<(Vec<u8>, ByteLease)> {
    encode_leased(value, max, budget, 0)
}

pub(crate) fn encode_leased<T: Serialize>(
    value: &T,
    max: usize,
    budget: &Arc<ByteBudget>,
    prefix: usize,
) -> io::Result<(Vec<u8>, ByteLease)> {
    let mut writer = LimitedVecWriter::new(max, budget, prefix)?;
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if let Some(abort) = writer.abort.take() {
            return Err(abort);
        }
        return Err(io::Error::other(error));
    }
    writer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_reservation_covers_escaped_tokens_and_survives_later_plain_tokens() {
        assert_eq!(scratch_bytes(br#"{"plain":"normal text"}"#), 0);
        assert_eq!(
            scratch_bytes(br#"{"x":"a\nb","later":"longer plain text"}"#),
            8
        );
        assert_eq!(scratch_bytes(br#"{"a\nb":"x","later":"text"}"#), 8);
        let escaped = "x".repeat(100) + "\n";
        let raw = serde_json::to_vec(&serde_json::json!([escaped, "z".repeat(300)])).unwrap();
        assert_eq!(scratch_bytes(&raw), 204);
    }

    #[test]
    fn numeric_workspace_covers_float_roundtrip_and_borrowed_canonicalization() {
        let number = "0.1000000000000000000000000000000000000000";
        assert!(scratch_bytes(number.as_bytes()) >= number.len());
        assert_eq!(parse(number.as_bytes()).unwrap(), serde_json::json!(0.1));
        assert_eq!(decimal(number).unwrap(), decimal("1e-1").unwrap());
        assert_eq!(scratch_bytes(b"123"), 0);
        assert_eq!(scratch_bytes(b"18446744073709551615"), 0);
        let view = decimal("123.456").unwrap();
        assert_eq!(view.digits.as_ptr(), "123.456".as_ptr());
        let decoded = serde_json::to_vec(&serde_json::json!({"text":"escaped\ntext"})).unwrap();
        assert!(scratch_bytes(&decoded) > 0);
    }

    #[test]
    fn rejects_duplicate_keys_at_any_depth() {
        for source in [
            r#"{"a":{"x":1,"x":2}}"#,
            r#"{"a":1,"a":2}"#,
            r#"[{"x":1,"x":2}]"#,
            r#"{"x":1,"\u0078":2}"#,
        ] {
            assert!(parse(source.as_bytes()).is_err(), "{source}");
        }
        assert_eq!(
            parse(br#"{"a":[null,true,-5,1.2,"x"]}"#).unwrap()["a"][2],
            -5
        );
    }

    #[test]
    fn accepts_supported_numbers_without_changing_decimal_value() {
        for source in [
            "0",
            "-0",
            "-0.0",
            "0.1",
            "1e3",
            "1E+3",
            "1000.00",
            "0.0012300",
            "18446744073709551615",
            "-9223372036854775808",
            "9007199254740993",
            "1e-300",
            "1e+300",
        ] {
            let parsed =
                parse(source.as_bytes()).unwrap_or_else(|error| panic!("{source}: {error:#}"));
            assert_eq!(
                decimal(source).unwrap(),
                decimal(&parsed.to_string()).unwrap(),
                "{source}"
            );
        }
    }

    #[test]
    fn refuses_overflow_underflow_and_precision_loss() {
        for source in [
            "18446744073709551616",
            "-9223372036854775809",
            "18446744073709551617001",
            "0.12345678901234567890123456789",
            "1.00000000000000001",
            "1e-400",
            "1e+400",
            "0e999999999999999999999999999999999999999",
        ] {
            assert!(parse(source.as_bytes()).is_err(), "{source}");
        }
        assert!(parse(br#"{"nested":[0.1234567890123456789012345]}"#).is_err());
    }

    #[test]
    fn strings_with_escapes_and_number_text_are_not_numeric_tokens() {
        let value = serde_json::json!({
            "18446744073709551616000": "1.000000000000000000001",
            "escaped": "\" \\\\ \\\" 1e99999999 [ { -0.1234567890123456789012345",
            "unicode": "中文 १२३ 1e999"
        });
        let bytes = serde_json::to_vec(&value).unwrap();
        assert_eq!(parse(&bytes).unwrap(), value);
    }

    #[test]
    fn preserves_all_object_keys_through_typed_ipc_and_sdk() {
        let source = r#"{"big":18446744073709551615,"decimal":0.1,"nested":[{"$serde_json::private::Number":"123"},{"$serde_json::private::RawValue":"[1,2]"}]}"#;
        let value = parse(source.as_bytes()).unwrap();
        let arguments = value.as_object().unwrap().clone();
        let request = crate::ipc::Request {
            protocol: crate::ipc::PROTOCOL,
            namespace: "test".into(),
            build: "test".into(),
            digest: "test".into(),
            timeout_ms: 1000,
            max_timeout_ms: None,
            deadline_ms: 1000,
            explicit_deadline_ms: None,
            operation: crate::ipc::Operation::Call {
                server: "server".into(),
                tool: "echo".into(),
                arguments: arguments.clone(),
                inline_media: false,
            },
        };
        let wire = serde_json::to_vec(&request).unwrap();
        let received: crate::ipc::Request = serde_json::from_slice(&wire).unwrap();
        let crate::ipc::Operation::Call {
            arguments: received_arguments,
            ..
        } = received.operation
        else {
            panic!("wrong operation")
        };
        assert_eq!(received_arguments, arguments);
        let params =
            rmcp::model::CallToolRequestParams::new("echo").with_arguments(received_arguments);
        let wire = serde_json::to_vec(&params).unwrap();
        let received: rmcp::model::CallToolRequestParams = serde_json::from_slice(&wire).unwrap();
        assert_eq!(received.arguments.unwrap(), arguments);
        assert_eq!(serde_json::to_string(&value).unwrap(), source);
    }

    #[test]
    fn equivalent_float_spellings_remain_stable_across_ipc_and_sdk() {
        for source in [
            "1.917236612140147400e-154",
            "-1.59235705930735720e+264",
            "2.20559579231966550e-150",
            "1.9172366121401474e-154",
            "-1.5923570593073572e+264",
            "2.2055957923196655e-150",
        ] {
            let value = parse(source.as_bytes()).unwrap();
            let request = crate::ipc::Request {
                protocol: crate::ipc::PROTOCOL,
                namespace: "test".into(),
                build: "test".into(),
                digest: "test".into(),
                timeout_ms: 1000,
                max_timeout_ms: None,
                deadline_ms: 1000,
                explicit_deadline_ms: None,
                operation: crate::ipc::Operation::Call {
                    server: "server".into(),
                    tool: "echo".into(),
                    arguments: Map::from_iter([("value".into(), value.clone())]),
                    inline_media: false,
                },
            };
            let received: crate::ipc::Request =
                serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
            let crate::ipc::Operation::Call { arguments, .. } = received.operation else {
                panic!("wrong operation")
            };
            assert_eq!(arguments["value"], value, "IPC drift for {source}");
            let params = rmcp::model::CallToolRequestParams::new("echo").with_arguments(arguments);
            let received: rmcp::model::CallToolRequestParams =
                serde_json::from_slice(&serde_json::to_vec(&params).unwrap()).unwrap();
            let received = &received.arguments.unwrap()["value"];
            assert_eq!(received, &value, "SDK drift for {source}");
            assert_eq!(
                decimal(source).unwrap(),
                decimal(&received.to_string()).unwrap()
            );
        }
    }

    #[test]
    fn caps_container_depth_including_empty_containers() {
        let allowed = format!("{}0{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(parse(allowed.as_bytes()).is_ok());
        let excessive = format!(
            "{}0{}",
            "[".repeat(MAX_DEPTH + 1),
            "]".repeat(MAX_DEPTH + 1)
        );
        assert!(parse(excessive.as_bytes()).is_err());
        let empty = format!("{}[]{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(parse(empty.as_bytes()).is_err());
        let hostile = format!("{}0{}", "[".repeat(10_000), "]".repeat(10_000));
        assert!(parse(hostile.as_bytes()).is_err());
    }

    #[test]
    fn rejects_trailing_data_and_malformed_json() {
        for source in [
            b"null true".as_slice(),
            br#"{"a":1} {"b":2}"#,
            br#"{"a":01}"#,
            br#"{"a":NaN}"#,
            br#"{"a":1,}"#,
            b"[1,]",
            b"",
            &[0xff],
        ] {
            assert!(parse(source).is_err());
        }
        assert_eq!(
            parse(b" \r\n [true, null, \"text\"] \t").unwrap(),
            serde_json::json!([true, null, "text"])
        );
    }

    #[test]
    fn normalizes_equivalent_decimal_spellings() {
        assert_eq!(decimal("1e3").unwrap(), decimal("1000.0").unwrap());
        assert_eq!(decimal("-12.3000e-2").unwrap(), decimal("-0.123").unwrap());
        assert_ne!(decimal("-0.0").unwrap(), decimal("0.0").unwrap());
        assert!(decimal("1.0e-9223372036854775808").is_err());
    }

    #[test]
    fn caps_input() {
        assert!(read_limited(&b"abcd"[..], 3).is_err());
        assert_eq!(read_limited(&b"abc"[..], 3).unwrap(), b"abc");
    }
}
