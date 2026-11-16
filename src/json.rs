//! JSON entry points reject duplicate keys, excessive depth, and numeric precision loss.
use anyhow::{Context, Result, bail, ensure};
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;
use std::io::Read;
use std::path::Path;

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

pub fn parse(bytes: &[u8]) -> Result<Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let Unique(value) = Unique::deserialize(&mut deserializer).context("invalid JSON")?;
    deserializer.end().context("trailing data after JSON")?;
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

#[derive(Debug, PartialEq)]
struct Decimal {
    negative: bool,
    digits: String,
    exponent: i64,
}

// Canonical exact decimal value: significant digits times a power of ten.
fn decimal(text: &str) -> Result<Decimal> {
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
    let digits: String = mantissa
        .chars()
        .filter(|character| *character != '.')
        .collect();
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Ok(Decimal {
            negative,
            digits: "0".into(),
            exponent: 0,
        });
    }
    let trimmed = significant.trim_end_matches('0');
    let trailing = i64::try_from(significant.len() - trimmed.len())?;
    let exponent = exponent
        .checked_sub(i64::try_from(fraction)?)
        .and_then(|exponent| exponent.checked_add(trailing))
        .context("number exponent exceeds the supported range")?;
    Ok(Decimal {
        negative,
        digits: trimmed.into(),
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

#[cfg(test)]
mod tests {
    use super::*;

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
            deadline_ms: 1000,
            operation: crate::ipc::Operation::Call {
                server: "server".into(),
                tool: "echo".into(),
                arguments: arguments.clone(),
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
                deadline_ms: 1000,
                operation: crate::ipc::Operation::Call {
                    server: "server".into(),
                    tool: "echo".into(),
                    arguments: Map::from_iter([("value".into(), value.clone())]),
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
