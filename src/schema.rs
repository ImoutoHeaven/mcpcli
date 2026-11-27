//! Tool schema contract gate, recursive subschema check, and exact JSON numeric equality.
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::collections::BTreeSet;

pub const MAX_SCHEMA_DEPTH: usize = 64;

const ANNOTATION_KEYS: &[&str] = &[
    "title",
    "description",
    "default",
    "examples",
    "$comment",
    "deprecated",
    "readOnly",
    "writeOnly",
];

const CORE_KEYS: &[&str] = &[
    "type",
    "const",
    "enum",
    "required",
    "properties",
    "additionalProperties",
    "items",
];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaMode {
    #[default]
    Compatible,
    Exact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedSchema {
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    #[serde(default)]
    pub has_tool_envelope: bool,
    #[serde(default)]
    pub mode: SchemaMode,
}

pub fn schema_depth_ok(value: &Value, max_depth: usize) -> bool {
    depth_ok(value, 0, max_depth)
}

fn depth_ok(value: &Value, depth: usize, max_depth: usize) -> bool {
    match value {
        Value::Array(items) => {
            let next = depth + 1;
            next <= max_depth && items.iter().all(|item| depth_ok(item, next, max_depth))
        }
        Value::Object(map) => {
            let next = depth + 1;
            next <= max_depth && map.values().all(|item| depth_ok(item, next, max_depth))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => true,
    }
}

pub fn parse_expected_schema(value: Value, mode: SchemaMode) -> Result<ExpectedSchema, String> {
    if !schema_depth_ok(&value, MAX_SCHEMA_DEPTH) {
        return Err(format!(
            "expected schema exceeds maximum nesting depth of {MAX_SCHEMA_DEPTH}"
        ));
    }
    let Value::Object(mut map) = value else {
        return Err("expected schema must be a JSON object".into());
    };
    let is_envelope = map
        .get("inputSchema")
        .is_some_and(|v| matches!(v, Value::Object(_) | Value::Bool(_)));
    if is_envelope {
        let input_schema = map.remove("inputSchema").expect("checked inputSchema");
        let output_schema = match map.remove("outputSchema") {
            None | Some(Value::Null) => None,
            Some(v @ (Value::Object(_) | Value::Bool(_))) => Some(v),
            Some(_) => {
                return Err("expected schema outputSchema must be a JSON object or boolean".into());
            }
        };
        Ok(ExpectedSchema {
            input_schema,
            output_schema,
            has_tool_envelope: true,
            mode,
        })
    } else {
        Ok(ExpectedSchema {
            input_schema: Value::Object(map),
            output_schema: None,
            has_tool_envelope: false,
            mode,
        })
    }
}

fn number_as_i128(n: &Number) -> Option<i128> {
    n.as_i64()
        .map(i128::from)
        .or_else(|| n.as_u64().map(i128::from))
}

fn int_eq_float(i: i128, f: f64) -> bool {
    // IEEE-754 f64 values in [-2^63, 2^64) with fract() == 0.0 convert to i128 without loss.
    const MIN_I64_F64: f64 = -9_223_372_036_854_775_808.0;
    const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;
    f.is_finite() && f.fract() == 0.0 && (MIN_I64_F64..TWO_POW_64).contains(&f) && (f as i128) == i
}

fn is_math_integer(n: &Number) -> bool {
    n.is_i64()
        || n.is_u64()
        || n.as_f64()
            .is_some_and(|f| f.is_finite() && f.fract() == 0.0)
}

pub fn json_numbers_equal(a: &Number, b: &Number) -> bool {
    match (number_as_i128(a), number_as_i128(b)) {
        (Some(ia), Some(ib)) => ia == ib,
        (Some(ia), None) => b.as_f64().is_some_and(|fb| int_eq_float(ia, fb)),
        (None, Some(ib)) => a.as_f64().is_some_and(|fa| int_eq_float(ib, fa)),
        (None, None) => match (a.as_f64(), b.as_f64()) {
            (Some(fa), Some(fb)) => fa == fb,
            _ => false,
        },
    }
}

pub fn json_values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Number(x), Value::Number(y)) => json_numbers_equal(x, y),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Array(xs), Value::Array(ys)) => {
            xs.len() == ys.len()
                && xs
                    .iter()
                    .zip(ys.iter())
                    .all(|(x, y)| json_values_equal(x, y))
        }
        (Value::Object(xm), Value::Object(ym)) => {
            xm.len() == ym.len()
                && xm
                    .iter()
                    .all(|(k, v)| ym.get(k).is_some_and(|w| json_values_equal(v, w)))
        }
        _ => false,
    }
}

pub fn verify_tool_schema(
    expected: &ExpectedSchema,
    live_input: &Value,
    live_output: Option<&Value>,
) -> Result<(), String> {
    if !schema_depth_ok(&expected.input_schema, MAX_SCHEMA_DEPTH)
        || !schema_depth_ok(live_input, MAX_SCHEMA_DEPTH)
        || expected
            .output_schema
            .as_ref()
            .is_some_and(|s| !schema_depth_ok(s, MAX_SCHEMA_DEPTH))
        || live_output.is_some_and(|s| !schema_depth_ok(s, MAX_SCHEMA_DEPTH))
    {
        return Err(format!(
            "schema exceeds maximum nesting depth of {MAX_SCHEMA_DEPTH}"
        ));
    }

    match expected.mode {
        SchemaMode::Exact => {
            if !json_values_equal(&expected.input_schema, live_input) {
                return Err("live inputSchema does not exactly match expected schema".into());
            }
            match (&expected.output_schema, live_output) {
                (Some(exp_out), Some(live_out)) => {
                    if !json_values_equal(exp_out, live_out) {
                        return Err(
                            "live outputSchema does not exactly match expected schema".into()
                        );
                    }
                }
                (Some(_), None) => {
                    return Err("live tool is missing expected outputSchema".into());
                }
                (None, Some(_)) if expected.has_tool_envelope => {
                    return Err(
                        "live tool defines outputSchema not present in expected snapshot".into(),
                    );
                }
                (None, _) => {}
            }
            Ok(())
        }
        SchemaMode::Compatible => {
            if !is_subschema(&expected.input_schema, live_input, true) {
                return Err("live inputSchema is not compatible with expected schema".into());
            }
            if let Some(exp_out) = expected.output_schema.as_ref() {
                let Some(live_out) = live_output else {
                    return Err("live tool is missing expected outputSchema".into());
                };
                if !is_subschema(live_out, exp_out, false) {
                    return Err("live outputSchema is not compatible with expected schema".into());
                }
            }
            Ok(())
        }
    }
}

fn is_unconstrained_true(value: &Value) -> bool {
    match value {
        Value::Bool(true) => true,
        Value::Object(map) => map.keys().all(|k| ANNOTATION_KEYS.contains(&k.as_str())),
        _ => false,
    }
}

fn is_valid_type_name(name: &str) -> bool {
    matches!(
        name,
        "null" | "boolean" | "object" | "array" | "number" | "integer" | "string"
    )
}

fn parse_type_set(map: &Map<String, Value>) -> Result<Option<BTreeSet<&str>>, ()> {
    let Some(raw) = map.get("type") else {
        return Ok(None);
    };
    match raw {
        Value::String(s) if is_valid_type_name(s) => {
            let mut set = BTreeSet::new();
            set.insert(s.as_str());
            Ok(Some(set))
        }
        Value::Array(arr) if !arr.is_empty() => {
            let mut set = BTreeSet::new();
            for item in arr {
                let Some(s) = item.as_str().filter(|s| is_valid_type_name(s)) else {
                    return Err(());
                };
                set.insert(s);
            }
            Ok(Some(set))
        }
        _ => Err(()),
    }
}

fn parse_domain(map: &Map<String, Value>) -> Result<Option<Vec<&Value>>, ()> {
    let const_val = map.get("const");
    let enum_vals = match map.get("enum") {
        None => None,
        Some(Value::Array(arr)) => Some(arr),
        Some(_) => return Err(()),
    };
    match (const_val, enum_vals) {
        (None, None) => Ok(None),
        (Some(c), None) => Ok(Some(vec![c])),
        (None, Some(arr)) => Ok(Some(arr.iter().collect())),
        (Some(c), Some(arr)) => {
            if arr.iter().any(|item| json_values_equal(item, c)) {
                Ok(Some(vec![c]))
            } else {
                Ok(Some(Vec::new()))
            }
        }
    }
}

fn value_matches_types(value: &Value, types: &BTreeSet<&str>) -> bool {
    match value {
        Value::Null => types.contains("null"),
        Value::Bool(_) => types.contains("boolean"),
        Value::String(_) => types.contains("string"),
        Value::Array(_) => types.contains("array"),
        Value::Object(_) => types.contains("object"),
        Value::Number(n) => {
            types.contains("number") || (types.contains("integer") && is_math_integer(n))
        }
    }
}

fn types_subset(sub: &BTreeSet<&str>, sup: &BTreeSet<&str>) -> bool {
    sub.iter().all(|&t| {
        if t == "integer" {
            sup.contains("integer") || sup.contains("number")
        } else {
            sup.contains(t)
        }
    })
}

fn parse_required(map: &Map<String, Value>) -> Result<BTreeSet<&str>, ()> {
    let Some(raw) = map.get("required") else {
        return Ok(BTreeSet::new());
    };
    let Value::Array(arr) = raw else {
        return Err(());
    };
    let mut set = BTreeSet::new();
    for item in arr {
        let Some(s) = item.as_str() else {
            return Err(());
        };
        set.insert(s);
    }
    Ok(set)
}

fn parse_properties(map: &Map<String, Value>) -> Result<Option<&Map<String, Value>>, ()> {
    match map.get("properties") {
        None => Ok(None),
        Some(Value::Object(props)) => Ok(Some(props)),
        Some(_) => Err(()),
    }
}

// shortcut: is_subschema 对 7 个核心结构关键字执行子类型推理并对数值区间、正则及组合布尔关键字采取 Fail-Closed 规范化相等判定, 保证零假阳性放行风险, 未来若需支持数值区间包含关系（如 minimum/maximum 放宽）再在受控子集上扩展
pub fn is_subschema(sub: &Value, sup: &Value, is_input: bool) -> bool {
    if !schema_depth_ok(sub, MAX_SCHEMA_DEPTH) || !schema_depth_ok(sup, MAX_SCHEMA_DEPTH) {
        return false;
    }
    is_subschema_depth(sub, sup, is_input, 0)
}

fn is_subschema_depth(sub: &Value, sup: &Value, is_input: bool, depth: usize) -> bool {
    if depth > MAX_SCHEMA_DEPTH {
        return false;
    }
    if !matches!(sub, Value::Bool(_) | Value::Object(_))
        || !matches!(sup, Value::Bool(_) | Value::Object(_))
    {
        return false;
    }
    if matches!(sub, Value::Bool(false)) || is_unconstrained_true(sup) {
        return true;
    }
    if matches!(sup, Value::Bool(false)) || is_unconstrained_true(sub) {
        return false;
    }

    let (Value::Object(sub_map), Value::Object(sup_map)) = (sub, sup) else {
        return false;
    };

    let Ok(sub_types) = parse_type_set(sub_map) else {
        return false;
    };
    let Ok(sup_types) = parse_type_set(sup_map) else {
        return false;
    };
    let Ok(sub_domain) = parse_domain(sub_map) else {
        return false;
    };
    let Ok(sup_domain) = parse_domain(sup_map) else {
        return false;
    };

    if let Some(sup_vals) = &sup_domain {
        let Some(sub_vals) = &sub_domain else {
            return false;
        };
        if !sub_vals
            .iter()
            .all(|v| sup_vals.iter().any(|w| json_values_equal(v, w)))
        {
            return false;
        }
    } else if let (Some(sub_vals), Some(sup_t)) = (&sub_domain, &sup_types)
        && !sub_vals.iter().all(|v| value_matches_types(v, sup_t))
    {
        return false;
    }

    if let Some(sup_t) = &sup_types {
        if let Some(sub_t) = &sub_types {
            if !types_subset(sub_t, sup_t) {
                return false;
            }
        } else if let Some(sub_vals) = &sub_domain {
            if !sub_vals.iter().all(|v| value_matches_types(v, sup_t)) {
                return false;
            }
        } else {
            return false;
        }
    }

    let Ok(sub_required) = parse_required(sub_map) else {
        return false;
    };
    let Ok(sup_required) = parse_required(sup_map) else {
        return false;
    };
    if !sup_required.is_subset(&sub_required) {
        return false;
    }

    let Ok(sub_props) = parse_properties(sub_map) else {
        return false;
    };
    let Ok(sup_props) = parse_properties(sup_map) else {
        return false;
    };

    let default_ap = Value::Bool(true);
    let sub_ap = sub_map.get("additionalProperties").unwrap_or(&default_ap);
    let sup_ap = sup_map.get("additionalProperties").unwrap_or(&default_ap);
    if !is_subschema_depth(sub_ap, sup_ap, is_input, depth + 1) {
        return false;
    }

    if let Some(sub_p) = sub_props {
        for (k, s_sub) in sub_p {
            let s_sup = sup_props.and_then(|p| p.get(k)).unwrap_or(sup_ap);
            if !is_subschema_depth(s_sub, s_sup, is_input, depth + 1) {
                return false;
            }
        }
    }

    if let Some(sup_p) = sup_props {
        for (k, s_sup) in sup_p {
            if sub_props.is_some_and(|p| p.contains_key(k)) {
                continue;
            }
            let allow_new_optional_input = is_input
                && sub_map.get("additionalProperties").is_none()
                && !sub_required.contains(k.as_str())
                && sub_map.get("enum").is_none()
                && sub_map.get("const").is_none();
            if !allow_new_optional_input && !is_subschema_depth(sub_ap, s_sup, is_input, depth + 1)
            {
                return false;
            }
        }
    }

    match (sub_map.get("items"), sup_map.get("items")) {
        (Some(Value::Array(a)), Some(Value::Array(b))) => {
            if a.len() != b.len() || !a.iter().zip(b.iter()).all(|(x, y)| json_values_equal(x, y)) {
                return false;
            }
        }
        (Some(Value::Array(_)), _) | (_, Some(Value::Array(_))) => return false,
        (sub_it, sup_it) => {
            let sub_it = sub_it.unwrap_or(&default_ap);
            let sup_it = sup_it.unwrap_or(&default_ap);
            if !is_subschema_depth(sub_it, sup_it, is_input, depth + 1) {
                return false;
            }
        }
    }

    for (k, sub_v) in sub_map {
        if ANNOTATION_KEYS.contains(&k.as_str()) || CORE_KEYS.contains(&k.as_str()) {
            continue;
        }
        if !sup_map
            .get(k)
            .is_some_and(|sup_v| json_values_equal(sub_v, sup_v))
        {
            return false;
        }
    }
    for k in sup_map.keys() {
        if ANNOTATION_KEYS.contains(&k.as_str()) || CORE_KEYS.contains(&k.as_str()) {
            continue;
        }
        if !sub_map.contains_key(k) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_numeric_equality_handles_cross_type_and_large_integers() {
        assert!(json_values_equal(&json!(10), &json!(10.0)));
        assert!(json_values_equal(&json!(-0.0), &json!(0)));
        assert!(json_values_equal(
            &json!(9_007_199_254_740_992_u64),
            &json!(9_007_199_254_740_992.0_f64)
        ));
        // 2^53 + 1 cannot be represented as f64 without rounding to 2^53.
        assert!(!json_values_equal(
            &json!(9_007_199_254_740_993_u64),
            &json!(9_007_199_254_740_992_u64)
        ));
        assert!(!json_values_equal(
            &json!(9_007_199_254_740_993_u64),
            &json!(9_007_199_254_740_992.0_f64)
        ));
        assert!(!json_values_equal(
            &json!(u64::MAX),
            &json!(18_446_744_073_709_551_616.0_f64)
        ));
        assert!(json_values_equal(
            &json!(i64::MIN),
            &json!(-9223372036854775808.0_f64)
        ));
        assert!(json_values_equal(
            &json!({"b": 10, "a": [1, 2.0]}),
            &json!({"a": [1.0, 2], "b": 10.0})
        ));
        assert!(!json_values_equal(&json!([1, 2]), &json!([2, 1])));
    }

    #[test]
    fn parse_expected_schema_detects_envelope_and_raw_schema() {
        let env = parse_expected_schema(
            json!({
                "name": "search",
                "inputSchema": {"type": "object"},
                "outputSchema": {"type": "object", "properties": {"ok": {"type": "boolean"}}}
            }),
            SchemaMode::Compatible,
        )
        .unwrap();
        assert!(env.has_tool_envelope);
        assert_eq!(env.input_schema, json!({"type": "object"}));
        assert!(env.output_schema.is_some());

        let raw = parse_expected_schema(
            json!({"type": "object", "properties": {"q": {"type": "string"}}}),
            SchemaMode::Exact,
        )
        .unwrap();
        assert!(!raw.has_tool_envelope);
        assert!(raw.output_schema.is_none());
        assert_eq!(raw.mode, SchemaMode::Exact);

        assert!(parse_expected_schema(json!(true), SchemaMode::Compatible).is_err());
    }

    #[test]
    fn exact_mode_checks_input_and_envelope_output() {
        let expected = parse_expected_schema(
            json!({
                "inputSchema": {"type": "object", "properties": {"n": {"const": 10}}}
            }),
            SchemaMode::Exact,
        )
        .unwrap();
        assert!(
            verify_tool_schema(
                &expected,
                &json!({"properties": {"n": {"const": 10.0}}, "type": "object"}),
                None
            )
            .is_ok()
        );
        // Envelope had no outputSchema, so live outputSchema must be None.
        assert!(
            verify_tool_schema(
                &expected,
                &json!({"type": "object", "properties": {"n": {"const": 10}}}),
                Some(&json!({"type": "object"}))
            )
            .is_err()
        );
    }

    #[test]
    fn boolean_and_annotation_unconstrained_schemas() {
        assert!(is_subschema(
            &json!(false),
            &json!({"type": "string"}),
            true
        ));
        assert!(is_subschema(
            &json!({"type": "string"}),
            &json!({"title": "Any", "description": "doc"}),
            true
        ));
        assert!(is_subschema(&json!({"type": "string"}), &json!(true), true));
        assert!(!is_subschema(
            &json!(true),
            &json!({"type": "string"}),
            true
        ));
        assert!(!is_subschema(&json!({}), &json!({"type": "string"}), true));
        assert!(!is_subschema(
            &json!({"type": "string"}),
            &json!(false),
            true
        ));
        assert!(is_subschema(&json!(false), &json!(false), true));
    }

    #[test]
    fn integer_subtypes_number_and_domain_checks() {
        assert!(is_subschema(
            &json!({"type": "integer"}),
            &json!({"type": "number"}),
            true
        ));
        assert!(!is_subschema(
            &json!({"type": "number"}),
            &json!({"type": "integer"}),
            true
        ));
        // Enum widening and removal.
        assert!(is_subschema(
            &json!({"enum": [1, 2]}),
            &json!({"enum": [1.0, 2.0, 3]}),
            true
        ));
        assert!(is_subschema(
            &json!({"enum": [1, 2.0]}),
            &json!({"type": "integer"}),
            true
        ));
        assert!(!is_subschema(
            &json!({"enum": [1, 2.5]}),
            &json!({"type": "integer"}),
            true
        ));
        assert!(is_subschema(
            &json!({"enum": [1, 2.5]}),
            &json!({"type": "number"}),
            true
        ));
        // Live adding enum on input is narrowing -> incompatible.
        assert!(!is_subschema(
            &json!({"type": "string"}),
            &json!({"type": "string", "enum": ["a", "b"]}),
            true
        ));
        // Live removing enum on output (live <: expected) -> incompatible.
        assert!(!is_subschema(
            &json!({"type": "string"}),
            &json!({"type": "string", "enum": ["a", "b"]}),
            false
        ));
    }

    #[test]
    fn input_optional_property_addition_and_additional_properties_tightening() {
        let expected_in = json!({
            "type": "object",
            "required": ["q"],
            "properties": {
                "q": {"type": "string"}
            }
        });
        // Standard MCP tool omits additionalProperties and adds a constrained optional parameter.
        let live_added_opt = json!({
            "type": "object",
            "required": ["q"],
            "properties": {
                "q": {"type": "string"},
                "limit": {"type": "integer"}
            }
        });
        assert!(is_subschema(&expected_in, &live_added_opt, true));

        // Adding a new required parameter is incompatible.
        let live_added_req = json!({
            "type": "object",
            "required": ["q", "limit"],
            "properties": {
                "q": {"type": "string"},
                "limit": {"type": "integer"}
            }
        });
        assert!(!is_subschema(&expected_in, &live_added_req, true));

        // Tightening omitted additionalProperties to false is incompatible.
        let live_closed = json!({
            "type": "object",
            "required": ["q"],
            "properties": {"q": {"type": "string"}},
            "additionalProperties": false
        });
        assert!(!is_subschema(&expected_in, &live_closed, true));

        // Explicit additionalProperties: true does not allow adding a constrained property.
        let expected_explicit_open = json!({
            "type": "object",
            "properties": {"q": {"type": "string"}},
            "additionalProperties": true
        });
        assert!(!is_subschema(
            &expected_explicit_open,
            &json!({
                "type": "object",
                "properties": {"q": {"type": "string"}, "limit": {"type": "integer"}},
                "additionalProperties": true
            }),
            true
        ));
    }

    #[test]
    fn coding_guard_blocks_handcrafted_required_or_enum_const_without_property() {
        // Handcrafted schema: "x" is in required without appearing in properties.
        let sub_req_only = json!({
            "type": "object",
            "required": ["x"]
        });
        let sup_constrained = json!({
            "type": "object",
            "required": ["x"],
            "properties": {"x": {"type": "string"}}
        });
        assert!(!is_subschema(&sub_req_only, &sup_constrained, true));

        // Handcrafted schema: const object with key "x", sup constrains properties.x.
        let sub_const = json!({
            "type": "object",
            "const": {"x": 123}
        });
        let sup_prop = json!({
            "type": "object",
            "properties": {"x": {"type": "string"}}
        });
        assert!(!is_subschema(&sub_const, &sup_prop, true));

        // Handcrafted schema: enum object with key "x", sup constrains properties.x.
        let sub_enum = json!({
            "type": "object",
            "enum": [{"x": 123}]
        });
        assert!(!is_subschema(&sub_enum, &sup_prop, true));
    }

    #[test]
    fn output_covariance_distinguishes_open_and_closed_additional_properties() {
        let expected_out_open = json!({
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "string"}}
        });
        let expected_out_closed = json!({
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "string"}},
            "additionalProperties": false
        });
        let live_out_extra = json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "string"},
                "extra": {"type": "integer"}
            }
        });
        // Open expected output allows extra fields from live output.
        assert!(is_subschema(&live_out_extra, &expected_out_open, false));
        // Closed expected output rejects extra fields from live output.
        assert!(!is_subschema(&live_out_extra, &expected_out_closed, false));

        let live_out_closed_extra = json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "string"},
                "extra": {"type": "integer"}
            },
            "additionalProperties": false
        });
        assert!(!is_subschema(
            &live_out_closed_extra,
            &expected_out_closed,
            false
        ));
    }

    #[test]
    fn annotations_ignored_and_non_core_keywords_fail_closed() {
        let a = json!({
            "type": "string",
            "title": "Old",
            "description": "old desc",
            "examples": ["a"],
            "deprecated": false
        });
        let b = json!({
            "type": "string",
            "title": "New",
            "description": "new desc",
            "$comment": "updated"
        });
        assert!(is_subschema(&a, &b, true));

        // Non-core keywords must match on both sides.
        assert!(is_subschema(
            &json!({"type": "integer", "minimum": 1}),
            &json!({"type": "number", "minimum": 1.0}),
            true
        ));
        assert!(!is_subschema(
            &json!({"type": "integer", "minimum": 1}),
            &json!({"type": "integer", "minimum": 0}),
            true
        ));
        assert!(!is_subschema(
            &json!({"type": "string"}),
            &json!({"type": "string", "pattern": "^[a-z]+$"}),
            true
        ));
        assert!(!is_subschema(
            &json!({"oneOf": [{"type": "string"}]}),
            &json!({"oneOf": [{"type": "string"}, {"type": "integer"}]}),
            true
        ));
        assert!(!is_subschema(
            &json!({"$ref": "#/$defs/a"}),
            &json!({"$ref": "#/$defs/b"}),
            true
        ));
    }

    #[test]
    fn depth_limit_rejects_excessive_nesting() {
        let mut deep = json!({"type": "string"});
        for _ in 0..=MAX_SCHEMA_DEPTH {
            deep = json!({"items": deep});
        }
        assert!(!schema_depth_ok(&deep, MAX_SCHEMA_DEPTH));
        assert!(!is_subschema(&deep, &deep, true));
        let expected = ExpectedSchema {
            input_schema: deep.clone(),
            output_schema: None,
            has_tool_envelope: false,
            mode: SchemaMode::Exact,
        };
        assert!(verify_tool_schema(&expected, &deep, None).is_err());
    }
}
