//! JSON function support
//!
//! Implements MySQL-compatible JSON functions with serde_json::Value as the
//! in-memory representation. JSON path expressions ($.key[idx]) are parsed
//! and evaluated for extraction and modification operations.

use serde_json::Value;

// ── JSON Path Parser ────────────────────────────────────────────────

/// A single leg of a JSON path expression
#[derive(Debug, Clone)]
pub enum PathLeg {
    /// Object key: $.key or $."key with spaces"
    Key(String),
    /// Array index: $[0], $[1]
    Index(usize),
    /// Array wildcard: $[*]
    ArrayWildcard,
    /// Object wildcard: $.* (all keys)
    ObjectWildcard,
}

/// Parse a MySQL JSON path expression like '$.foo.bar[0]' into path legs.
/// Returns None for invalid paths. The leading '$' is required.
pub fn parse_json_path(path: &str) -> Option<Vec<PathLeg>> {
    let path = path.trim();
    if !path.starts_with('$') {
        return None;
    }
    let rest = &path[1..];
    if rest.is_empty() {
        return Some(Vec::new()); // "$" = root
    }
    let mut legs = Vec::new();
    let mut chars = rest.chars().peekable();
    while chars.peek().is_some() {
        match chars.peek()? {
            '.' => {
                chars.next(); // consume '.'
                if chars.peek() == Some(&'*') {
                    chars.next();
                    legs.push(PathLeg::ObjectWildcard);
                } else if chars.peek() == Some(&'"') {
                    // Quoted key
                    chars.next(); // consume opening "
                    let mut key = String::new();
                    loop {
                        match chars.next() {
                            Some('"') => break,
                            Some('\\') => {
                                if let Some(c) = chars.next() {
                                    key.push(c);
                                }
                            }
                            Some(c) => key.push(c),
                            None => return None, // unterminated
                        }
                    }
                    legs.push(PathLeg::Key(key));
                } else {
                    // Unquoted key
                    let mut key = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == '.' || c == '[' {
                            break;
                        }
                        key.push(c);
                        chars.next();
                    }
                    if key.is_empty() {
                        return None;
                    }
                    legs.push(PathLeg::Key(key));
                }
            }
            '[' => {
                chars.next(); // consume '['
                if chars.peek() == Some(&'*') {
                    chars.next(); // consume '*'
                    if chars.next() != Some(']') {
                        return None;
                    }
                    legs.push(PathLeg::ArrayWildcard);
                } else {
                    let mut num_str = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == ']' {
                            break;
                        }
                        num_str.push(c);
                        chars.next();
                    }
                    if chars.next() != Some(']') {
                        return None;
                    }
                    let idx: usize = num_str.parse().ok()?;
                    legs.push(PathLeg::Index(idx));
                }
            }
            _ => return None, // unexpected character
        }
    }
    Some(legs)
}

// ── JSON Path Extraction ────────────────────────────────────────────

/// Extract a value at the given JSON path. Returns None if path doesn't exist.
pub fn json_extract<'a>(doc: &'a Value, path: &[PathLeg]) -> Option<&'a Value> {
    let mut current = doc;
    for leg in path {
        match leg {
            PathLeg::Key(key) => {
                current = current.as_object()?.get(key)?;
            }
            PathLeg::Index(idx) => {
                current = current.as_array()?.get(*idx)?;
            }
            PathLeg::ObjectWildcard | PathLeg::ArrayWildcard => {
                // Wildcards return multiple values — for single extraction, return None
                return None;
            }
        }
    }
    Some(current)
}

/// Extract with wildcard support — returns all matching values
pub fn json_extract_all<'a>(doc: &'a Value, path: &[PathLeg]) -> Vec<&'a Value> {
    if path.is_empty() {
        return vec![doc];
    }
    let leg = &path[0];
    let rest = &path[1..];
    match leg {
        PathLeg::Key(key) => {
            if let Some(obj) = doc.as_object() {
                if let Some(val) = obj.get(key) {
                    return json_extract_all(val, rest);
                }
            }
            Vec::new()
        }
        PathLeg::Index(idx) => {
            if let Some(arr) = doc.as_array() {
                if let Some(val) = arr.get(*idx) {
                    return json_extract_all(val, rest);
                }
            }
            Vec::new()
        }
        PathLeg::ObjectWildcard => {
            if let Some(obj) = doc.as_object() {
                let mut results = Vec::new();
                for val in obj.values() {
                    results.extend(json_extract_all(val, rest));
                }
                return results;
            }
            Vec::new()
        }
        PathLeg::ArrayWildcard => {
            if let Some(arr) = doc.as_array() {
                let mut results = Vec::new();
                for val in arr {
                    results.extend(json_extract_all(val, rest));
                }
                return results;
            }
            Vec::new()
        }
    }
}

// ── JSON Path Modification ──────────────────────────────────────────

/// Set a value at the given JSON path (creates intermediate objects/arrays)
pub fn json_set(doc: &mut Value, path: &[PathLeg], value: Value) {
    if path.is_empty() {
        *doc = value;
        return;
    }
    let leg = &path[0];
    let rest = &path[1..];
    match leg {
        PathLeg::Key(key) => {
            if !doc.is_object() {
                *doc = Value::Object(serde_json::Map::new());
            }
            let obj = doc.as_object_mut().unwrap();
            if rest.is_empty() {
                obj.insert(key.clone(), value);
            } else {
                let entry = obj
                    .entry(key.clone())
                    .or_insert_with(|| Value::Object(serde_json::Map::new()));
                json_set(entry, rest, value);
            }
        }
        PathLeg::Index(idx) => {
            if !doc.is_array() {
                return; // MySQL doesn't auto-create arrays
            }
            let arr = doc.as_array_mut().unwrap();
            if rest.is_empty() {
                if *idx < arr.len() {
                    arr[*idx] = value;
                } else {
                    // MySQL appends to end for out-of-bounds index
                    arr.push(value);
                }
            } else if *idx < arr.len() {
                json_set(&mut arr[*idx], rest, value);
            }
        }
        _ => {} // wildcards not supported in SET
    }
}

/// Insert a value only if the path doesn't already exist
pub fn json_insert(doc: &mut Value, path: &[PathLeg], value: Value) {
    if path.is_empty() {
        return; // can't replace root
    }
    // Check if path exists
    if json_extract(doc, path).is_some() {
        return; // already exists, don't replace
    }
    json_set(doc, path, value);
}

/// Replace a value only if the path already exists
pub fn json_replace(doc: &mut Value, path: &[PathLeg], value: Value) {
    if path.is_empty() {
        *doc = value;
        return;
    }
    // Check if path exists
    if json_extract(doc, path).is_none() {
        return; // doesn't exist, don't insert
    }
    json_set(doc, path, value);
}

/// Remove a value at the given path
pub fn json_remove(doc: &mut Value, path: &[PathLeg]) -> bool {
    if path.is_empty() {
        return false; // can't remove root
    }
    if path.len() == 1 {
        match &path[0] {
            PathLeg::Key(key) => {
                if let Some(obj) = doc.as_object_mut() {
                    return obj.remove(key).is_some();
                }
            }
            PathLeg::Index(idx) => {
                if let Some(arr) = doc.as_array_mut() {
                    if *idx < arr.len() {
                        arr.remove(*idx);
                        return true;
                    }
                }
            }
            _ => {}
        }
        return false;
    }
    // Navigate to parent
    let parent_path = &path[..path.len() - 1];
    let last = &path[path.len() - 1];
    // We need a mutable reference to the parent
    let parent = json_extract_mut(doc, parent_path);
    if let Some(parent) = parent {
        match last {
            PathLeg::Key(key) => {
                if let Some(obj) = parent.as_object_mut() {
                    return obj.remove(key).is_some();
                }
            }
            PathLeg::Index(idx) => {
                if let Some(arr) = parent.as_array_mut() {
                    if *idx < arr.len() {
                        arr.remove(*idx);
                        return true;
                    }
                }
            }
            _ => {}
        }
    }
    false
}

pub fn json_extract_mut<'a>(doc: &'a mut Value, path: &[PathLeg]) -> Option<&'a mut Value> {
    let mut current = doc;
    for leg in path {
        match leg {
            PathLeg::Key(key) => {
                current = current.as_object_mut()?.get_mut(key)?;
            }
            PathLeg::Index(idx) => {
                current = current.as_array_mut()?.get_mut(*idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Append value to array at path
pub fn json_array_append(doc: &mut Value, path: &[PathLeg], value: Value) {
    if let Some(target) = json_extract_mut(doc, path) {
        if let Some(arr) = target.as_array_mut() {
            arr.push(value);
        } else {
            // MySQL wraps non-array into array: [original, new]
            let original = target.take();
            *target = Value::Array(vec![original, value]);
        }
    }
}

/// Insert value into array at specific index
pub fn json_array_insert(doc: &mut Value, path: &[PathLeg]) {
    // This requires the last path leg to be an index
    // The value to insert comes from the caller
    // Handled in eval.rs directly
    let _ = doc;
    let _ = path;
}

// ── JSON Merge ──────────────────────────────────────────────────────

/// JSON_MERGE_PRESERVE: merge documents, preserving duplicate keys by wrapping in arrays
pub fn json_merge_preserve(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Object(o1), Value::Object(o2)) => {
            let mut result = o1.clone();
            for (key, val) in o2 {
                result
                    .entry(key.clone())
                    .and_modify(|existing| {
                        *existing = json_merge_preserve(existing, val);
                    })
                    .or_insert_with(|| val.clone());
            }
            Value::Object(result)
        }
        (Value::Array(a1), Value::Array(a2)) => {
            let mut result = a1.clone();
            result.extend(a2.iter().cloned());
            Value::Array(result)
        }
        (Value::Array(a1), _) => {
            let mut result = a1.clone();
            result.push(b.clone());
            Value::Array(result)
        }
        (_, Value::Array(a2)) => {
            let mut result = vec![a.clone()];
            result.extend(a2.iter().cloned());
            Value::Array(result)
        }
        _ => Value::Array(vec![a.clone(), b.clone()]),
    }
}

/// JSON_MERGE_PATCH: RFC 7396 merge — replaces duplicate keys
pub fn json_merge_patch(a: &Value, b: &Value) -> Value {
    match (a, b) {
        (Value::Object(o1), Value::Object(o2)) => {
            let mut result = o1.clone();
            for (key, val) in o2 {
                if val.is_null() {
                    result.remove(key);
                } else if let Some(existing) = result.get(key) {
                    result.insert(key.clone(), json_merge_patch(existing, val));
                } else {
                    result.insert(key.clone(), val.clone());
                }
            }
            Value::Object(result)
        }
        (_, _) => b.clone(),
    }
}

// ── JSON Search ─────────────────────────────────────────────────────

/// JSON_CONTAINS: check if target contains candidate
pub fn json_contains(target: &Value, candidate: &Value) -> bool {
    match (target, candidate) {
        (Value::Object(t), Value::Object(c)) => {
            // Every key-value pair in c must exist in t
            c.iter()
                .all(|(k, v)| t.get(k).is_some_and(|tv| json_contains(tv, v)))
        }
        (Value::Array(t), Value::Array(c)) => {
            // Every element in c must be contained in some element of t
            c.iter().all(|cv| t.iter().any(|tv| json_contains(tv, cv)))
        }
        (Value::Array(t), _) => {
            // Scalar candidate: check if any array element contains it
            t.iter().any(|tv| json_contains(tv, candidate))
        }
        _ => target == candidate,
    }
}

/// JSON_SEARCH: find paths matching a string pattern (LIKE semantics)
pub fn json_search(doc: &Value, mode: &str, pattern: &str) -> Vec<String> {
    let mut results = Vec::new();
    json_search_recursive(doc, pattern, "$", mode, &mut results);
    results
}

fn json_search_recursive(
    doc: &Value,
    pattern: &str,
    current_path: &str,
    mode: &str,
    results: &mut Vec<String>,
) {
    match doc {
        Value::String(s) => {
            if like_match(s, pattern) {
                results.push(format!("\"{}\"", current_path));
                if mode.eq_ignore_ascii_case("one") && !results.is_empty() {}
            }
        }
        Value::Object(obj) => {
            for (key, val) in obj {
                let path = format!("{}.{}", current_path, key);
                json_search_recursive(val, pattern, &path, mode, results);
                if mode.eq_ignore_ascii_case("one") && !results.is_empty() {
                    return;
                }
            }
        }
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                let path = format!("{}[{}]", current_path, i);
                json_search_recursive(val, pattern, &path, mode, results);
                if mode.eq_ignore_ascii_case("one") && !results.is_empty() {
                    return;
                }
            }
        }
        _ => {}
    }
}

/// Simple LIKE pattern matching (% = any, _ = single char)
fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_inner(&s_chars, &p_chars)
}

fn like_match_inner(s: &[char], p: &[char]) -> bool {
    if p.is_empty() {
        return s.is_empty();
    }
    if p[0] == '%' {
        // % matches zero or more characters
        for i in 0..=s.len() {
            if like_match_inner(&s[i..], &p[1..]) {
                return true;
            }
        }
        false
    } else if s.is_empty() {
        false
    } else if p[0] == '_' || p[0] == s[0] {
        like_match_inner(&s[1..], &p[1..])
    } else {
        false
    }
}

// ── JSON Type / Validation ──────────────────────────────────────────

/// Return MySQL JSON type name
pub fn json_type(val: &Value) -> &'static str {
    match val {
        Value::Null => "NULL",
        Value::Bool(true) => "TRUE",
        Value::Bool(false) => "FALSE",
        Value::Number(n) => {
            if n.is_f64() && n.as_f64().is_some_and(|f| f.fract() != 0.0) {
                "DOUBLE"
            } else {
                "INTEGER"
            }
        }
        Value::String(_) => "STRING",
        Value::Array(_) => "ARRAY",
        Value::Object(_) => "OBJECT",
    }
}

/// Calculate JSON depth (nesting level)
pub fn json_depth(val: &Value) -> u64 {
    match val {
        Value::Array(arr) => {
            if arr.is_empty() {
                1
            } else {
                1 + arr.iter().map(json_depth).max().unwrap_or(0)
            }
        }
        Value::Object(obj) => {
            if obj.is_empty() {
                1
            } else {
                1 + obj.values().map(json_depth).max().unwrap_or(0)
            }
        }
        _ => 1,
    }
}

// ── Datum Conversion Helpers ────────────────────────────────────────

/// Convert a Datum to a serde_json::Value
pub fn datum_to_json(datum: &super::datum::Datum) -> Value {
    use super::datum::Datum;
    match datum {
        Datum::Null => Value::Null,
        Datum::Bool(b) => Value::Bool(*b),
        Datum::Int(i) => Value::Number(serde_json::Number::from(*i)),
        Datum::UnsignedInt(u) => Value::Number(serde_json::Number::from(*u)),
        Datum::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Datum::String(s) => {
            // Try to parse as JSON first; if it's valid JSON, use the parsed value
            serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.clone()))
        }
        Datum::Json(v) => v.clone(),
        Datum::Decimal { value, scale } => {
            let s = super::datum::format_decimal(*value, *scale);
            serde_json::Number::from_f64(s.parse::<f64>().unwrap_or(0.0))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        _ => Value::String(datum.to_display_string()),
    }
}

/// Convert a serde_json::Value to a Datum::Json
pub fn json_to_datum(val: Value) -> super::datum::Datum {
    super::datum::Datum::Json(val)
}

/// Parse a string argument as JSON, returning the Value
pub fn parse_json_arg(datum: &super::datum::Datum) -> Option<Value> {
    use super::datum::Datum;
    match datum {
        Datum::Null => None,
        Datum::Json(v) => Some(v.clone()),
        Datum::String(s) => serde_json::from_str(s).ok(),
        other => {
            let s = other.to_display_string();
            serde_json::from_str(&s).ok().or(Some(Value::String(s)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_basic() {
        let path = parse_json_path("$").unwrap();
        assert!(path.is_empty());

        let path = parse_json_path("$.foo").unwrap();
        assert_eq!(path.len(), 1);
        assert!(matches!(&path[0], PathLeg::Key(k) if k == "foo"));

        let path = parse_json_path("$.foo.bar").unwrap();
        assert_eq!(path.len(), 2);

        let path = parse_json_path("$.foo[0]").unwrap();
        assert_eq!(path.len(), 2);
        assert!(matches!(&path[1], PathLeg::Index(0)));

        let path = parse_json_path("$[*]").unwrap();
        assert_eq!(path.len(), 1);
        assert!(matches!(&path[0], PathLeg::ArrayWildcard));
    }

    #[test]
    fn test_json_extract() {
        let doc: Value = serde_json::json!({"a": {"b": [1, 2, 3]}});
        let path = parse_json_path("$.a.b[1]").unwrap();
        assert_eq!(json_extract(&doc, &path), Some(&serde_json::json!(2)));

        let path = parse_json_path("$.a.b").unwrap();
        assert_eq!(
            json_extract(&doc, &path),
            Some(&serde_json::json!([1, 2, 3]))
        );

        let path = parse_json_path("$.x").unwrap();
        assert_eq!(json_extract(&doc, &path), None);
    }

    #[test]
    fn test_json_set() {
        let mut doc: Value = serde_json::json!({"a": 1});
        let path = parse_json_path("$.b").unwrap();
        json_set(&mut doc, &path, serde_json::json!(2));
        assert_eq!(doc, serde_json::json!({"a": 1, "b": 2}));
    }

    #[test]
    fn test_json_contains() {
        let target: Value = serde_json::json!({"a": 1, "b": [2, 3]});
        let candidate: Value = serde_json::json!({"a": 1});
        assert!(json_contains(&target, &candidate));

        let candidate: Value = serde_json::json!({"a": 2});
        assert!(!json_contains(&target, &candidate));
    }

    #[test]
    fn test_json_merge_preserve() {
        let a: Value = serde_json::json!({"a": 1});
        let b: Value = serde_json::json!({"a": 2, "b": 3});
        let result = json_merge_preserve(&a, &b);
        assert_eq!(result, serde_json::json!({"a": [1, 2], "b": 3}));
    }

    #[test]
    fn test_json_merge_patch() {
        let a: Value = serde_json::json!({"a": 1, "b": 2});
        let b: Value = serde_json::json!({"a": 3, "b": null, "c": 4});
        let result = json_merge_patch(&a, &b);
        assert_eq!(result, serde_json::json!({"a": 3, "c": 4}));
    }

    #[test]
    fn test_json_type() {
        assert_eq!(json_type(&serde_json::json!(null)), "NULL");
        assert_eq!(json_type(&serde_json::json!(true)), "TRUE");
        assert_eq!(json_type(&serde_json::json!(42)), "INTEGER");
        assert_eq!(json_type(&serde_json::json!(3.14)), "DOUBLE");
        assert_eq!(json_type(&serde_json::json!("hello")), "STRING");
        assert_eq!(json_type(&serde_json::json!([1, 2])), "ARRAY");
        assert_eq!(json_type(&serde_json::json!({"a": 1})), "OBJECT");
    }

    #[test]
    fn test_json_depth() {
        assert_eq!(json_depth(&serde_json::json!(1)), 1);
        assert_eq!(json_depth(&serde_json::json!([1, 2])), 2);
        assert_eq!(json_depth(&serde_json::json!({"a": [1, 2]})), 3);
    }
}
