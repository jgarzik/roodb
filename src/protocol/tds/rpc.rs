//! RPC requests: procedure identification and parameter binding.
//!
//! Drivers run parameterised statements through a handful of system
//! procedures (`sp_executesql`, `sp_prepare`, ...). The statement text
//! refers to parameters by name (`@P1`); the query engine binds values to
//! positional `?` placeholders. [`bind_parameters`] rewrites the parameter
//! *references* to placeholders. Parameter values never enter the SQL
//! text: they travel separately as typed values.

use super::params::{read_param, ParamError, Reader, RpcParam};

/// Marks a procedure identified by number instead of by name
const PROC_ID_SWITCH: u16 = 0xFFFF;
/// Separates the procedure calls of one request
const BATCH_FLAG: u8 = 0x80;
/// Separates procedure calls; the following call is not to be executed
/// by servers that cannot (TDS 7.2+ clients use it like `BATCH_FLAG`)
const NO_EXEC_FLAG: u8 = 0xFF;

/// The system procedures drivers use for parameterised statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Procedure {
    /// `sp_executesql`: run a statement with parameters
    ExecuteSql,
    /// `sp_prepare`: prepare a statement, returning a handle
    Prepare,
    /// `sp_execute`: run a prepared statement
    Execute,
    /// `sp_prepexec`: prepare and run in one call
    PrepExec,
    /// `sp_unprepare`: release a prepared statement
    Unprepare,
    /// Any other procedure, by name or number
    Other(String),
}

impl Procedure {
    fn from_id(id: u16) -> Self {
        match id {
            10 => Procedure::ExecuteSql,
            11 => Procedure::Prepare,
            12 => Procedure::Execute,
            13 => Procedure::PrepExec,
            15 => Procedure::Unprepare,
            other => Procedure::Other(format!("#{other}")),
        }
    }

    fn from_name(name: &str) -> Self {
        // Callers may qualify the name (sys.sp_executesql).
        let bare = name.rsplit('.').next().unwrap_or(name);
        match bare.to_ascii_lowercase().as_str() {
            "sp_executesql" => Procedure::ExecuteSql,
            "sp_prepare" => Procedure::Prepare,
            "sp_execute" => Procedure::Execute,
            "sp_prepexec" => Procedure::PrepExec,
            "sp_unprepare" => Procedure::Unprepare,
            _ => Procedure::Other(name.to_string()),
        }
    }
}

/// One procedure call of an RPC request.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcCall {
    pub procedure: Procedure,
    pub params: Vec<RpcParam>,
}

/// Parse the procedure calls of an RPC request (after ALL_HEADERS).
///
/// `max_value` bounds any single parameter value.
pub fn parse_rpc(body: &[u8], max_value: usize) -> Result<Vec<RpcCall>, ParamError> {
    let mut r = Reader::new(body);
    let mut calls = Vec::new();

    while !r.is_empty() {
        // Procedure name, or 0xFFFF followed by a procedure number
        let name_len = r.u16()?;
        let procedure = if name_len == PROC_ID_SWITCH {
            Procedure::from_id(r.u16()?)
        } else {
            let name = super::login::decode_ucs2(r.bytes(usize::from(name_len) * 2)?);
            Procedure::from_name(&name)
        };
        let _option_flags = r.u16()?;

        let mut params = Vec::new();
        loop {
            match r.peek() {
                None => break,
                Some(BATCH_FLAG) | Some(NO_EXEC_FLAG) => {
                    r.u8()?;
                    break;
                }
                Some(_) => params.push(read_param(&mut r, max_value)?),
            }
        }
        calls.push(RpcCall { procedure, params });
    }

    if calls.is_empty() {
        return Err(ParamError::Malformed);
    }
    Ok(calls)
}

/// Parameter names from a declaration list such as
/// `@P1 int, @name nvarchar(50) OUTPUT`, lower-cased, in order.
pub fn declared_names(declaration: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut item = String::new();
    // A trailing comma flushes the last item.
    for c in declaration.chars().chain(std::iter::once(',')) {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                if let Some(name) = item.split_whitespace().next() {
                    if name.starts_with('@') {
                        names.push(name.to_lowercase());
                    }
                }
                item.clear();
                continue;
            }
            _ => {}
        }
        item.push(c);
    }
    names
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '$' | '#')
}

/// Rewrite references to the named parameters in `sql` as positional `?`
/// placeholders.
///
/// Returns the rewritten statement and, for each placeholder in order,
/// the index into `names` of the parameter it refers to. `names` are
/// lower-case and include the `@`. Text inside string literals, quoted
/// identifiers and comments is left alone, as are `@@system` variables
/// and `@names` that are not parameters (user variables).
pub fn bind_parameters(sql: &str, names: &[String]) -> (String, Vec<usize>) {
    let mut out = String::with_capacity(sql.len());
    let mut order = Vec::new();
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // Quoted text: copy through the closing quote
            '\'' | '"' | '`' => {
                out.push(c);
                while let Some(q) = chars.next() {
                    out.push(q);
                    if q == '\\' && c != '`' {
                        if let Some(escaped) = chars.next() {
                            out.push(escaped);
                        }
                    } else if q == c {
                        if chars.peek() == Some(&c) {
                            out.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
            }
            // `-- ` and `#` comments run to the end of the line
            '-' if chars.peek() == Some(&'-') => {
                out.push(c);
                for q in chars.by_ref() {
                    out.push(q);
                    if q == '\n' {
                        break;
                    }
                }
            }
            '#' => {
                out.push(c);
                for q in chars.by_ref() {
                    out.push(q);
                    if q == '\n' {
                        break;
                    }
                }
            }
            // Block comments
            '/' if chars.peek() == Some(&'*') => {
                out.push_str("/*");
                chars.next();
                let mut previous = '\0';
                for q in chars.by_ref() {
                    out.push(q);
                    if previous == '*' && q == '/' {
                        break;
                    }
                    previous = q;
                }
            }
            '@' => {
                if chars.peek() == Some(&'@') {
                    // @@system_variable
                    out.push('@');
                    while let Some(&q) = chars.peek() {
                        if q == '@' || is_name_char(q) {
                            out.push(q);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    continue;
                }
                let mut name = String::from("@");
                while let Some(&q) = chars.peek() {
                    if is_name_char(q) {
                        name.push(q);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let lower = name.to_lowercase();
                match names.iter().position(|n| *n == lower) {
                    Some(index) => {
                        out.push('?');
                        order.push(index);
                    }
                    None => out.push_str(&name),
                }
            }
            _ => out.push(c),
        }
    }
    (out, order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::datum::Datum;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn declarations() {
        assert_eq!(
            declared_names("@P1 int, @P2 nvarchar(50), @d decimal(10, 2) OUTPUT"),
            names(&["@p1", "@p2", "@d"])
        );
        assert_eq!(declared_names(""), names(&[]));
        assert_eq!(declared_names("  @only   bigint  "), names(&["@only"]));
        // Not a parameter declaration
        assert_eq!(declared_names("junk, , @a int"), names(&["@a"]));
    }

    #[test]
    fn references_become_placeholders_in_order() {
        let n = names(&["@p1", "@p2"]);
        let (sql, order) =
            bind_parameters("SELECT * FROM t WHERE a = @P2 AND b = @p1 OR c = @P2", &n);
        assert_eq!(sql, "SELECT * FROM t WHERE a = ? AND b = ? OR c = ?");
        assert_eq!(order, vec![1, 0, 1]);

        // A longer name that merely starts with a parameter name is not it
        let (sql, order) = bind_parameters("SELECT @p10, @p1", &n);
        assert_eq!(sql, "SELECT @p10, ?");
        assert_eq!(order, vec![0]);
    }

    #[test]
    fn quoted_text_and_comments_are_not_rewritten() {
        let n = names(&["@p1"]);
        let cases = [
            ("SELECT '@p1', @p1", "SELECT '@p1', ?"),
            ("SELECT 'it''s @p1', @p1", "SELECT 'it''s @p1', ?"),
            (r"SELECT 'a\'@p1', @p1", r"SELECT 'a\'@p1', ?"),
            ("SELECT \"@p1\", `@p1`, @p1", "SELECT \"@p1\", `@p1`, ?"),
            ("SELECT 1 -- @p1\n, @p1", "SELECT 1 -- @p1\n, ?"),
            ("SELECT 1 # @p1\n, @p1", "SELECT 1 # @p1\n, ?"),
            ("SELECT /* @p1 */ @p1", "SELECT /* @p1 */ ?"),
            ("SELECT /*/ @p1 */ @p1", "SELECT /*/ @p1 */ ?"),
            (
                "SELECT @@version, @other, @p1",
                "SELECT @@version, @other, ?",
            ),
        ];
        for (input, expected) in cases {
            let (sql, order) = bind_parameters(input, &n);
            assert_eq!(sql, expected, "{input}");
            assert_eq!(order, vec![0], "{input}");
        }
        // Unterminated literal or comment: copied as is, no panic
        for input in ["SELECT '@p1", "SELECT /* @p1", "SELECT \"x", "@", "'", "-"] {
            let (sql, order) = bind_parameters(input, &n);
            assert_eq!(sql, input);
            assert!(order.is_empty());
        }
    }

    /// A value is never part of the statement text, whatever it contains.
    #[test]
    fn parameter_values_do_not_reach_the_sql() {
        let n = names(&["@p1"]);
        let (sql, order) = bind_parameters("SELECT * FROM users WHERE name = @P1", &n);
        assert_eq!(sql, "SELECT * FROM users WHERE name = ?");
        assert_eq!(order, vec![0]);
    }

    fn ucs2(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
    }

    fn int_param(name: &str, value: i32) -> Vec<u8> {
        let mut p = vec![name.encode_utf16().count() as u8];
        p.extend(ucs2(name));
        p.extend_from_slice(&[0x00, 0x26, 4, 4]);
        p.extend_from_slice(&value.to_le_bytes());
        p
    }

    #[test]
    fn parses_calls_by_id_and_by_name() {
        // sp_executesql by ID with two parameters, then sp_unprepare by name
        let mut body = vec![0xFF, 0xFF, 10, 0, 0, 0];
        body.extend(int_param("@a", 1));
        body.extend(int_param("", 2));
        body.push(BATCH_FLAG);
        let name = "sp_unprepare";
        body.extend_from_slice(&(name.len() as u16).to_le_bytes());
        body.extend(ucs2(name));
        body.extend_from_slice(&[0, 0]);
        body.extend(int_param("@handle", 7));

        let calls = parse_rpc(&body, 1 << 20).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].procedure, Procedure::ExecuteSql);
        assert_eq!(calls[0].params.len(), 2);
        assert_eq!(calls[0].params[0].name, "@a");
        assert_eq!(calls[0].params[1].value, Datum::Int(2));
        assert_eq!(calls[1].procedure, Procedure::Unprepare);
        assert_eq!(calls[1].params[0].value, Datum::Int(7));
    }

    #[test]
    fn procedure_names() {
        assert_eq!(Procedure::from_name("SP_ExecuteSQL"), Procedure::ExecuteSql);
        assert_eq!(Procedure::from_name("sys.sp_prepexec"), Procedure::PrepExec);
        assert_eq!(
            Procedure::from_name("my_proc"),
            Procedure::Other("my_proc".to_string())
        );
        assert_eq!(Procedure::from_id(12), Procedure::Execute);
        assert_eq!(Procedure::from_id(99), Procedure::Other("#99".to_string()));
    }

    #[test]
    fn malformed_requests_are_errors_not_panics() {
        assert_eq!(parse_rpc(&[], 1024), Err(ParamError::Malformed));
        // The request the old code indexed past: 4 bytes, then nothing
        assert_eq!(
            parse_rpc(&[0xFF, 0xFF, 0x0A, 0x00], 1024),
            Err(ParamError::Malformed)
        );
        // Name longer than the data
        assert_eq!(
            parse_rpc(&[0x10, 0x00, b'a', 0], 1024),
            Err(ParamError::Malformed)
        );

        let mut good = vec![0xFF, 0xFF, 10, 0, 0, 0];
        good.extend(int_param("@a", 1));
        for len in 0..good.len() {
            let _ = parse_rpc(&good[..len], 1024);
        }
        for pos in 0..good.len() {
            for value in [0x00, 0x7F, 0x80, 0xFF] {
                let mut corrupt = good.clone();
                corrupt[pos] = value;
                let _ = parse_rpc(&corrupt, 1024);
            }
        }
    }
}
