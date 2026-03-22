//! SQL parser wrapper around sqlparser crate

use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::sql::error::{SqlError, SqlResult};

/// SQL parser
pub struct Parser;

impl Parser {
    /// Normalize SQL for ALTER TABLE parsing (subset of full normalization)
    pub fn normalize_for_alter(sql: &str) -> String {
        Self::normalize_mysql_syntax(sql)
    }

    /// Parse a single SQL statement
    pub fn parse_one(sql: &str) -> SqlResult<Statement> {
        let dialect = MySqlDialect {};
        // Normalize MySQL-specific syntax that sqlparser doesn't handle
        let normalized = Self::normalize_mysql_syntax(sql);
        let ast = SqlParser::parse_sql(&dialect, &normalized)?;

        if ast.is_empty() {
            // Distinguish empty query from comment-only query.
            // sqlparser strips comments, so if the original had content but
            // no statements, it was comment-only.
            let stripped = normalized.trim().trim_end_matches(';').trim();
            if !stripped.is_empty() {
                return Err(SqlError::CommentOnly);
            }
            return Err(SqlError::EmptyQuery);
        }
        if ast.len() > 1 {
            return Err(SqlError::Parse(
                "Multiple statements not supported".to_string(),
            ));
        }

        Ok(ast.into_iter().next().unwrap())
    }
    /// Normalize MySQL-specific syntax that sqlparser doesn't handle:
    /// - `) charset xxx` → `) DEFAULT CHARSET=xxx`
    /// - CREATE PROCEDURE name() BEGIN → CREATE PROCEDURE name() AS BEGIN
    ///   (sqlparser expects AS before BEGIN for procedure bodies)
    /// - DECLARE var TYPE [DEFAULT expr]; → SET var = expr; (inside procedure bodies)
    ///   (sqlparser's MySQL dialect only supports DECLARE ... CURSOR FOR ...)
    // MySQL-compatible version for /*!NNNNN*/ comments (80045 = 8.0.45)
    const MYSQL_VERSION_ID: u64 = 80045;

    /// Process MySQL conditional comments (`/*!...*/`).
    ///
    /// MySQL rules:
    /// - `/*!NNNNN code */` where NNNNN is exactly 5 digits: if server version >= NNNNN,
    ///   execute `code`; otherwise treat as comment (strip).
    /// - `/*!code */` (no 5-digit prefix): execute `code` unconditionally.
    ///
    /// sqlparser's `tokenize_comment_hints` incorrectly strips ALL leading digits
    /// (not just exactly 5), so we must handle this before sqlparser sees the SQL.
    fn normalize_conditional_comments(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let bytes = sql.as_bytes();
        let len = bytes.len();
        let mut i = 0;

        while i < len {
            // Look for /*! pattern
            if i + 2 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' && bytes[i + 2] == b'!' {
                // Find the matching */
                let content_start = i + 3; // after "/*!"
                let mut end = content_start;
                while end + 1 < len {
                    if bytes[end] == b'*' && bytes[end + 1] == b'/' {
                        break;
                    }
                    end += 1;
                }
                if end + 1 >= len {
                    // Unclosed comment — leave as-is for parser to report error
                    result.push_str(&sql[i..]);
                    break;
                }

                let inner = &sql[content_start..end]; // content between /*! and */
                let closing = end + 2; // position after */
                                       // Check if inner starts with exactly 5 digits
                let digit_count = inner.bytes().take_while(|b| b.is_ascii_digit()).count();
                if digit_count >= 5 {
                    // Has a version number (first 5 digits)
                    let version_str = &inner[..5];
                    let version: u64 = version_str.parse().unwrap_or(0);
                    let code = &inner[5..];
                    if Self::MYSQL_VERSION_ID >= version {
                        // Our version >= required: include the code
                        result.push(' ');
                        result.push_str(code);
                        result.push(' ');
                    }
                    // else: strip entirely (our version is too old)
                } else {
                    // No 5-digit version prefix: include content unconditionally
                    result.push(' ');
                    result.push_str(inner);
                    result.push(' ');
                }

                i = closing;
            } else {
                result.push(sql[i..].chars().next().unwrap());
                i += sql[i..].chars().next().unwrap().len_utf8();
            }
        }

        result
    }

    fn normalize_mysql_syntax(sql: &str) -> String {
        use regex::Regex;

        // Process conditional comments first, before any other normalization
        let sql = Self::normalize_conditional_comments(sql);

        // MOD is a keyword in sqlparser. Handle both MOD(x,y) function and x MOD y infix.
        // Replace MOD( with _ROODB_MOD( for function call form.
        let re_mod_fn = Regex::new(r"(?i)\bMOD\s*\(").unwrap();
        let result = re_mod_fn.replace_all(&sql, "_ROODB_MOD(");
        // Replace infix `MOD` keyword with `%` operator.
        // \bMOD\b won't match _ROODB_MOD since \b needs a word boundary.
        // MOD( was already replaced above, so only standalone MOD remains.
        let re_mod_infix = Regex::new(r"(?i)\bMOD\b").unwrap();
        let result = re_mod_infix.replace_all(&result, "%");
        let re_insert_fn = Regex::new(r"(?i)\bINSERT\s*\(").unwrap();
        let result = re_insert_fn.replace_all(&result, "_ROODB_INSERT(");

        // MySQL `!expr` is NOT — sqlparser doesn't parse `!` as unary NOT in MySQL dialect.
        // Replace `!` (when used as unary prefix, not !=) with NOT
        let re_bang = Regex::new(r"!([^=])").unwrap();
        let result = re_bang.replace_all(&result, "NOT $1");

        // MySQL `DEFAULT` keyword in VALUES context → NULL (auto-fill with default value)
        let re_default = Regex::new(r"(?i)\bVALUES\b[^;]*").unwrap();
        let result = {
            let s = result.to_string();
            re_default
                .replace_all(&s, |caps: &regex::Captures| {
                    caps[0]
                        .replace("DEFAULT", "NULL")
                        .replace("default", "NULL")
                })
                .to_string()
        };

        // MySQL ZEROFILL is a display attribute that sqlparser doesn't handle. Strip it.
        let re_zerofill = Regex::new(r"(?i)\s+ZEROFILL\b").unwrap();
        let result = re_zerofill.replace_all(&result, "");

        // MySQL allows BLOB(N) and TEXT(N) with size hints that sqlparser doesn't accept.
        // Strip the size hint: BLOB(250) → BLOB, TEXT(70000) → TEXT
        let re_blob_size = Regex::new(r"(?i)\b(BLOB|TEXT)\s*\(\s*\d+\s*\)").unwrap();
        let result = re_blob_size.replace_all(&result, "$1");

        // MySQL `LONG` type = MEDIUMTEXT, `LONG BYTE` = MEDIUMBLOB
        // Must replace LONG BYTE before standalone LONG
        let re_long_byte = Regex::new(r"(?i)\bLONG\s+BYTE\b").unwrap();
        let result = re_long_byte.replace_all(&result, "MEDIUMBLOB");
        // Replace standalone LONG (not followed by known compound types)
        // We match LONG and check the next word in a callback
        let result = {
            let s = result.to_string();
            let re_long = Regex::new(r"(?i)\bLONG\b").unwrap();
            let mut out = String::with_capacity(s.len());
            let mut last = 0;
            for m in re_long.find_iter(&s) {
                out.push_str(&s[last..m.start()]);
                // Check what follows LONG
                let after = s[m.end()..].trim_start();
                let next_word = after.split_whitespace().next().unwrap_or("").to_uppercase();
                if matches!(next_word.as_str(), "VARCHAR" | "BLOB" | "TEXT" | "BYTE") {
                    // Keep original (LONG VARCHAR, LONG BLOB, etc.)
                    out.push_str(m.as_str());
                } else {
                    out.push_str("MEDIUMTEXT");
                }
                last = m.end();
            }
            out.push_str(&s[last..]);
            out
        };

        // Replace bare `charset <name>` (not preceded by DEFAULT or CHARACTER)
        // after a closing paren or table option context
        let re = Regex::new(r"(?i)\)\s+charset\s+(\w+)").unwrap();
        let result = re.replace_all(&result, ") DEFAULT CHARSET=$1");

        // Normalize CREATE PROCEDURE: insert AS before BEGIN if missing
        // MySQL: CREATE PROCEDURE name(...) BEGIN ... END
        // sqlparser expects: CREATE PROCEDURE name(...) AS BEGIN ... END
        let re_proc = Regex::new(r"(?i)(CREATE\s+PROCEDURE\s+\w+\s*\([^)]*\))\s+BEGIN\b").unwrap();
        let result = re_proc.replace_all(&result, "$1 AS BEGIN");

        // Normalize DECLARE variable statements inside procedure bodies.
        // sqlparser's MySQL dialect only supports DECLARE ... CURSOR FOR ...,
        // not DECLARE var TYPE [DEFAULT expr]. Convert to SET statements.
        Self::normalize_declare_in_body(&result)
    }

    /// Normalize MySQL procedure body syntax that sqlparser doesn't support:
    /// - DECLARE var TYPE [DEFAULT expr]; → SET var = [expr|NULL];
    /// - WHILE cond DO ... END WHILE → WHILE cond BEGIN ... END
    fn normalize_declare_in_body(sql: &str) -> String {
        use regex::Regex;
        let upper = sql.to_uppercase();
        if !upper.contains("BEGIN") {
            return sql.to_string();
        }

        // Normalize WHILE: END WHILE → END, DO → BEGIN (for WHILE bodies)
        let mut sql = sql.to_string();
        if upper.contains("WHILE") {
            // Replace END WHILE with END
            let re_end_while = Regex::new(r"(?i)\bEND\s+WHILE\b").unwrap();
            sql = re_end_while.replace_all(&sql, "END").to_string();

            // Replace standalone DO keyword (after WHILE condition) with BEGIN
            // Match: a word boundary + DO + whitespace/newline (not part of another word)
            // Only safe in procedure body context where DO is only used in WHILE...DO
            let re_do = Regex::new(r"(?i)\bDO\s").unwrap();
            sql = re_do.replace_all(&sql, "BEGIN ").to_string();
        }

        // Only process DECLARE if it exists
        if !upper.contains("DECLARE ") {
            return sql;
        }

        // Process each semicolon-delimited segment
        let re_declare = Regex::new(r"(?i)\bDECLARE\s+(\w+)\s+").unwrap();
        let mut result = String::with_capacity(sql.len());
        let mut last_end = 0;

        // Find each DECLARE statement by scanning for the pattern
        for m in re_declare.find_iter(&sql) {
            let start = m.start();
            // Find the rest of this statement (up to the next semicolon)
            let rest_start = m.end();
            let semi_pos = sql[rest_start..].find(';').map(|p| rest_start + p);
            let stmt_end = semi_pos.unwrap_or(sql.len());

            // Extract the full DECLARE statement
            let full_stmt = &sql[start..stmt_end];
            let words: Vec<&str> = full_stmt.split_whitespace().collect();

            // Skip cursor declarations: DECLARE name CURSOR ...
            if words.len() >= 3 && words[2].eq_ignore_ascii_case("CURSOR") {
                continue;
            }

            // This is a variable declaration - replace it
            let var_name = words.get(1).unwrap_or(&"_");
            let stmt_upper = full_stmt.to_uppercase();

            let replacement = if let Some(default_pos) = stmt_upper.find(" DEFAULT ") {
                let default_val = &full_stmt[default_pos + 9..];
                format!("SET {} = {}", var_name, default_val.trim())
            } else {
                format!("SET {} = NULL", var_name)
            };

            // Copy everything up to this DECLARE, then the replacement
            result.push_str(&sql[last_end..start]);
            result.push_str(&replacement);
            last_end = stmt_end;
        }

        // Copy remaining text
        result.push_str(&sql[last_end..]);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast as sp;

    #[test]
    fn test_parse_drop_view() {
        let stmt = Parser::parse_one("drop view if exists v1").unwrap();
        match &stmt {
            sp::Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                assert_eq!(*object_type, sp::ObjectType::View);
                assert!(*if_exists);
                assert_eq!(names.len(), 1);
                assert_eq!(names[0].to_string(), "v1");
            }
            other => panic!("Expected Drop, got: {:#?}", other),
        }
    }

    #[test]
    fn test_parse_select() {
        let stmt = Parser::parse_one("SELECT id, name FROM users WHERE id = 1").unwrap();
        assert!(matches!(stmt, sp::Statement::Query(_)));
    }

    #[test]
    fn test_parse_insert() {
        let stmt = Parser::parse_one("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        assert!(matches!(stmt, sp::Statement::Insert(_)));
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = Parser::parse_one(
            "CREATE TABLE users (id INT NOT NULL, name VARCHAR(100), PRIMARY KEY (id))",
        )
        .unwrap();
        assert!(matches!(stmt, sp::Statement::CreateTable(_)));
    }

    #[test]
    fn test_parse_update() {
        let stmt = Parser::parse_one("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        assert!(matches!(stmt, sp::Statement::Update(_)));
    }

    #[test]
    fn test_parse_delete() {
        let stmt = Parser::parse_one("DELETE FROM users WHERE id = 1").unwrap();
        assert!(matches!(stmt, sp::Statement::Delete(_)));
    }

    #[test]
    fn test_conditional_comment_no_version() {
        // /*!2*/ has no 5-digit version: content "2" included unconditionally
        let result = Parser::normalize_conditional_comments("select 1/*!2*/");
        assert!(
            result.contains("2"),
            "content should be included: {}",
            result
        );
    }

    #[test]
    fn test_conditional_comment_version_low() {
        // /*!00000 2 */ version 0 <= our version: include content
        let result = Parser::normalize_conditional_comments("select 1 + /*!00000 2 */ + 3");
        assert!(result.contains("2"), "code should be included: {}", result);
    }

    #[test]
    fn test_conditional_comment_version_high() {
        // /*!99999 noise */ version 99999 > our version: strip
        let result = Parser::normalize_conditional_comments("select 1/*!99999 noise*/");
        assert!(
            !result.contains("noise"),
            "code should be stripped: {}",
            result
        );
    }

    #[test]
    fn test_conditional_comment_mixed() {
        let result = Parser::normalize_conditional_comments(
            "select 1 + /*!00000 2 */ + 3 /*!99999 noise*/ + 4",
        );
        assert!(result.contains("2"), "low version should be included");
        assert!(!result.contains("noise"), "high version should be stripped");
        assert!(result.contains("4"), "trailing content preserved");
    }

    #[test]
    fn test_parse_join() {
        let stmt = Parser::parse_one(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        assert!(matches!(stmt, sp::Statement::Query(_)));
    }
}
