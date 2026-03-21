//! SQL parser wrapper around sqlparser crate

use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser as SqlParser;

use crate::sql::error::{SqlError, SqlResult};

/// SQL parser
pub struct Parser;

impl Parser {
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
    fn normalize_mysql_syntax(sql: &str) -> String {
        use regex::Regex;
        // MOD is a keyword in sqlparser, so MOD(x,y) doesn't parse as a function call.
        // Replace MOD( with _ROODB_MOD( to make it a normal function name.
        let re_mod = Regex::new(r"(?i)\bMOD\s*\(").unwrap();
        let result = re_mod.replace_all(sql, "_ROODB_MOD(");

        // MySQL allows BLOB(N) and TEXT(N) with size hints that sqlparser doesn't accept.
        // Strip the size hint: BLOB(250) → BLOB, TEXT(70000) → TEXT
        let re_blob_size = Regex::new(r"(?i)\b(BLOB|TEXT)\s*\(\s*\d+\s*\)").unwrap();
        let result = re_blob_size.replace_all(&result, "$1");

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
    fn test_parse_join() {
        let stmt = Parser::parse_one(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        assert!(matches!(stmt, sp::Statement::Query(_)));
    }
}
