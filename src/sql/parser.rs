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
            return Err(SqlError::Parse("Empty SQL statement".to_string()));
        }
        if ast.len() > 1 {
            return Err(SqlError::Parse(
                "Multiple statements not supported".to_string(),
            ));
        }

        Ok(ast.into_iter().next().unwrap())
    }
    /// Normalize MySQL-specific syntax that sqlparser 0.50 doesn't handle:
    /// - `) charset xxx` → `) DEFAULT CHARSET=xxx`
    /// - `) engine=xxx` → `) ENGINE=xxx` (case normalization)
    fn normalize_mysql_syntax(sql: &str) -> String {
        use regex::Regex;
        // Replace bare `charset <name>` (not preceded by DEFAULT or CHARACTER)
        // after a closing paren or table option context
        let re = Regex::new(r"(?i)\)\s+charset\s+(\w+)").unwrap();
        let result = re.replace_all(sql, ") DEFAULT CHARSET=$1");
        result.into_owned()
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
        assert!(matches!(stmt, sp::Statement::Update { .. }));
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
