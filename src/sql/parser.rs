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
        let ast = SqlParser::parse_sql(&dialect, sql)?;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast as sp;

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
