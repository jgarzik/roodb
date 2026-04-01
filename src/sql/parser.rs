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
        let mut ast = SqlParser::parse_sql(&dialect, &normalized)?;

        // Fix DIV associativity: sqlparser's MySQL dialect makes DIV right-associative
        // (parses `A DIV B / C` as `A DIV (B / C)` instead of `(A DIV B) / C`).
        // Walk the AST and rotate DIV chains to be left-associative.
        for stmt in &mut ast {
            Self::fix_div_associativity_stmt(stmt);
        }

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

        // DO expr → SELECT expr (evaluate and discard result)
        let re_do = Regex::new(r"(?i)^\s*DO\s+").unwrap();
        let sql = re_do.replace(&sql, "SELECT ");
        let sql = sql.to_string();

        // SCHEMA() is a synonym for DATABASE() but SCHEMA is a reserved keyword
        // in sqlparser and can't be parsed as a function call. Rewrite to DATABASE().
        let sql = {
            let re_schema = Regex::new(r"(?i)\bSCHEMA\s*\(\s*\)").unwrap();
            re_schema.replace_all(&sql, "DATABASE()").to_string()
        };

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

        // MySQL `DEFAULT` keyword in VALUES context → special marker for resolver
        // to substitute with the column's actual default value.
        let re_default = Regex::new(r"(?i)\bVALUES\b[^;]*").unwrap();
        let result = {
            let s = result.to_string();
            re_default
                .replace_all(&s, |caps: &regex::Captures| {
                    // Replace whole-word DEFAULT (case-insensitive) with marker
                    let re_kw = Regex::new(r"(?i)\bDEFAULT\b").unwrap();
                    re_kw.replace_all(&caps[0], "__ROODB_DEFAULT__").to_string()
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
        // Use a non-regex approach to handle nested parens in param types like VARCHAR(50)
        let result = Self::insert_procedure_as_keyword(&result);

        // Normalize single-statement procedure bodies (no BEGIN/END):
        // MySQL: CREATE PROCEDURE name(...) SELECT ...;
        // → CREATE PROCEDURE name(...) AS BEGIN SELECT ...; END
        // Uses balanced-paren scanner to handle types like CHAR(16) in params.
        let result = Self::normalize_single_stmt_procedure(&result);

        // Normalize DECLARE variable statements inside procedure bodies.
        // sqlparser's MySQL dialect only supports DECLARE ... CURSOR FOR ...,
        // not DECLARE var TYPE [DEFAULT expr]. Convert to SET statements.
        Self::normalize_declare_in_body(&result)
    }

    /// Normalize single-statement procedure bodies (no BEGIN/END) to AS BEGIN...END.
    fn normalize_single_stmt_procedure(sql: &str) -> String {
        let upper = sql.to_uppercase();
        if !upper.contains("CREATE PROCEDURE") || upper.contains(" AS BEGIN") {
            return sql.to_string();
        }
        // Find CREATE PROCEDURE, then its param list
        let Some(cp) = upper.find("CREATE PROCEDURE") else {
            return sql.to_string();
        };
        let Some(rel_paren) = sql[cp..].find('(') else {
            return sql.to_string();
        };
        let paren_start = cp + rel_paren;
        let Some(paren_end) = Self::find_matching_close_paren(sql, paren_start) else {
            return sql.to_string();
        };
        // Check what follows the closing paren
        let after = sql[paren_end + 1..].trim_start();
        let after_upper = after.to_uppercase();
        // If it starts with BEGIN, the multi-statement handler handles it
        if after_upper.starts_with("BEGIN") || after_upper.starts_with("AS ") {
            return sql.to_string();
        }
        // Check if it starts with a DML/SET keyword (single-statement body)
        let is_single_stmt = after_upper.starts_with("SELECT ")
            || after_upper.starts_with("INSERT ")
            || after_upper.starts_with("UPDATE ")
            || after_upper.starts_with("DELETE ")
            || after_upper.starts_with("SET ");
        if is_single_stmt {
            let prefix = &sql[..paren_end + 1];
            let body = after.trim_end_matches(';').trim();
            format!("{} AS BEGIN {}; END", prefix, body)
        } else {
            sql.to_string()
        }
    }

    /// Find the matching closing paren for an opening paren at `start`,
    /// skipping parens inside single-quoted string literals.
    fn find_matching_close_paren(sql: &str, start: usize) -> Option<usize> {
        let bytes = sql.as_bytes();
        let mut depth = 0;
        let mut in_string = false;
        let mut i = start;
        while i < bytes.len() {
            let ch = bytes[i];
            if in_string {
                if ch == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_string = false;
                }
            } else {
                match ch {
                    b'\'' => in_string = true,
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(i);
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        None
    }

    /// Insert AS keyword before BEGIN in CREATE PROCEDURE if missing.
    /// Handles nested parens in parameter types like VARCHAR(50).
    fn insert_procedure_as_keyword(sql: &str) -> String {
        let upper = sql.to_uppercase();
        if !upper.contains("CREATE PROCEDURE") {
            return sql.to_string();
        }
        let mut result = sql.to_string();
        let mut search_start = 0;
        loop {
            let upper_remaining = result[search_start..].to_uppercase();
            let Some(cp_pos) = upper_remaining.find("CREATE PROCEDURE") else {
                break;
            };
            let abs_pos = search_start + cp_pos;
            // Find the opening paren of the param list
            let Some(rel_paren) = result[abs_pos..].find('(') else {
                break;
            };
            let paren_start = abs_pos + rel_paren;
            let Some(paren_end) = Self::find_matching_close_paren(&result, paren_start) else {
                break;
            };
            // Get text after closing paren, trimmed of whitespace
            let after = result[paren_end + 1..].trim_start();
            let after_upper = after.to_uppercase();
            // Check: starts with BEGIN as a keyword (not part of longer word), not already AS
            let starts_with_begin = after_upper.starts_with("BEGIN")
                && after_upper
                    .as_bytes()
                    .get(5)
                    .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_');
            if starts_with_begin {
                // Find exact position of BEGIN in the original string
                let ws_len = result[paren_end + 1..].len() - after.len();
                let insert_pos = paren_end + 1 + ws_len;
                result.insert_str(insert_pos, "AS ");
                search_start = insert_pos + 3 + 5; // past "AS BEGIN"
            } else {
                search_start = paren_end + 1;
            }
        }
        result
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

        // Normalize WHILE blocks structurally (not with regex)
        let mut sql = sql.to_string();
        if upper.contains("WHILE") {
            sql = Self::normalize_while_blocks(&sql);
        }

        // Normalize FETCH: MySQL uses FETCH cursor INTO var1, var2, ...
        // sqlparser's FETCH only supports single INTO target (sqlparser 0.61).
        // Convert to sentinel SET that execute_procedure_stmt detects at runtime.
        // Uses @__roodb_fetch__ (reserved internal name) as the carrier.
        let sql_upper = sql.to_uppercase();
        if sql_upper.contains("FETCH ") {
            use std::sync::LazyLock;
            static RE_FETCH: LazyLock<Regex> =
                LazyLock::new(|| Regex::new(r"(?i)\bFETCH\s+(\w+)\s+INTO\s+([^;]+)").unwrap());
            sql = RE_FETCH
                .replace_all(&sql, "SET @__roodb_fetch__ = '$1 INTO $2'")
                .to_string();
        }

        // Remove semicolons after inner END blocks inside IF/CASE.
        // MySQL: END; ELSE → sqlparser expects: END ELSE
        // Only targets ELSE/ELSEIF/END IF/END CASE — NOT bare END (which needs its semicolon).
        {
            use std::sync::LazyLock;
            static RE_END_SEMI: LazyLock<Regex> = LazyLock::new(|| {
                Regex::new(r"(?i)\bEND\s*;\s*(ELSE\b|ELSEIF\b|END\s+IF\b|END\s+CASE\b)").unwrap()
            });
            while RE_END_SEMI.is_match(&sql) {
                sql = RE_END_SEMI.replace_all(&sql, "END $1").to_string();
            }
        }

        // Only process DECLARE if it exists
        if !sql_upper.contains("DECLARE ") {
            return sql;
        }

        // Pre-process: split multi-variable DECLARE into individual ones.
        // DECLARE x, y, z INT DEFAULT 0 → DECLARE x INT DEFAULT 0; DECLARE y INT DEFAULT 0; DECLARE z INT DEFAULT 0
        sql = Self::split_multi_var_declare(&sql);

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
            // Remove handler declarations: DECLARE CONTINUE/EXIT HANDLER ...
            // (These are processed separately by extract_handlers)
            if words.len() >= 3
                && (words[1].eq_ignore_ascii_case("CONTINUE")
                    || words[1].eq_ignore_ascii_case("EXIT"))
                && words[2].eq_ignore_ascii_case("HANDLER")
            {
                // Replace the entire DECLARE HANDLER statement + semicolon with empty
                result.push_str(&sql[last_end..start]);
                // Skip past the semicolon
                last_end = if stmt_end < sql.len() && sql.as_bytes()[stmt_end] == b';' {
                    stmt_end + 1
                } else {
                    stmt_end
                };
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
    /// Structurally transform `WHILE cond DO body END WHILE` blocks
    /// into `WHILE cond BEGIN body END;` for sqlparser.
    ///
    /// sqlparser 0.61 doesn't support MySQL's DO/END WHILE keywords.
    /// If the body starts with a nested BEGIN...END block followed by more
    /// statements, the inner block is flattened into the WHILE body.
    fn normalize_while_blocks(sql: &str) -> String {
        let bytes = sql.as_bytes();
        let len = bytes.len();
        let mut result = String::with_capacity(len);
        let mut pos = 0;

        while pos < len {
            // Find next WHILE keyword (word boundary aware)
            let while_pos = match Self::find_keyword(sql, pos, "WHILE") {
                Some(p) => p,
                None => break,
            };
            // Copy text before WHILE
            result.push_str(&sql[pos..while_pos]);

            // Find DO keyword after the WHILE condition
            let do_pos = match Self::find_keyword(sql, while_pos + 5, "DO") {
                Some(p) => p,
                None => {
                    // No DO found — not a WHILE...DO block, copy as-is
                    result.push_str(&sql[while_pos..while_pos + 5]);
                    pos = while_pos + 5;
                    continue;
                }
            };

            // Check: is there an END WHILE between while_pos and do_pos?
            // If so, the DO we found belongs to a deeper context — skip
            if Self::find_keyword_before(sql, while_pos + 5, do_pos, "END").is_some() {
                result.push_str(&sql[while_pos..while_pos + 5]);
                pos = while_pos + 5;
                continue;
            }

            // Extract condition
            let condition = sql[while_pos + 5..do_pos].trim();

            // Find matching END WHILE — track WHILE nesting
            let body_start = do_pos + 2; // skip "DO"
            let end_while_pos = match Self::find_matching_end_while(sql, body_start) {
                Some(p) => p,
                None => {
                    // Malformed — copy as-is
                    result.push_str(&sql[while_pos..while_pos + 5]);
                    pos = while_pos + 5;
                    continue;
                }
            };

            // Extract body between DO and END WHILE
            let body = sql[body_start..end_while_pos].trim();

            // Flatten: if body starts with BEGIN, merge inner block with trailing stmts
            let flat_body = Self::flatten_while_body(body);

            // Emit: WHILE condition BEGIN flat_body END;
            result.push_str("WHILE ");
            result.push_str(condition);
            result.push_str(" BEGIN ");
            result.push_str(&flat_body);
            result.push_str(" END;");

            // Advance past END WHILE + optional semicolon
            let mut skip = end_while_pos;
            // Skip "END" + whitespace + "WHILE"
            let after_end = sql[skip..].trim_start();
            if after_end.to_uppercase().starts_with("END") {
                skip += sql[skip..].len() - after_end.len() + 3; // past "END"
                let after_end2 = sql[skip..].trim_start();
                if after_end2.to_uppercase().starts_with("WHILE") {
                    skip += sql[skip..].len() - after_end2.len() + 5; // past "WHILE"
                }
            }
            // Skip optional semicolon
            let after = sql[skip..].trim_start();
            if after.starts_with(';') {
                skip += sql[skip..].len() - after.len() + 1;
            }
            pos = skip;
        }

        result.push_str(&sql[pos..]);
        result
    }

    /// Find a keyword (case-insensitive, word-boundary aware) starting at `from`.
    fn find_keyword(sql: &str, from: usize, keyword: &str) -> Option<usize> {
        let upper = sql[from..].to_uppercase();
        let kw = keyword.to_uppercase();
        let kw_len = kw.len();
        let mut search_from = 0;
        loop {
            let rel = upper[search_from..].find(&kw)?;
            let abs = from + search_from + rel;
            // Check word boundaries
            let before_ok = abs == 0
                || !sql.as_bytes()[abs - 1].is_ascii_alphanumeric()
                    && sql.as_bytes()[abs - 1] != b'_';
            let after_ok = abs + kw_len >= sql.len()
                || !sql.as_bytes()[abs + kw_len].is_ascii_alphanumeric()
                    && sql.as_bytes()[abs + kw_len] != b'_';
            if before_ok && after_ok {
                // Make sure we're not inside a string literal
                if !Self::is_inside_string(sql, abs) {
                    return Some(abs);
                }
            }
            search_from += rel + kw_len;
        }
    }

    /// Check if position `pos` is inside a single-quoted string literal.
    fn is_inside_string(sql: &str, pos: usize) -> bool {
        let mut in_str = false;
        let bytes = sql.as_bytes();
        let mut i = 0;
        while i < pos && i < bytes.len() {
            if bytes[i] == b'\'' {
                if in_str {
                    // Check escaped quote
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_str = false;
                } else {
                    in_str = true;
                }
            }
            i += 1;
        }
        in_str
    }

    /// Find keyword between `from` and `before` positions.
    fn find_keyword_before(sql: &str, from: usize, before: usize, keyword: &str) -> Option<usize> {
        let result = Self::find_keyword(sql, from, keyword)?;
        if result < before {
            Some(result)
        } else {
            None
        }
    }

    /// Find matching END WHILE for a WHILE body starting at `body_start`.
    /// Tracks WHILE nesting depth to handle nested WHILEs.
    fn find_matching_end_while(sql: &str, body_start: usize) -> Option<usize> {
        use regex::Regex;
        use std::sync::LazyLock;
        // Find all WHILE and END WHILE tokens, process in order
        static RE_END_WHILE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)\bEND\s+WHILE\b").unwrap());
        static RE_WHILE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\bWHILE\b").unwrap());

        let mut depth: i32 = 1; // we're inside one WHILE
        let mut pos = body_start;

        loop {
            // Find next END WHILE
            let ew = RE_END_WHILE
                .find(&sql[pos..])
                .filter(|m| !Self::is_inside_string(sql, pos + m.start()))
                .map(|m| pos + m.start());
            // Find next bare WHILE (not part of END WHILE)
            let w = {
                let mut search = pos;
                loop {
                    match RE_WHILE.find(&sql[search..]) {
                        Some(m) => {
                            let abs = search + m.start();
                            if Self::is_inside_string(sql, abs) {
                                search = abs + 5;
                                continue;
                            }
                            // Check it's not preceded by END (which makes it END WHILE)
                            let before = sql[..abs].trim_end();
                            if before.to_uppercase().ends_with("END") {
                                search = abs + 5;
                                continue;
                            }
                            break Some(abs);
                        }
                        None => break None,
                    }
                }
            };

            // Process whichever comes first
            match (w, ew) {
                (Some(w_pos), Some(ew_pos)) if w_pos < ew_pos => {
                    depth += 1;
                    pos = w_pos + 5;
                }
                (_, Some(ew_pos)) => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(ew_pos);
                    }
                    // Skip past "END WHILE"
                    let m = RE_END_WHILE.find(&sql[ew_pos..]).unwrap();
                    pos = ew_pos + m.end();
                }
                (Some(w_pos), None) => {
                    depth += 1;
                    pos = w_pos + 5;
                }
                (None, None) => return None,
            }
        }
    }

    /// Flatten a WHILE body: if it starts with BEGIN...END; followed by more
    /// statements, remove the inner BEGIN/END to produce a flat statement list.
    fn flatten_while_body(body: &str) -> String {
        let trimmed = body.trim();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("BEGIN") {
            return trimmed.to_string();
        }
        // Check it's BEGIN as keyword
        if upper
            .as_bytes()
            .get(5)
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
        {
            return trimmed.to_string();
        }

        // Find matching END for this inner BEGIN (track nesting)
        let mut depth = 0;
        let mut end_pos = None;
        let mut i = 0;
        let bytes = trimmed.as_bytes();
        let mut in_str = false;
        while i < bytes.len() {
            if in_str {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    in_str = false;
                }
                i += 1;
                continue;
            }
            if bytes[i] == b'\'' {
                in_str = true;
                i += 1;
                continue;
            }
            // Check for BEGIN keyword
            if i + 5 <= bytes.len() && trimmed[i..].to_uppercase().starts_with("BEGIN") {
                let before_ok =
                    i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
                let after_ok = i + 5 >= bytes.len()
                    || !bytes[i + 5].is_ascii_alphanumeric() && bytes[i + 5] != b'_';
                if before_ok && after_ok {
                    depth += 1;
                    i += 5;
                    continue;
                }
            }
            // Check for END keyword (but not END IF, END CASE, END WHILE)
            if i + 3 <= bytes.len() && trimmed[i..].to_uppercase().starts_with("END") {
                let before_ok =
                    i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
                let after_ok = i + 3 >= bytes.len()
                    || !bytes[i + 3].is_ascii_alphanumeric() && bytes[i + 3] != b'_';
                if before_ok && after_ok {
                    // Check it's not END IF / END CASE / END WHILE
                    let after_end = trimmed[i + 3..].trim_start().to_uppercase();
                    if !after_end.starts_with("IF")
                        && !after_end.starts_with("CASE")
                        && !after_end.starts_with("WHILE")
                    {
                        depth -= 1;
                        if depth == 0 {
                            end_pos = Some(i);
                            break;
                        }
                    }
                    i += 3;
                    continue;
                }
            }
            i += 1;
        }

        let Some(end_pos) = end_pos else {
            return trimmed.to_string();
        };

        // Check if there are more statements after the inner END
        let after_inner_end = trimmed[end_pos + 3..].trim_start();
        let after_semi = after_inner_end.strip_prefix(';').unwrap_or(after_inner_end);
        let trailing = after_semi.trim();

        if trailing.is_empty() {
            // Body is just BEGIN...END with nothing after — use contents directly
            let inner = trimmed[5..end_pos].trim();
            inner.to_string()
        } else {
            // Body is BEGIN...END; more_stmts — flatten into contents + trailing
            let inner = trimmed[5..end_pos].trim();
            let mut flat = inner.to_string();
            if !flat.ends_with(';') {
                flat.push(';');
            }
            flat.push(' ');
            flat.push_str(trailing);
            flat
        }
    }

    /// Split multi-variable DECLARE into individual statements.
    /// Public for use by protocol/roodb normalize_declare_stmts.
    /// `DECLARE x, y, z INT DEFAULT 0` → `DECLARE x INT DEFAULT 0; DECLARE y INT DEFAULT 0; DECLARE z INT DEFAULT 0`
    /// Skips DECLARE CURSOR and DECLARE HANDLER which have different syntax.
    pub fn split_multi_var_declare(sql: &str) -> String {
        use regex::Regex;
        use std::sync::LazyLock;
        // Match DECLARE followed by comma-separated names then a type keyword
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"(?i)\bDECLARE\s+((\w+\s*,\s*)+\w+)\s+(INT|INTEGER|BIGINT|SMALLINT|TINYINT|MEDIUMINT|FLOAT|DOUBLE|DECIMAL|NUMERIC|CHAR|VARCHAR|TEXT|BLOB|DATE|TIME|DATETIME|TIMESTAMP|BOOLEAN|BOOL)\b").unwrap()
        });

        if !RE.is_match(sql) {
            return sql.to_string();
        }

        let mut result = sql.to_string();
        // Keep replacing until no more multi-var DECLAREs exist
        loop {
            let Some(caps) = RE.captures(&result) else {
                break;
            };
            let full_match = caps.get(0).unwrap();
            let names_str = &caps[1];
            let type_keyword = &caps[3];

            // Get the rest of the statement after the type keyword (e.g., " DEFAULT 0" or " UNSIGNED")
            let after_type_start = full_match.end();
            // Find the semicolon that ends this statement
            let rest = &result[after_type_start..];
            let semi_pos = rest.find(';').unwrap_or(rest.len());
            let suffix = rest[..semi_pos].to_string();

            let names: Vec<&str> = names_str.split(',').map(|n| n.trim()).collect();
            let replacements: Vec<String> = names
                .iter()
                .map(|name| format!("DECLARE {} {}{}", name, type_keyword, suffix))
                .collect();
            let replacement = replacements.join("; ");

            let start = full_match.start();
            let end = after_type_start + semi_pos;
            result = format!("{}{}{}", &result[..start], replacement, &result[end..]);
        }
        result
    }

    /// Fix DIV associativity in a statement by walking expressions.
    /// sqlparser's MySQL dialect parses `A DIV B / C` as `A DIV (B / C)`.
    /// We need `(A DIV B) / C` (left-associative, same as MySQL).
    fn fix_div_associativity_stmt(stmt: &mut Statement) {
        let _ = sqlparser::ast::visit_expressions_mut(stmt, |expr| {
            Self::fix_div_expr(expr);
            std::ops::ControlFlow::<()>::Continue(())
        });
    }

    /// Fix a single expression: if it's `X DIV (Y op Z)` where op is *, /, %, DIV,
    /// rotate to `(X DIV Y) op Z` (left-associative).
    fn fix_div_expr(expr: &mut sqlparser::ast::Expr) {
        use sqlparser::ast::BinaryOperator as BO;
        use sqlparser::ast::Expr;

        let needs_rotation = matches!(
            expr,
            Expr::BinaryOp {
                op: BO::MyIntegerDivide,
                right,
                ..
            } if matches!(
                right.as_ref(),
                Expr::BinaryOp {
                    op: BO::Multiply | BO::Divide | BO::Modulo | BO::MyIntegerDivide,
                    ..
                }
            )
        );

        if needs_rotation {
            let old = std::mem::replace(expr, Expr::Value(sqlparser::ast::Value::Null.into()));
            if let Expr::BinaryOp {
                left: a,
                op: BO::MyIntegerDivide,
                right: bc_box,
            } = old
            {
                if let Expr::BinaryOp {
                    left: b,
                    op: outer_op,
                    right: c,
                } = *bc_box
                {
                    *expr = Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: a,
                            op: BO::MyIntegerDivide,
                            right: b,
                        }),
                        op: outer_op,
                        right: c,
                    };
                }
            }
        }
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
    fn test_parse_while_with_handler() {
        // WHILE with DO BEGIN compound block + DECLARE HANDLER + extra statement
        let sql = "CREATE PROCEDURE test_round(in arg bigint)\nBEGIN\n  DECLARE i int;\n  SET i = 0;\n  WHILE (i >= -2) DO\n    BEGIN\n      DECLARE CONTINUE HANDLER FOR SQLSTATE '22003' SHOW ERRORS;\n      SELECT arg, i, round(arg, i);\n    END;\n    SET i = i - 1;\n  END WHILE;\nEND";
        Parser::parse_one(sql).expect("Should parse WHILE with handler");
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

    #[test]
    fn test_parse_scientific_notation() {
        // 0e0, 1e199, etc. should parse as valid SQL
        let result = Parser::parse_one("SELECT 0e0, 1e10, 1.5e2");
        assert!(result.is_ok(), "Parse error: {:?}", result.err());
        let result = Parser::parse_one("SELECT 1e199 + 0e0");
        assert!(result.is_ok(), "Parse error: {:?}", result.err());
    }
}
