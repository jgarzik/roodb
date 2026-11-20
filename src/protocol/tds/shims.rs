//! Driver shims: the small amount of T-SQL that SQL Server drivers send on
//! their own, translated to what RooDB understands.
//!
//! RooDB's SQL dialect is MySQL's, and the TDS endpoint does not change
//! that. But drivers issue a few statements themselves, to set up a
//! session and to manage transactions, and without them a connection
//! cannot be established or used:
//!
//! - several statements in one batch;
//! - `BEGIN TRAN`, `COMMIT TRAN`, `ROLLBACK TRAN` and the guarded form
//!   `IF @@TRANCOUNT > 0 ...`;
//! - `SET IMPLICIT_TRANSACTIONS`, and session options that have no
//!   counterpart here and are accepted without effect;
//! - probes such as `@@SPID`, `@@TRANCOUNT`, `SERVERPROPERTY(...)`,
//!   `DB_NAME()`.
//!
//! Anything else is passed to the engine unchanged.

/// Session options drivers set at connect time that have no effect here.
/// Each describes SQL Server behaviour (ANSI semantics, message
/// formatting, limits) that either already matches or does not apply.
const ACCEPTED_SET_OPTIONS: &[&str] = &[
    "ANSI_DEFAULTS",
    "ANSI_NULLS",
    "ANSI_NULL_DFLT_ON",
    "ANSI_NULL_DFLT_OFF",
    "ANSI_PADDING",
    "ANSI_WARNINGS",
    "ARITHABORT",
    "ARITHIGNORE",
    "CONCAT_NULL_YIELDS_NULL",
    "CURSOR_CLOSE_ON_COMMIT",
    "DATEFIRST",
    "DATEFORMAT",
    "DEADLOCK_PRIORITY",
    "FMTONLY",
    "LANGUAGE",
    "LOCK_TIMEOUT",
    "NOCOUNT",
    "NOEXEC",
    "NO_BROWSETABLE",
    "NUMERIC_ROUNDABORT",
    "QUERY_GOVERNOR_COST_LIMIT",
    "QUOTED_IDENTIFIER",
    "ROWCOUNT",
    "TEXTSIZE",
    "XACT_ABORT",
];

/// What the session looks like, for probes and guarded statements.
pub struct ShimContext<'a> {
    pub connection_id: u32,
    pub in_transaction: bool,
    pub database: &'a str,
    pub user: &'a str,
    /// Product version, "major.minor.build"
    pub product_version: &'a str,
}

/// How to carry out one statement of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Run this SQL in the engine
    Run(String),
    /// Nothing to do; report success
    Accept,
    /// Refuse with this message
    Refuse(&'static str),
}

/// Which lexical region a character belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Region {
    Code,
    Quoted,
    Comment,
}

/// Classify every character of `sql` as code, quoted text (strings and
/// quoted identifiers, including their quotes) or comment.
fn regions(sql: &str) -> Vec<(char, Region)> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\'' | '"' | '`' => {
                out.push((c, Region::Quoted));
                i += 1;
                while i < chars.len() {
                    let q = chars[i];
                    out.push((q, Region::Quoted));
                    i += 1;
                    if q == '\\' && c != '`' {
                        if i < chars.len() {
                            out.push((chars[i], Region::Quoted));
                            i += 1;
                        }
                    } else if q == c {
                        if chars.get(i) == Some(&c) {
                            out.push((c, Region::Quoted));
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() {
                    out.push((chars[i], Region::Comment));
                    i += 1;
                    if chars[i - 1] == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.get(i + 1) == Some(&'*') => {
                out.push(('/', Region::Comment));
                out.push(('*', Region::Comment));
                i += 2;
                while i < chars.len() {
                    out.push((chars[i], Region::Comment));
                    i += 1;
                    if chars[i - 1] == '/' && chars[i - 2] == '*' && out.len() > 3 {
                        // "/*/" is not a complete comment
                        let body_start = out
                            .iter()
                            .rposition(|(_, r)| *r != Region::Comment)
                            .map_or(0, |p| p + 1);
                        if out.len() - body_start >= 4 {
                            break;
                        }
                    }
                }
            }
            _ => {
                out.push((c, Region::Code));
                i += 1;
            }
        }
    }
    out
}

/// Split a batch into statements at top-level semicolons.
///
/// A routine definition (`CREATE PROCEDURE/FUNCTION/TRIGGER`) contains
/// semicolons in its body and is kept whole.
pub fn split_batch(sql: &str) -> Vec<String> {
    let words: Vec<String> = sql
        .split_whitespace()
        .take(8)
        .map(str::to_uppercase)
        .collect();
    let defines_routine = words.first().is_some_and(|w| w == "CREATE")
        && words
            .iter()
            .any(|w| matches!(w.as_str(), "PROCEDURE" | "FUNCTION" | "TRIGGER"));
    if defines_routine {
        return vec![sql.to_string()];
    }

    let mut statements = Vec::new();
    let mut current = String::new();
    let mut has_code = false;
    for (c, region) in regions(sql) {
        if region == Region::Code && c == ';' {
            if has_code {
                statements.push(current.trim().to_string());
            }
            current.clear();
            has_code = false;
        } else {
            current.push(c);
            has_code |= region != Region::Comment && !c.is_whitespace();
        }
    }
    if has_code {
        statements.push(current.trim().to_string());
    }
    if statements.is_empty() {
        // Nothing but whitespace, comments and semicolons: let the engine
        // answer as it does for an empty statement.
        statements.push(sql.to_string());
    }
    statements
}

/// Escape server-side text for use as a string literal.
fn literal(text: &str) -> String {
    let escaped = text.replace('\\', "\\\\").replace('\'', "''");
    format!("'{escaped}'")
}

/// The literal a `SERVERPROPERTY` name evaluates to.
fn server_property(name: &str, ctx: &ShimContext<'_>) -> String {
    match name.to_ascii_uppercase().as_str() {
        "PRODUCTVERSION" => literal(ctx.product_version),
        "PRODUCTLEVEL" => literal("RTM"),
        "EDITION" => literal("RooDB"),
        "ENGINEEDITION" => "3".to_string(),
        "SERVERNAME" | "MACHINENAME" => literal("RooDB"),
        "PRODUCTMAJORVERSION" => literal(ctx.product_version.split('.').next().unwrap_or_default()),
        "COLLATION" => literal("SQL_Latin1_General_CP1_CI_AS"),
        "ISCLUSTERED" | "ISHADRENABLED" | "ISINTEGRATEDSECURITYONLY" => "0".to_string(),
        _ => "NULL".to_string(),
    }
}

/// Parse `'name' )` starting at `start`: optional whitespace, one string
/// literal without escapes or embedded quotes, optional whitespace and a
/// closing parenthesis. Returns the name and the position after the `)`.
fn property_argument(marked: &[(char, Region)], start: usize) -> Option<(String, usize)> {
    let skip_space = |mut at: usize| {
        while matches!(marked.get(at), Some((c, Region::Code)) if c.is_whitespace()) {
            at += 1;
        }
        at
    };

    let open = skip_space(start);
    if marked.get(open) != Some(&('\'', Region::Quoted)) {
        return None;
    }
    // The literal is the whole run of quoted characters.
    let mut close = open + 1;
    while matches!(marked.get(close), Some((_, Region::Quoted))) {
        close += 1;
    }
    let literal: String = marked[open..close].iter().map(|(c, _)| *c).collect();
    let name = literal.strip_prefix('\'')?.strip_suffix('\'')?;
    if literal.len() < 2 || name.contains(['\'', '\\']) {
        return None;
    }

    let paren = skip_space(close);
    if marked.get(paren) != Some(&(')', Region::Code)) {
        return None;
    }
    Some((name.to_string(), paren + 1))
}

/// Replace SQL Server probes that RooDB has no equivalent for with their
/// values. Only code is rewritten; strings, quoted identifiers and
/// comments are left alone.
fn rewrite_probes(sql: &str, ctx: &ShimContext<'_>) -> String {
    let marked = regions(sql);
    let is_word = |c: char| c.is_alphanumeric() || matches!(c, '_' | '@' | '$' | '#');
    // Case-insensitive match of `probe` in code at position `at`, ending
    // on a word boundary.
    let matches_at = |at: usize, probe: &str| -> bool {
        let len = probe.chars().count();
        if at + len > marked.len() {
            return false;
        }
        let same = probe
            .chars()
            .zip(&marked[at..at + len])
            .all(|(p, (c, r))| *r == Region::Code && c.to_ascii_uppercase() == p);
        let ends = probe.ends_with(')') || marked.get(at + len).is_none_or(|(c, _)| !is_word(*c));
        same && ends
    };

    let probes: [(&str, String); 11] = [
        ("@@SPID", ctx.connection_id.to_string()),
        ("@@TRANCOUNT", u8::from(ctx.in_transaction).to_string()),
        ("@@MAX_PRECISION", "38".to_string()),
        ("@@SERVERNAME", literal("RooDB")),
        ("@@LANGUAGE", literal("us_english")),
        ("@@MICROSOFTVERSION", "0x10000000".to_string()),
        ("DB_NAME()", literal(ctx.database)),
        ("SUSER_SNAME()", literal(ctx.user)),
        ("SUSER_NAME()", literal(ctx.user)),
        ("ORIGINAL_LOGIN()", literal(ctx.user)),
        ("SCHEMA_NAME()", literal("dbo")),
    ];

    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    'scan: while i < marked.len() {
        let (c, region) = marked[i];
        let at_word_start = i == 0 || !is_word(marked[i - 1].0) || marked[i - 1].1 != Region::Code;
        if region == Region::Code && at_word_start {
            for (probe, value) in &probes {
                if matches_at(i, probe) {
                    out.push_str(value);
                    i += probe.chars().count();
                    continue 'scan;
                }
            }
            // SERVERPROPERTY('name'): the argument must be exactly one
            // plain string literal, as the lexical scan sees it.
            if matches_at(i, "SERVERPROPERTY(") {
                if let Some((name, end)) = property_argument(&marked, i + 15) {
                    out.push_str(&server_property(&name, ctx));
                    i = end;
                    continue 'scan;
                }
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Decide how to carry out one statement.
pub fn translate(statement: &str, ctx: &ShimContext<'_>) -> Action {
    // Compare on upper-cased words, with `>` split off so that
    // `@@TRANCOUNT>0` and `@@TRANCOUNT > 0` read the same.
    let spaced = statement.trim().trim_end_matches(';').replace('>', " > ");
    let upper: Vec<String> = spaced.split_whitespace().map(str::to_uppercase).collect();
    let words: Vec<&str> = upper.iter().map(String::as_str).collect();
    let is_tran = |w: &str| matches!(w, "TRAN" | "TRANSACTION");

    match words.as_slice() {
        // Transactions, optionally named
        ["BEGIN", t, ..] if is_tran(t) => Action::Run("BEGIN".to_string()),
        ["COMMIT"] | ["COMMIT", "WORK"] => Action::Run("COMMIT".to_string()),
        ["COMMIT", t, ..] if is_tran(t) => Action::Run("COMMIT".to_string()),
        ["ROLLBACK"] | ["ROLLBACK", "WORK"] => Action::Run("ROLLBACK".to_string()),
        ["ROLLBACK", t] if is_tran(t) => Action::Run("ROLLBACK".to_string()),
        // A named rollback is a rollback to a savepoint
        ["ROLLBACK", t, _name] if is_tran(t) => Action::Refuse("Savepoints are not supported"),
        ["SAVE", t, ..] if is_tran(t) => Action::Refuse("Savepoints are not supported"),

        // `IF @@TRANCOUNT > 0 <statement>`
        ["IF", "@@TRANCOUNT", ">", "0", rest @ ..] if !rest.is_empty() => {
            if ctx.in_transaction {
                translate(&rest.join(" "), ctx)
            } else {
                Action::Accept
            }
        }

        // Implicit transactions are the inverse of autocommit
        ["SET", "IMPLICIT_TRANSACTIONS", "ON"] => Action::Run("SET autocommit = 0".to_string()),
        ["SET", "IMPLICIT_TRANSACTIONS", "OFF"] => Action::Run("SET autocommit = 1".to_string()),

        // Snapshot isolation is served by the repeatable-read snapshot
        ["SET", "TRANSACTION", "ISOLATION", "LEVEL", "SNAPSHOT"] => {
            Action::Run("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ".to_string())
        }

        ["SET", option, ..] if ACCEPTED_SET_OPTIONS.contains(option) => Action::Accept,

        _ => Action::Run(rewrite_probes(statement, ctx)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(in_transaction: bool) -> ShimContext<'static> {
        ShimContext {
            connection_id: 42,
            in_transaction,
            database: "test",
            user: "root",
            product_version: "16.0.1000",
        }
    }

    fn run(sql: &str) -> Action {
        Action::Run(sql.to_string())
    }

    #[test]
    fn batches_split_at_top_level_semicolons() {
        assert_eq!(split_batch("SELECT 1"), ["SELECT 1"]);
        assert_eq!(split_batch("SELECT 1;"), ["SELECT 1"]);
        assert_eq!(
            split_batch("SET ANSI_NULLS ON; SET NOCOUNT ON ;\nSELECT 1"),
            ["SET ANSI_NULLS ON", "SET NOCOUNT ON", "SELECT 1"]
        );
        // Semicolons in strings, identifiers and comments do not split
        assert_eq!(
            split_batch("SELECT 'a;b', \"c;d\", `e;f` -- x;y\n; SELECT /* ; */ 2"),
            ["SELECT 'a;b', \"c;d\", `e;f` -- x;y", "SELECT /* ; */ 2"]
        );
        assert_eq!(split_batch(r"SELECT 'it\'s; fine'; SELECT 2").len(), 2);
        // Empty statements are dropped
        assert_eq!(split_batch(";; SELECT 1 ;;"), ["SELECT 1"]);
        // A comment on its own is not a statement
        assert_eq!(split_batch("SELECT 1; -- done"), ["SELECT 1"]);
    }

    #[test]
    fn routine_definitions_are_not_split() {
        let procedure = "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END";
        assert_eq!(split_batch(procedure), [procedure]);
        let trigger = "create trigger t after insert on x for each row begin set @a = 1; end";
        assert_eq!(split_batch(trigger), [trigger]);
    }

    #[test]
    fn empty_batches_reach_the_engine() {
        assert_eq!(split_batch(""), [""]);
        assert_eq!(split_batch(" ; "), [" ; "]);
        assert_eq!(split_batch("-- only a comment"), ["-- only a comment"]);
    }

    #[test]
    fn transaction_statements() {
        for sql in ["BEGIN TRAN", "begin transaction", "BEGIN TRAN t1;"] {
            assert_eq!(translate(sql, &ctx(false)), run("BEGIN"), "{sql}");
        }
        for sql in [
            "COMMIT",
            "COMMIT TRAN",
            "commit transaction t1",
            "COMMIT WORK",
        ] {
            assert_eq!(translate(sql, &ctx(true)), run("COMMIT"), "{sql}");
        }
        for sql in ["ROLLBACK", "ROLLBACK TRAN", "rollback transaction"] {
            assert_eq!(translate(sql, &ctx(true)), run("ROLLBACK"), "{sql}");
        }
        // Savepoints would silently become full rollbacks: refused instead
        assert!(matches!(
            translate("ROLLBACK TRAN sp1", &ctx(true)),
            Action::Refuse(_)
        ));
        assert!(matches!(
            translate("SAVE TRANSACTION sp1", &ctx(true)),
            Action::Refuse(_)
        ));
        // Plain BEGIN / START TRANSACTION are RooDB SQL and pass through
        assert_eq!(translate("BEGIN", &ctx(false)), run("BEGIN"));
        assert_eq!(
            translate("START TRANSACTION", &ctx(false)),
            run("START TRANSACTION")
        );
    }

    #[test]
    fn guarded_transaction_end() {
        for sql in [
            "IF @@TRANCOUNT > 0 COMMIT TRAN",
            "if @@trancount>0 commit",
            "IF @@TRANCOUNT > 0 COMMIT TRANSACTION;",
        ] {
            assert_eq!(translate(sql, &ctx(true)), run("COMMIT"), "{sql}");
            assert_eq!(translate(sql, &ctx(false)), Action::Accept, "{sql}");
        }
        assert_eq!(
            translate("IF @@TRANCOUNT > 0 ROLLBACK TRAN", &ctx(true)),
            run("ROLLBACK")
        );
        assert_eq!(
            translate("IF @@TRANCOUNT > 0 ROLLBACK TRAN", &ctx(false)),
            Action::Accept
        );
    }

    #[test]
    fn session_options() {
        for sql in [
            "SET ANSI_NULLS ON",
            "set quoted_identifier on",
            "SET NOCOUNT OFF;",
            "SET TEXTSIZE 2147483647",
            "SET LOCK_TIMEOUT -1",
            "SET DATEFORMAT mdy",
            "SET XACT_ABORT ON",
        ] {
            assert_eq!(translate(sql, &ctx(false)), Action::Accept, "{sql}");
        }
        assert_eq!(
            translate("SET IMPLICIT_TRANSACTIONS ON", &ctx(false)),
            run("SET autocommit = 0")
        );
        assert_eq!(
            translate("set implicit_transactions off", &ctx(false)),
            run("SET autocommit = 1")
        );
        assert_eq!(
            translate("SET TRANSACTION ISOLATION LEVEL SNAPSHOT", &ctx(false)),
            run("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        );
        // Options the server does know, and unknown ones, go to the engine
        for sql in [
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET @x = 1",
            "SET autocommit = 0",
            "SET NO_SUCH_OPTION ON",
        ] {
            assert_eq!(translate(sql, &ctx(false)), run(sql), "{sql}");
        }
    }

    #[test]
    fn probes_are_replaced_in_code_only() {
        let c = ctx(true);
        assert_eq!(translate("SELECT @@SPID", &c), run("SELECT 42"));
        assert_eq!(
            translate("select @@spid, @@TRANCOUNT", &c),
            run("select 42, 1")
        );
        assert_eq!(
            translate("SELECT @@TRANCOUNT", &ctx(false)),
            run("SELECT 0")
        );
        assert_eq!(
            translate("SELECT DB_NAME(), SUSER_SNAME(), @@SERVERNAME", &c),
            run("SELECT 'test', 'root', 'RooDB'")
        );
        assert_eq!(
            translate("SELECT SERVERPROPERTY('ProductVersion')", &c),
            run("SELECT '16.0.1000'")
        );
        assert_eq!(
            translate("SELECT SERVERPROPERTY( 'EngineEdition' ) AS e", &c),
            run("SELECT 3 AS e")
        );
        assert_eq!(
            translate("SELECT SERVERPROPERTY('NoSuchProperty')", &c),
            run("SELECT NULL")
        );
        // The argument must be one plain literal as the SQL lexer sees it.
        // An escaped quote keeps the string open, so these are not
        // rewritten and nothing inside them turns into code.
        for sql in [
            r"SELECT SERVERPROPERTY('a\') , 'x'",
            "SELECT SERVERPROPERTY('a''b')",
            "SELECT SERVERPROPERTY('a' 'b')",
            "SELECT SERVERPROPERTY('a'), 1) FROM t",
            "SELECT SERVERPROPERTY(\"ProductVersion\")",
            "SELECT SERVERPROPERTY('Edition' /* c */ )",
        ] {
            let Action::Run(out) = translate(sql, &c) else {
                panic!("{sql}");
            };
            if sql.contains("'a'), 1)") {
                assert_eq!(out, "SELECT NULL, 1) FROM t");
            } else {
                assert_eq!(out, sql, "{sql}");
            }
        }
        // Inside strings, identifiers and comments nothing changes
        for sql in [
            "SELECT '@@SPID', \"DB_NAME()\", `@@TRANCOUNT`",
            "SELECT 1 -- @@SPID",
            "SELECT /* @@SPID */ 1",
        ] {
            assert_eq!(translate(sql, &c), run(sql), "{sql}");
        }
        // Longer names that merely contain a probe are not probes
        for sql in ["SELECT @@SPIDER", "SELECT MY_DB_NAME()", "SELECT x@@SPID"] {
            assert_eq!(translate(sql, &c), run(sql), "{sql}");
        }
        // @@VERSION is a RooDB system variable and is left to the engine
        assert_eq!(translate("SELECT @@VERSION", &c), run("SELECT @@VERSION"));
    }

    #[test]
    fn session_values_are_escaped_as_literals() {
        let c = ShimContext {
            connection_id: 1,
            in_transaction: false,
            database: "we'ird\\db",
            user: "o'brien",
            product_version: "16.0.1000",
        };
        assert_eq!(
            translate("SELECT DB_NAME(), SUSER_NAME()", &c),
            run("SELECT 'we''ird\\\\db', 'o''brien'")
        );
    }

    #[test]
    fn odd_input_does_not_panic() {
        let c = ctx(false);
        for sql in [
            "",
            ";",
            "'",
            "\"",
            "`",
            "/*",
            "/*/",
            "--",
            "SERVERPROPERTY(",
            "SERVERPROPERTY('",
            "SELECT SERVERPROPERTY(x)",
            "IF @@TRANCOUNT > 0",
            "@@",
            "SET",
            "\u{1F600}@@SPID",
        ] {
            let _ = translate(sql, &c);
            let _ = split_batch(sql);
        }
    }
}
