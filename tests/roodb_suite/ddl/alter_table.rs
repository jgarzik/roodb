//! ALTER TABLE integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

/// A failed ALTER TABLE reports its error and leaves the connection usable.
#[tokio::test]
async fn test_alter_table_error_keeps_the_connection() {
    let server = TestServer::start("alter_table").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE alt_err (id INT PRIMARY KEY, a INT)")
        .await
        .unwrap();

    for sql in [
        "ALTER TABLE alt_no_such_table ADD COLUMN b INT",
        "ALTER TABLE alt_err DROP COLUMN no_such_column",
        "ALTER TABLE alt_err RENAME COLUMN no_such_column TO c",
    ] {
        let err = conn.query_drop(sql).await.expect_err(sql);
        assert!(
            matches!(err, mysql_async::Error::Server(_)),
            "{sql}: expected a server error, got {err:?}"
        );
        let one: Option<i32> = conn
            .query_first("SELECT 1")
            .await
            .unwrap_or_else(|e| panic!("{sql}: the connection is gone: {e}"));
        assert_eq!(one, Some(1));
    }

    conn.query_drop("ALTER TABLE alt_err ADD COLUMN b INT")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO alt_err VALUES (1, 2, 3)")
        .await
        .unwrap();
    conn.query_drop("DROP TABLE alt_err").await.unwrap();
}

fn code(err: &mysql_async::Error) -> u16 {
    match err {
        mysql_async::Error::Server(e) => e.code,
        other => panic!("not a server error: {other}"),
    }
}

async fn index_names(conn: &mut mysql_async::Conn, table: &str) -> Vec<String> {
    let mut names: Vec<String> = conn
        .query::<mysql_async::Row, _>(format!("SHOW INDEX FROM {table}"))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>(2).unwrap())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// A table's indexes are part of the table: altering or renaming the
/// table keeps them.
#[tokio::test]
async fn altering_a_table_keeps_its_indexes() {
    let server = TestServer::start("alter_keeps_indexes").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE alt_idx (id INT PRIMARY KEY, a INT, b INT)")
        .await
        .unwrap();
    conn.query_drop("CREATE INDEX alt_idx_a ON alt_idx (a)")
        .await
        .unwrap();
    conn.query_drop("CREATE INDEX alt_idx_b ON alt_idx (b)")
        .await
        .unwrap();
    conn.query_drop("ALTER TABLE alt_idx ADD COLUMN c INT")
        .await
        .unwrap();
    assert_eq!(
        index_names(&mut conn, "alt_idx").await,
        ["PRIMARY", "alt_idx_a", "alt_idx_b"]
    );
    conn.query_drop("ALTER TABLE alt_idx DROP INDEX alt_idx_a")
        .await
        .unwrap();
    assert_eq!(
        index_names(&mut conn, "alt_idx").await,
        ["PRIMARY", "alt_idx_b"]
    );
    conn.query_drop("ALTER TABLE alt_idx RENAME TO alt_idx2")
        .await
        .unwrap();
    assert_eq!(
        index_names(&mut conn, "alt_idx2").await,
        ["PRIMARY", "alt_idx_b"]
    );
    conn.query_drop("DROP TABLE alt_idx2").await.unwrap();
    server.shutdown().await;
}

/// An alteration that would leave the table with a definition that makes
/// no sense is refused, and the table stays as it was.
#[tokio::test]
async fn an_impossible_alteration_is_refused_and_the_table_stays() {
    let server = TestServer::start("alter_refused").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE alt_bad (id INT, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO alt_bad VALUES (1, 10)")
        .await
        .unwrap();
    assert!(conn
        .query_drop("ALTER TABLE alt_bad ADD PRIMARY KEY (no_such_column)")
        .await
        .is_err());
    assert!(conn
        .query_drop(
            "ALTER TABLE alt_bad ADD CONSTRAINT fk FOREIGN KEY (v) REFERENCES no_such_table (id)"
        )
        .await
        .is_err());
    conn.query_drop("CREATE TABLE alt_taken (id INT)")
        .await
        .unwrap();
    assert!(conn
        .query_drop("ALTER TABLE alt_bad RENAME TO alt_taken")
        .await
        .is_err());
    // Still there, still readable, still alterable.
    let rows: Vec<(i32, i32)> = conn.query("SELECT id, v FROM alt_bad").await.unwrap();
    assert_eq!(rows, [(1, 10)]);
    conn.query_drop("ALTER TABLE alt_bad ADD COLUMN w INT")
        .await
        .unwrap();
    conn.query_drop("DROP TABLE alt_bad, alt_taken")
        .await
        .unwrap();
    server.shutdown().await;
}

/// An index name can be used once; dropping an index that is not there is
/// an error.
#[tokio::test]
async fn index_names_are_checked() {
    let server = TestServer::start("index_names_checked").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE idx_chk (id INT PRIMARY KEY, a INT)")
        .await
        .unwrap();
    conn.query_drop("CREATE INDEX idx_chk_a ON idx_chk (a)")
        .await
        .unwrap();
    let err = conn
        .query_drop("CREATE INDEX idx_chk_a ON idx_chk (a)")
        .await
        .expect_err("an index name was used twice");
    assert_eq!(code(&err), 1061, "{err}");
    let err = conn
        .query_drop("DROP INDEX idx_chk_none")
        .await
        .expect_err("an index that is not there was dropped");
    assert_eq!(code(&err), 1091, "{err}");
    conn.query_drop("DROP INDEX idx_chk_a").await.unwrap();
    assert_eq!(index_names(&mut conn, "idx_chk").await, ["PRIMARY"]);
    conn.query_drop("DROP TABLE idx_chk").await.unwrap();
    server.shutdown().await;
}
