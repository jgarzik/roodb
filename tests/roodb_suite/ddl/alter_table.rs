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

/// A renamed table is the same table: its rows, its keys and its
/// AUTO_INCREMENT counter go with it, and its old name is free.
#[tokio::test]
async fn a_renamed_table_keeps_its_rows_and_its_counter() {
    let server = TestServer::start("alter_rename_rows").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE ren_a (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ren_a (v) VALUES (10), (20), (30)")
        .await
        .unwrap();
    conn.query_drop("ALTER TABLE ren_a RENAME TO ren_b")
        .await
        .unwrap();

    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, v FROM ren_b ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, [(1, 10), (2, 20), (3, 30)]);
    let by_key: Option<i32> = conn
        .query_first("SELECT v FROM ren_b WHERE id = 2")
        .await
        .unwrap();
    assert_eq!(by_key, Some(20));
    conn.query_drop("UPDATE ren_b SET v = 21 WHERE id = 2")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ren_b (v) VALUES (40)")
        .await
        .unwrap();
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, v FROM ren_b ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, [(1, 10), (2, 21), (3, 30), (4, 40)]);
    assert!(conn.query_drop("SELECT * FROM ren_a").await.is_err());

    // The old name is another table's to take, and that table is empty and
    // counts from 1.
    conn.query_drop("CREATE TABLE ren_a (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ren_a (v) VALUES (99)")
        .await
        .unwrap();
    let rows: Vec<(i32, i32)> = conn.query("SELECT id, v FROM ren_a").await.unwrap();
    assert_eq!(rows, [(1, 99)]);
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT id, v FROM ren_b ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
    conn.query_drop("DROP TABLE ren_a, ren_b").await.unwrap();
    server.shutdown().await;
}

/// The rows of `sql`, each as its values' text (`NULL` for a null).
async fn rows(conn: &mut mysql_async::Conn, sql: &str) -> Vec<Vec<String>> {
    let rows: Vec<mysql_async::Row> = conn
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    rows.into_iter()
        .map(|row| {
            (0..row.len())
                .map(|i| match row.as_ref(i).unwrap() {
                    mysql_async::Value::NULL => "NULL".to_string(),
                    mysql_async::Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect()
        })
        .collect()
}

fn texts(expected: &[&[&str]]) -> Vec<Vec<String>> {
    expected
        .iter()
        .map(|row| row.iter().map(|value| value.to_string()).collect())
        .collect()
}

/// Columns are added, dropped, moved and renamed without touching a row:
/// the rows that were there read as they should under every definition,
/// and still do after a restart.
#[tokio::test]
async fn columns_change_and_the_rows_stay_right() {
    use crate::test_utils::process::ServerProcess;

    let mut server = ServerProcess::init(&[]);
    server.start();
    let mut conn = mysql_async::Conn::new(server.opts()).await.unwrap();
    conn.query_drop("CREATE TABLE alt_cols (id INT PRIMARY KEY, a INT, b VARCHAR(10))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO alt_cols VALUES (1, 10, 'x'), (2, 20, 'y')")
        .await
        .unwrap();
    let all = "SELECT * FROM alt_cols ORDER BY 1";

    // Added columns: a row older than the column reads as the default the
    // column was added with; NULL where it has none; the type's own
    // default where it has none and cannot be NULL.
    for sql in [
        "ALTER TABLE alt_cols ADD COLUMN c INT DEFAULT 7",
        "ALTER TABLE alt_cols ADD COLUMN d VARCHAR(5)",
        "ALTER TABLE alt_cols ADD COLUMN e INT NOT NULL",
    ] {
        conn.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        rows(&mut conn, all).await,
        texts(&[
            &["1", "10", "x", "7", "NULL", "0"],
            &["2", "20", "y", "7", "NULL", "0"],
        ])
    );
    // A NULL stored in a column that has a default stays NULL, and a new
    // row has what it was given.
    conn.query_drop("UPDATE alt_cols SET c = NULL WHERE id = 1")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO alt_cols VALUES (3, 30, 'z', 8, 'dd', 9)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO alt_cols (id, a, b, e) VALUES (4, 40, 'w', 1)")
        .await
        .unwrap();
    assert_eq!(
        rows(&mut conn, all).await,
        texts(&[
            &["1", "10", "x", "NULL", "NULL", "0"],
            &["2", "20", "y", "7", "NULL", "0"],
            &["3", "30", "z", "8", "dd", "9"],
            &["4", "40", "w", "7", "NULL", "1"],
        ])
    );

    // Placed first and in the middle; an existing column moved.
    for sql in [
        "ALTER TABLE alt_cols ADD COLUMN f INT DEFAULT 1 FIRST",
        "ALTER TABLE alt_cols ADD COLUMN g INT AFTER a",
        "ALTER TABLE alt_cols MODIFY COLUMN e INT NOT NULL AFTER id",
    ] {
        conn.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(
        rows(&mut conn, "SELECT * FROM alt_cols WHERE id = 3").await,
        texts(&[&["1", "3", "9", "30", "NULL", "z", "8", "dd"]])
    );

    // Dropped: the column's values are gone, and a column added under its
    // name afterwards is a new one that has none of them.
    conn.query_drop("ALTER TABLE alt_cols DROP COLUMN a")
        .await
        .unwrap();
    conn.query_drop("ALTER TABLE alt_cols ADD COLUMN a INT")
        .await
        .unwrap();
    assert_eq!(
        rows(&mut conn, "SELECT id, a, b, c FROM alt_cols ORDER BY id").await,
        texts(&[
            &["1", "NULL", "x", "NULL"],
            &["2", "NULL", "y", "7"],
            &["3", "NULL", "z", "8"],
            &["4", "NULL", "w", "7"],
        ])
    );

    // Renamed, the key column too: rows are found by the key under its
    // new name, and written.
    for sql in [
        "ALTER TABLE alt_cols RENAME COLUMN b TO bb",
        "ALTER TABLE alt_cols RENAME COLUMN id TO idd",
        "ALTER TABLE alt_cols CHANGE COLUMN c cc BIGINT DEFAULT 7",
        "ALTER TABLE alt_cols MODIFY COLUMN bb VARCHAR(20)",
    ] {
        conn.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    conn.query_drop("UPDATE alt_cols SET bb = 'a longer value' WHERE idd = 2")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO alt_cols (idd, e, bb) VALUES (5, 5, 'v')")
        .await
        .unwrap();
    let expected = texts(&[
        &["1", "x", "NULL"],
        &["2", "a longer value", "7"],
        &["3", "z", "8"],
        &["4", "w", "7"],
        &["5", "v", "7"],
    ]);
    let narrow = "SELECT idd, bb, cc FROM alt_cols ORDER BY idd";
    assert_eq!(rows(&mut conn, narrow).await, expected);
    assert_eq!(
        rows(&mut conn, "SELECT bb FROM alt_cols WHERE idd = 4").await,
        texts(&[&["w"]])
    );
    let err = conn
        .query_drop("INSERT INTO alt_cols (idd, e) VALUES (5, 0)")
        .await
        .expect_err("a second row with the key 5");
    assert!(
        matches!(err, mysql_async::Error::Server(ref e) if e.code == 1062),
        "{err}"
    );
    let wide = rows(&mut conn, "SELECT * FROM alt_cols ORDER BY idd").await;
    drop(conn);

    // All of it from storage.
    server.kill();
    server.start();
    let mut conn = mysql_async::Conn::new(server.opts()).await.unwrap();
    assert_eq!(rows(&mut conn, narrow).await, expected);
    assert_eq!(
        rows(&mut conn, "SELECT * FROM alt_cols ORDER BY idd").await,
        wide
    );
    // A column added now gets a number none before it had.
    conn.query_drop("ALTER TABLE alt_cols ADD COLUMN h INT DEFAULT 3")
        .await
        .unwrap();
    assert_eq!(
        rows(&mut conn, "SELECT idd, h, e FROM alt_cols WHERE idd = 3").await,
        texts(&[&["3", "3", "9"]])
    );
}

/// A change that needs the table's rows rewritten or stored under other
/// keys is refused (until the online rebuild does it), and leaves the
/// table as it was.
#[tokio::test]
async fn a_change_that_needs_the_rows_rewritten_is_refused() {
    let server = TestServer::start("alter_rebuild").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE alt_rebuild (id INT PRIMARY KEY, n INT, v VARCHAR(10))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO alt_rebuild VALUES (1, 5, 'five')")
        .await
        .unwrap();
    for sql in [
        // The stored values do not fit the new type.
        "ALTER TABLE alt_rebuild MODIFY COLUMN v INT",
        "ALTER TABLE alt_rebuild MODIFY COLUMN n SMALLINT",
        "ALTER TABLE alt_rebuild MODIFY COLUMN v VARCHAR(5)",
        "ALTER TABLE alt_rebuild CHANGE COLUMN n m VARCHAR(10)",
        // The rows would be stored under other keys.
        "ALTER TABLE alt_rebuild DROP PRIMARY KEY",
        "ALTER TABLE alt_rebuild DROP COLUMN id",
        "ALTER TABLE alt_rebuild ADD COLUMN seq INT AUTO_INCREMENT",
    ] {
        let err = conn.query_drop(sql).await.expect_err(sql);
        assert!(
            matches!(err, mysql_async::Error::Server(ref e) if e.code == 1235),
            "{sql}: {err}"
        );
    }
    conn.query_drop("CREATE TABLE alt_rebuild_nokey (n INT, v VARCHAR(10))")
        .await
        .unwrap();
    let sql = "ALTER TABLE alt_rebuild_nokey ADD PRIMARY KEY (n)";
    let err = conn.query_drop(sql).await.expect_err(sql);
    assert!(
        matches!(err, mysql_async::Error::Server(ref e) if e.code == 1235),
        "{sql}: {err}"
    );

    assert_eq!(
        rows(&mut conn, "SELECT * FROM alt_rebuild WHERE id = 1").await,
        texts(&[&["1", "5", "five"]])
    );
    conn.query_drop("DROP TABLE alt_rebuild").await.unwrap();
    conn.query_drop("DROP TABLE alt_rebuild_nokey")
        .await
        .unwrap();
    server.shutdown().await;
}
