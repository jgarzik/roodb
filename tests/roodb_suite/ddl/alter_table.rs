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
