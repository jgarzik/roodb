//! Primary keys of every type sort as their values do: a range read on the
//! key returns the rows in the range, whatever the key's type.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

async fn keys(conn: &mut mysql_async::Conn, sql: &str) -> Vec<String> {
    let rows: Vec<mysql_async::Row> = conn.query(sql).await.unwrap();
    let mut out: Vec<String> = rows
        .into_iter()
        .map(|row| {
            let value: mysql_async::Value = row.get(0).unwrap();
            match value {
                mysql_async::Value::Bytes(bytes) => String::from_utf8(bytes).unwrap(),
                other => format!("{other:?}"),
            }
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn a_range_on_a_double_key_returns_the_rows_in_it() {
    let server = TestServer::start("key_order_double").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE ko_double (k DOUBLE PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop(
        "INSERT INTO ko_double VALUES (-250.5, 1), (-2.5, 2), (-1, 3), (0.5, 4), (3, 5), (100.25, 6)",
    )
    .await
    .unwrap();
    assert_eq!(
        keys(
            &mut conn,
            "SELECT v FROM ko_double WHERE k BETWEEN -3 AND 50"
        )
        .await,
        ["2", "3", "4", "5"]
    );
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_double WHERE k < 0").await,
        ["1", "2", "3"]
    );
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_double WHERE k >= 3").await,
        ["5", "6"]
    );
    conn.query_drop("DROP TABLE ko_double").await.unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn a_range_on_a_decimal_key_returns_the_rows_in_it() {
    let server = TestServer::start("key_order_decimal").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE ko_decimal (k DECIMAL(12,2) PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop(
        "INSERT INTO ko_decimal VALUES (-1000.50, 1), (-10.25, 2), (-0.01, 3), (0, 4), (0.99, 5), (7, 6), (65536.00, 7), (1000000.10, 8)",
    )
    .await
    .unwrap();
    assert_eq!(
        keys(
            &mut conn,
            "SELECT v FROM ko_decimal WHERE k BETWEEN -11 AND 8"
        )
        .await,
        ["2", "3", "4", "5", "6"]
    );
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_decimal WHERE k > 7").await,
        ["7", "8"]
    );
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_decimal WHERE k < -0.5").await,
        ["1", "2"]
    );
    conn.query_drop("DROP TABLE ko_decimal").await.unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn a_range_on_a_timestamp_key_returns_the_rows_in_it() {
    let server = TestServer::start("key_order_timestamp").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE ko_ts (k TIMESTAMP PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop(
        "INSERT INTO ko_ts VALUES ('1970-01-02 00:00:00', 1), ('1999-12-31 23:59:59', 2), ('2000-01-01 00:00:00', 3), ('2024-06-15 12:00:00', 4), ('2037-01-01 00:00:00', 5)",
    )
    .await
    .unwrap();
    assert_eq!(
        keys(
            &mut conn,
            "SELECT v FROM ko_ts WHERE k BETWEEN '1999-01-01 00:00:00' AND '2030-01-01 00:00:00'"
        )
        .await,
        ["2", "3", "4"]
    );
    assert_eq!(
        keys(
            &mut conn,
            "SELECT v FROM ko_ts WHERE k < '2000-01-01 00:00:00'"
        )
        .await,
        ["1", "2"]
    );
    conn.query_drop("DROP TABLE ko_ts").await.unwrap();
    server.shutdown().await;
}

/// A value looked up by key is put in the key column's type first: a whole
/// number written as a decimal finds its row, and a bound with a fraction
/// moves to the next whole key.
#[tokio::test]
async fn a_lookup_value_of_another_type_finds_the_key() {
    let server = TestServer::start("key_order_lookup").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE ko_int (k INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ko_int VALUES (-2, 1), (1, 2), (2, 3), (3, 4), (4, 5)")
        .await
        .unwrap();
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_int WHERE k = 3.0").await,
        ["4"]
    );
    assert!(keys(&mut conn, "SELECT v FROM ko_int WHERE k = 2.5")
        .await
        .is_empty());
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_int WHERE k > 2.5").await,
        ["4", "5"]
    );
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_int WHERE k < 2.5").await,
        ["1", "2", "3"]
    );
    assert_eq!(
        keys(
            &mut conn,
            "SELECT v FROM ko_int WHERE k BETWEEN -2.5 AND 1.5"
        )
        .await,
        ["1", "2"]
    );
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_int WHERE k >= 2.0").await,
        ["3", "4", "5"]
    );
    // Beyond every INT there is: nothing above, everything below.
    assert!(keys(&mut conn, "SELECT v FROM ko_int WHERE k > 1e30")
        .await
        .is_empty());
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_int WHERE k < 1e30")
            .await
            .len(),
        5
    );

    conn.query_drop("UPDATE ko_int SET v = 40 WHERE k = 3.0")
        .await
        .unwrap();
    conn.query_drop("DELETE FROM ko_int WHERE k = 4.0")
        .await
        .unwrap();
    conn.query_drop("DELETE FROM ko_int WHERE k = 1.5")
        .await
        .unwrap();
    assert_eq!(
        keys(&mut conn, "SELECT v FROM ko_int").await,
        ["1", "2", "3", "40"]
    );
    conn.query_drop("DROP TABLE ko_int").await.unwrap();
    server.shutdown().await;
}

/// A value the key column cannot hold exactly finds no row, and bounds a
/// range where it lies among the keys, not where the column would round it
/// to.
#[tokio::test]
async fn a_value_the_key_cannot_hold_is_not_rounded_to_one_it_can() {
    let server = TestServer::start("key_order_inexact").await;
    let mut conn = server.connect().await;

    // A decimal key without a fraction, and a floating-point value.
    conn.query_drop("CREATE TABLE ko_dec0 (k DECIMAL(10,0) PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ko_dec0 VALUES (2, 2), (3, 3), (4, 4)")
        .await
        .unwrap();
    for (filter, expected) in [
        ("k = 2.5e0", vec![]),
        ("k = 3.0e0", vec!["3"]),
        ("k > 2.5e0", vec!["3", "4"]),
        ("k >= 2.5e0", vec!["3", "4"]),
        ("k < 2.5e0", vec!["2"]),
        ("k <= 3.5e0", vec!["2", "3"]),
        ("k BETWEEN 2.4e0 AND 3.6e0", vec!["3"]),
        ("k = 2.5", vec![]),
        ("k > 2.5", vec!["3", "4"]),
        ("k < 3.5", vec!["2", "3"]),
    ] {
        assert_eq!(
            keys(&mut conn, &format!("SELECT v FROM ko_dec0 WHERE {filter}")).await,
            expected,
            "DECIMAL(10,0): {filter}"
        );
    }

    // A decimal key with fewer digits after the point than the value.
    conn.query_drop("CREATE TABLE ko_dec1 (k DECIMAL(10,1) PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ko_dec1 VALUES (1.2, 12), (1.3, 13), (1.4, 14)")
        .await
        .unwrap();
    for (filter, expected) in [
        ("k = 1.25", vec![]),
        ("k = 1.30", vec!["13"]),
        ("k > 1.25", vec!["13", "14"]),
        ("k < 1.35", vec!["12", "13"]),
        ("k >= 1.25e0", vec!["13", "14"]),
    ] {
        assert_eq!(
            keys(&mut conn, &format!("SELECT v FROM ko_dec1 WHERE {filter}")).await,
            expected,
            "DECIMAL(10,1): {filter}"
        );
    }

    // An integer key, and a number written as a string.
    conn.query_drop("CREATE TABLE ko_int (k INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ko_int VALUES (2, 2), (3, 3), (4, 4)")
        .await
        .unwrap();
    for (filter, expected) in [
        ("k = '2.7'", vec![]),
        ("k = '3'", vec!["3"]),
        ("k = '3.0'", vec!["3"]),
        ("k > '2.7'", vec!["3", "4"]),
        ("k < '3.2'", vec!["2", "3"]),
        ("k >= '3'", vec!["3", "4"]),
    ] {
        assert_eq!(
            keys(&mut conn, &format!("SELECT v FROM ko_int WHERE {filter}")).await,
            expected,
            "INT: {filter}"
        );
    }
    server.shutdown().await;
}

/// A comparison with NULL is never true: a key range with a NULL end has
/// no rows.
#[tokio::test]
async fn a_key_range_bounded_by_null_is_empty() {
    let server = TestServer::start("key_order_null").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE TABLE ko_null (k INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO ko_null VALUES (1, 1), (5, 5), (20, 20)")
        .await
        .unwrap();
    for filter in [
        "k > NULL",
        "k >= NULL",
        "k < NULL",
        "k <= NULL",
        "k = NULL",
        "k BETWEEN NULL AND 10",
        "k BETWEEN 2 AND NULL",
    ] {
        assert_eq!(
            keys(&mut conn, &format!("SELECT v FROM ko_null WHERE {filter}")).await,
            Vec::<String>::new(),
            "{filter}"
        );
    }
    server.shutdown().await;
}
