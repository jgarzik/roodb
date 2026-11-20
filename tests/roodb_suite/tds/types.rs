//! Every RooDB column type over TDS, with values, NULLs and large values.

use tiberius::numeric::Numeric;
use tiberius::ColumnData;

use super::{connect, TdsClient, TRANSPORTS};
use crate::roodb_suite::harness::TestServer;

async fn run(client: &mut TdsClient, sql: String) {
    client
        .simple_query(sql.clone())
        .await
        .unwrap_or_else(|e| panic!("`{}` failed: {e}", &sql[..sql.len().min(120)]))
        .into_results()
        .await
        .unwrap_or_else(|e| panic!("`{}` failed: {e}", &sql[..sql.len().min(120)]));
}

#[tokio::test]
async fn test_type_matrix() {
    let server = TestServer::start("tds_types").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_types_{name}");

        run(&mut client, format!("DROP TABLE IF EXISTS {table}")).await;
        run(
            &mut client,
            format!(
                "CREATE TABLE {table} (
                    id INT PRIMARY KEY,
                    c_bool BOOLEAN,
                    c_tiny TINYINT,
                    c_small SMALLINT,
                    c_int INT,
                    c_big BIGINT,
                    c_ubig BIGINT UNSIGNED,
                    c_float FLOAT,
                    c_double DOUBLE,
                    c_varchar VARCHAR(100),
                    c_text TEXT,
                    c_blob BLOB,
                    c_ts TIMESTAMP,
                    c_dec DECIMAL(10,2),
                    c_json JSON
                )"
            ),
        )
        .await;
        run(
            &mut client,
            format!(
                "INSERT INTO {table} VALUES (
                    1, TRUE, -128, -32768, -2147483648, -9223372036854775808,
                    18446744073709551615, 1.5, -2.25, 'h\u{e9}llo', 'text value',
                    x'00FF10', '2024-02-29 13:14:15', -12345.67, '{{\"a\": 1}}'
                )"
            ),
        )
        .await;
        run(
            &mut client,
            format!(
                "INSERT INTO {table} VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
            ),
        )
        .await;

        let rows = client
            .simple_query(format!("SELECT * FROM {table} ORDER BY id"))
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap_or_else(|e| panic!("{name}: reading the type matrix failed: {e}"));
        assert_eq!(rows.len(), 2, "{name}");

        let r = &rows[0];
        assert_eq!(r.get::<bool, _>("c_bool"), Some(true), "{name}");
        assert_eq!(r.get::<i16, _>("c_tiny"), Some(-128), "{name}");
        assert_eq!(r.get::<i16, _>("c_small"), Some(-32768), "{name}");
        assert_eq!(r.get::<i32, _>("c_int"), Some(i32::MIN), "{name}");
        assert_eq!(r.get::<i64, _>("c_big"), Some(i64::MIN), "{name}");
        assert_eq!(
            r.get::<Numeric, _>("c_ubig"),
            Some(Numeric::new_with_scale(u64::MAX as i128, 0)),
            "{name}"
        );
        assert_eq!(r.get::<f32, _>("c_float"), Some(1.5), "{name}");
        assert_eq!(r.get::<f64, _>("c_double"), Some(-2.25), "{name}");
        assert_eq!(r.get::<&str, _>("c_varchar"), Some("h\u{e9}llo"), "{name}");
        assert_eq!(r.get::<&str, _>("c_text"), Some("text value"), "{name}");
        assert_eq!(
            r.get::<&[u8], _>("c_blob"),
            Some(&[0x00u8, 0xFF, 0x10][..]),
            "{name}"
        );
        assert_eq!(
            r.get::<Numeric, _>("c_dec"),
            Some(Numeric::new_with_scale(-1_234_567, 2)),
            "{name}"
        );
        let json = r.get::<&str, _>("c_json").expect("json text");
        assert_eq!(
            json.replace(' ', ""),
            "{\"a\":1}",
            "{name}: json comes back as text"
        );

        // 2024-02-29 13:14:15 as DATETIME2(3): day number and milliseconds
        let ts = rows[0]
            .cells()
            .find(|(column, _)| column.name() == "c_ts")
            .map(|(_, data)| data.clone())
            .unwrap();
        match ts {
            ColumnData::DateTime2(Some(dt)) => {
                // Days from 0001-01-01 to 2024-02-29
                assert_eq!(dt.date().days(), 738_944, "{name}");
                assert_eq!(dt.time().scale(), 3, "{name}");
                assert_eq!(
                    dt.time().increments(),
                    (13 * 3600 + 14 * 60 + 15) * 1000,
                    "{name}"
                );
            }
            other => panic!("{name}: expected DATETIME2, got {other:?}"),
        }

        // Every column of the second row is NULL (except the key)
        let nulls = &rows[1];
        assert_eq!(nulls.get::<i32, _>("id"), Some(2));
        for (column, data) in nulls.cells().skip(1) {
            let is_null = match data {
                ColumnData::Bit(v) => v.is_none(),
                ColumnData::I16(v) => v.is_none(),
                ColumnData::I32(v) => v.is_none(),
                ColumnData::I64(v) => v.is_none(),
                ColumnData::F32(v) => v.is_none(),
                ColumnData::F64(v) => v.is_none(),
                ColumnData::Numeric(v) => v.is_none(),
                ColumnData::String(v) => v.is_none(),
                ColumnData::Binary(v) => v.is_none(),
                ColumnData::DateTime2(v) => v.is_none(),
                other => panic!("{name}: unexpected type for {}: {other:?}", column.name()),
            };
            assert!(is_null, "{name}: {} should be NULL", column.name());
        }

        run(&mut client, format!("DROP TABLE {table}")).await;
    }
    server.shutdown().await;
}

/// Values larger than a packet, and larger than the 8000-byte limit of
/// the non-max types, arrive intact.
#[tokio::test]
async fn test_large_values() {
    let server = TestServer::start("tds_large_values").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_lob_{name}");
        run(&mut client, format!("DROP TABLE IF EXISTS {table}")).await;
        run(
            &mut client,
            format!("CREATE TABLE {table} (id INT PRIMARY KEY, t TEXT, b BLOB)"),
        )
        .await;

        let text: String = (0..100_000)
            .map(|i| char::from(b'a' + (i % 26) as u8))
            .collect();
        let blob_hex: String = (0..70_000u32).map(|i| format!("{:02X}", i % 251)).collect();
        run(
            &mut client,
            format!("INSERT INTO {table} VALUES (1, '{text}', x'{blob_hex}')"),
        )
        .await;
        run(
            &mut client,
            format!("INSERT INTO {table} VALUES (2, '', x'')"),
        )
        .await;

        let rows = client
            .simple_query(format!("SELECT t, b FROM {table} ORDER BY id"))
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap_or_else(|e| panic!("{name}: reading large values failed: {e}"));
        assert_eq!(rows[0].get::<&str, _>(0), Some(text.as_str()), "{name}");
        let blob = rows[0].get::<&[u8], _>(1).expect("blob");
        assert_eq!(blob.len(), 70_000, "{name}");
        assert!(
            blob.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8),
            "{name}: blob content"
        );
        // Empty is not NULL
        assert_eq!(rows[1].get::<&str, _>(0), Some(""), "{name}");
        assert_eq!(rows[1].get::<&[u8], _>(1), Some(&[][..]), "{name}");

        run(&mut client, format!("DROP TABLE {table}")).await;
    }
    server.shutdown().await;
}

/// Expressions, aggregates and functions: whatever type the planner
/// declares, the value must arrive as a well-formed column.
#[tokio::test]
async fn test_expression_results() {
    let server = TestServer::start("tds_expr").await;
    let (encryption, name) = TRANSPORTS[0];
    let mut client = connect(&server, encryption).await;
    let table = format!("tds_expr_{name}");
    run(&mut client, format!("DROP TABLE IF EXISTS {table}")).await;
    run(
        &mut client,
        format!("CREATE TABLE {table} (id INT PRIMARY KEY, v INT, d DECIMAL(8,2), s VARCHAR(20))"),
    )
    .await;
    run(
        &mut client,
        format!("INSERT INTO {table} VALUES (1, 10, 1.25, 'a'), (2, 20, 2.50, 'b'), (3, NULL, NULL, NULL)"),
    )
    .await;

    for sql in [
        format!("SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM {table}"),
        format!("SELECT SUM(d), AVG(d), MIN(s), MAX(s) FROM {table}"),
        format!("SELECT v + 1, v * 2.5, v / 3, v DIV 3, -v, v > 5, v IS NULL FROM {table}"),
        format!("SELECT CONCAT(s, '!'), UPPER(s), LENGTH(s), COALESCE(s, 'none') FROM {table}"),
        format!("SELECT CAST(v AS CHAR), CAST(s AS BINARY), CAST(v AS DECIMAL(10,3)) FROM {table}"),
        "SELECT 1, 1.5, 'x', NULL, TRUE, 1e10, 18446744073709551615".to_string(),
        "SELECT NOW(), CURRENT_DATE, 1 = 1, 1 IN (1, 2), 'a' LIKE 'a%'".to_string(),
        format!("SELECT id, CASE WHEN v > 10 THEN 'big' ELSE 'small' END FROM {table}"),
        format!("SELECT s, COUNT(*) FROM {table} GROUP BY s"),
    ] {
        let rows = client
            .simple_query(sql.clone())
            .await
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
            .into_first_result()
            .await
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
        assert!(!rows.is_empty(), "`{sql}` returned no rows");
    }

    // Spot-check values whose declared type and runtime type could differ
    let rows = client
        .simple_query(format!("SELECT COUNT(*), SUM(v) FROM {table}"))
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let as_i64 = |data: &ColumnData<'_>| -> Option<i64> {
        match data {
            ColumnData::I16(v) => v.map(i64::from),
            ColumnData::I32(v) => v.map(i64::from),
            ColumnData::I64(v) => *v,
            ColumnData::Numeric(v) => v.map(|n| (n.value() / 10i128.pow(n.scale() as u32)) as i64),
            ColumnData::F64(v) => v.map(|f| f as i64),
            other => panic!("unexpected numeric column type: {other:?}"),
        }
    };
    let cells: Vec<_> = rows[0].cells().map(|(_, d)| d.clone()).collect();
    assert_eq!(as_i64(&cells[0]), Some(3));
    assert_eq!(as_i64(&cells[1]), Some(30));

    run(&mut client, format!("DROP TABLE {table}")).await;
    server.shutdown().await;
}
