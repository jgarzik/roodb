//! Parameterised statements over TDS RPC requests.

use tiberius::numeric::Numeric;

use super::raw::*;
use super::{connect, TdsClient, TRANSPORTS};
use crate::roodb_suite::harness::TestServer;

const SP_EXECUTESQL: u16 = 10;
const SP_PREPARE: u16 = 11;
const SP_EXECUTE: u16 = 12;
const SP_PREPEXEC: u16 = 13;
const SP_UNPREPARE: u16 = 15;

async fn run(client: &mut TdsClient, sql: String) {
    client
        .simple_query(sql.clone())
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
        .into_results()
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

/// tiberius `query` and `execute` send sp_executesql with typed
/// parameters.
#[tokio::test]
async fn test_parameterised_queries() {
    let server = TestServer::start("tds_rpc").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_rpc_{name}");
        run(&mut client, format!("DROP TABLE IF EXISTS {table}")).await;
        run(
            &mut client,
            format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(100), score DOUBLE, \
                 amount DECIMAL(10,2), data BLOB, flag BOOLEAN, big BIGINT)"
            ),
        )
        .await;

        let insert = format!("INSERT INTO {table} VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7)");
        let affected = client
            .execute(
                insert.as_str(),
                &[
                    &1i32,
                    &"alice",
                    &1.5f64,
                    &Numeric::new_with_scale(-1_234_567, 2),
                    &&[0u8, 255, 16][..],
                    &true,
                    &i64::MIN,
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: parameterised insert failed: {e}"));
        assert_eq!(affected.total(), 1, "{name}");

        // NULLs of each kind, and non-ASCII text
        client
            .execute(
                insert.as_str(),
                &[
                    &2i32,
                    &"b\u{f6}b \u{65e5}\u{672c}",
                    &Option::<f64>::None,
                    &Option::<Numeric>::None,
                    &Option::<&[u8]>::None,
                    &Option::<bool>::None,
                    &Option::<i64>::None,
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: insert with NULLs failed: {e}"));

        // The same statement again exercises the statement cache.
        for id in 3..6i32 {
            client
                .execute(
                    insert.as_str(),
                    &[
                        &id,
                        &format!("row{id}"),
                        &(id as f64),
                        &Numeric::new_with_scale(id as i128 * 100, 2),
                        &&[id as u8][..],
                        &false,
                        &(id as i64),
                    ],
                )
                .await
                .unwrap();
        }

        let rows = client
            .query(
                format!("SELECT * FROM {table} WHERE id = @P1").as_str(),
                &[&1i32],
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: parameterised select failed: {e}"))
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "{name}");
        let r = &rows[0];
        assert_eq!(r.get::<&str, _>("name"), Some("alice"));
        assert_eq!(r.get::<f64, _>("score"), Some(1.5));
        assert_eq!(
            r.get::<Numeric, _>("amount"),
            Some(Numeric::new_with_scale(-1_234_567, 2))
        );
        assert_eq!(r.get::<&[u8], _>("data"), Some(&[0u8, 255, 16][..]));
        assert_eq!(r.get::<bool, _>("flag"), Some(true));
        assert_eq!(r.get::<i64, _>("big"), Some(i64::MIN));

        let rows = client
            .query(
                format!("SELECT name, score, data FROM {table} WHERE id = @P1").as_str(),
                &[&2i32],
            )
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows[0].get::<&str, _>(0), Some("b\u{f6}b \u{65e5}\u{672c}"));
        assert_eq!(rows[0].get::<f64, _>(1), None);
        assert_eq!(rows[0].get::<&[u8], _>(2), None);

        // A parameter used twice, and two parameters out of order
        let rows = client
            .query(
                format!(
                    "SELECT id FROM {table} WHERE id >= @P2 AND id <= @P1 AND id <> @P2 ORDER BY id"
                )
                .as_str(),
                &[&5i32, &3i32],
            )
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        let ids: Vec<i32> = rows.iter().map(|r| r.get::<i32, _>(0).unwrap()).collect();
        assert_eq!(ids, vec![4, 5], "{name}");

        // No parameters at all still goes through sp_executesql
        let rows = client
            .query(format!("SELECT COUNT(*) FROM {table}").as_str(), &[])
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);

        // An error in a parameterised statement leaves the session usable
        let duplicate = client
            .execute(
                insert.as_str(),
                &[
                    &1i32,
                    &"again",
                    &0f64,
                    &Numeric::new_with_scale(0, 2),
                    &&[][..],
                    &false,
                    &0i64,
                ],
            )
            .await;
        match duplicate {
            Err(tiberius::error::Error::Server(e)) => assert_eq!(e.code(), 2627, "{name}: {e:?}"),
            other => panic!("{name}: expected a duplicate key error, got {other:?}"),
        }
        let rows = client
            .query(
                format!("SELECT name FROM {table} WHERE id = @P1").as_str(),
                &[&1i32],
            )
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows[0].get::<&str, _>(0), Some("alice"), "{name}");

        run(&mut client, format!("DROP TABLE {table}")).await;
    }
    server.shutdown().await;
}

/// Parameter values are data. Text that would change the statement if it
/// were pasted into it must be stored and compared verbatim.
#[tokio::test]
async fn test_parameter_values_cannot_inject_sql() {
    let server = TestServer::start("tds_rpc_inject").await;
    let (encryption, name) = TRANSPORTS[1];
    let mut client = connect(&server, encryption).await;
    let table = format!("tds_inj_{name}");
    run(&mut client, format!("DROP TABLE IF EXISTS {table}")).await;
    run(
        &mut client,
        format!("CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(200))"),
    )
    .await;
    run(
        &mut client,
        format!("INSERT INTO {table} VALUES (1, 'alice'), (2, 'bob')"),
    )
    .await;

    let payloads = [
        r"\' OR 1=1 -- ",
        "' OR '1'='1",
        "'; DROP TABLE tds_inj_tds8; -- ",
        r"C:\dir\",
        "a\\",
        "\"; DELETE FROM tds_inj_tds8 WHERE \"1\"=\"1",
        "/* comment */ @P1 ?",
        "back`tick",
        "nul\0byte",
    ];

    for (i, payload) in payloads.iter().enumerate() {
        // As a filter: matches nothing, rather than everything.
        let rows = client
            .query(
                format!("SELECT id FROM {table} WHERE name = @P1").as_str(),
                &[payload],
            )
            .await
            .unwrap_or_else(|e| panic!("filter with {payload:?} failed: {e}"))
            .into_first_result()
            .await
            .unwrap();
        assert!(rows.is_empty(), "{payload:?} matched rows: {}", rows.len());

        // As a stored value: comes back byte for byte.
        let id = 100 + i as i32;
        client
            .execute(
                format!("INSERT INTO {table} VALUES (@P1, @P2)").as_str(),
                &[&id, payload],
            )
            .await
            .unwrap_or_else(|e| panic!("insert of {payload:?} failed: {e}"));
        let rows = client
            .query(
                format!("SELECT name FROM {table} WHERE id = @P1").as_str(),
                &[&id],
            )
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert_eq!(rows[0].get::<&str, _>(0), Some(*payload));
    }

    // The table and its original rows are intact.
    let rows = client
        .simple_query(format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let rows = client
        .query(
            format!("SELECT name FROM {table} WHERE id IN (@P1, @P2) ORDER BY id").as_str(),
            &[&1i32, &2i32],
        )
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.get::<&str, _>(0).unwrap()).collect();
    assert_eq!(names, vec!["alice", "bob"]);

    run(&mut client, format!("DROP TABLE {table}")).await;
    server.shutdown().await;
}

fn handle_of(tokens: &[Token]) -> i32 {
    tokens
        .iter()
        .find_map(|t| match t {
            Token::ReturnValue { value, .. } => Some(*value),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no handle returned: {tokens:?}"))
}

/// sp_prepare / sp_execute / sp_unprepare and sp_prepexec, as ODBC and
/// JDBC drivers use them.
#[tokio::test]
async fn test_prepare_execute_unprepare() {
    let server = TestServer::start("tds_rpc_prepare").await;
    let mut client = RawTds::login_root(&server).await;
    for sql in [
        "DROP TABLE IF EXISTS tds_prep_t",
        "CREATE TABLE tds_prep_t (id INT PRIMARY KEY, name VARCHAR(50))",
    ] {
        let tokens = client.batch(sql).await;
        assert!(errors(&tokens).is_empty(), "{sql}: {tokens:?}");
    }

    // sp_prepare @handle OUTPUT, @params, @stmt
    let tokens = client
        .rpc(
            SP_PREPARE,
            &[
                rpc_param("@handle", true, &int_value(None)),
                rpc_param(
                    "@params",
                    false,
                    &nvarchar_value("@id int, @name nvarchar(50)"),
                ),
                rpc_param(
                    "@stmt",
                    false,
                    &nvarchar_value("INSERT INTO tds_prep_t VALUES (@id, @name)"),
                ),
            ],
        )
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert!(tokens.contains(&Token::ReturnStatus(0)), "{tokens:?}");
    let handle = handle_of(&tokens);
    assert!(handle > 0);

    // sp_execute @handle, values: by position, then by name in any order
    let tokens = client
        .rpc(
            SP_EXECUTE,
            &[
                rpc_param("", false, &int_value(Some(handle))),
                rpc_param("", false, &int_value(Some(1))),
                rpc_param("", false, &nvarchar_value("one")),
            ],
        )
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    let tokens = client
        .rpc(
            SP_EXECUTE,
            &[
                rpc_param("", false, &int_value(Some(handle))),
                rpc_param("@name", false, &nvarchar_value("two")),
                rpc_param("@ID", false, &int_value(Some(2))),
            ],
        )
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");

    // Wrong arguments are refused and the session continues
    for params in [
        // missing value
        vec![
            rpc_param("", false, &int_value(Some(handle))),
            rpc_param("", false, &int_value(Some(3))),
        ],
        // unknown parameter name
        vec![
            rpc_param("", false, &int_value(Some(handle))),
            rpc_param("@nope", false, &int_value(Some(3))),
            rpc_param("@name", false, &nvarchar_value("x")),
        ],
        // too many values
        vec![
            rpc_param("", false, &int_value(Some(handle))),
            rpc_param("", false, &int_value(Some(3))),
            rpc_param("", false, &nvarchar_value("x")),
            rpc_param("", false, &int_value(Some(4))),
        ],
        // unknown handle
        vec![rpc_param("", false, &int_value(Some(987_654)))],
    ] {
        let tokens = client.rpc(SP_EXECUTE, &params).await;
        assert_eq!(errors(&tokens).len(), 1, "{tokens:?}");
        assert_eq!(final_done(&tokens) & DONE_ERROR, DONE_ERROR, "{tokens:?}");
    }

    // sp_unprepare, after which the handle is gone
    let tokens = client
        .rpc(
            SP_UNPREPARE,
            &[rpc_param("", false, &int_value(Some(handle)))],
        )
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    let tokens = client
        .rpc(
            SP_EXECUTE,
            &[
                rpc_param("", false, &int_value(Some(handle))),
                rpc_param("", false, &int_value(Some(9))),
                rpc_param("", false, &nvarchar_value("gone")),
            ],
        )
        .await;
    assert_eq!(errors(&tokens).len(), 1, "{tokens:?}");

    // sp_prepexec prepares and runs in one call and returns a handle
    let tokens = client
        .rpc(
            SP_PREPEXEC,
            &[
                rpc_param("@handle", true, &int_value(None)),
                rpc_param("", false, &nvarchar_value("@id int, @name nvarchar(50)")),
                rpc_param(
                    "",
                    false,
                    &nvarchar_value("INSERT INTO tds_prep_t VALUES (@id, @name)"),
                ),
                rpc_param("", false, &int_value(Some(3))),
                rpc_param("", false, &nvarchar_value("three")),
            ],
        )
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    let second = handle_of(&tokens);
    assert_ne!(second, handle);

    // sp_executesql through the raw client, parameters by name
    let tokens = client
        .rpc(
            SP_EXECUTESQL,
            &[
                rpc_param(
                    "",
                    false,
                    &nvarchar_value("DELETE FROM tds_prep_t WHERE id = @id"),
                ),
                rpc_param("", false, &nvarchar_value("@id int")),
                rpc_param("@id", false, &int_value(Some(1))),
            ],
        )
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");

    // Rows 2 and 3 exist, row 1 was deleted: inserting them again tells.
    for (id, expect_duplicate) in [(1, false), (2, true), (3, true)] {
        let tokens = client
            .batch(&format!("INSERT INTO tds_prep_t VALUES ({id}, 'x')"))
            .await;
        assert_eq!(
            !errors(&tokens).is_empty(),
            expect_duplicate,
            "row {id}: {tokens:?}"
        );
    }

    // A procedure that does not exist
    let mut payload = all_headers();
    let name = "no_such_proc";
    payload.extend_from_slice(&(name.len() as u16).to_le_bytes());
    payload.extend_from_slice(&ucs2(name));
    payload.extend_from_slice(&0u16.to_le_bytes());
    client.send(PKT_RPC, STATUS_EOM, &payload).await;
    let tokens = client.response().await;
    assert_eq!(errors(&tokens), vec![2812], "{tokens:?}");

    // A malformed RPC body is refused, not fatal
    let mut payload = all_headers();
    payload.extend_from_slice(&[0xFF, 0xFF, 0x0A, 0x00]);
    client.send(PKT_RPC, STATUS_EOM, &payload).await;
    let tokens = client.response().await;
    assert_eq!(errors(&tokens).len(), 1, "{tokens:?}");

    let tokens = client.batch("DROP TABLE tds_prep_t").await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    server.shutdown().await;
}

/// Parameterised text that the driver shims also touch: several
/// statements, a leading session option, probes that depend on session
/// state. Parameter values must still be bound and probes must be
/// current.
#[tokio::test]
async fn test_parameterised_batches_and_probes() {
    let server = TestServer::start("tds_rpc_batch").await;
    for (encryption, name) in TRANSPORTS {
        let mut client = connect(&server, encryption).await;
        let table = format!("tds_rpcb_{name}");
        run(&mut client, format!("DROP TABLE IF EXISTS {table}")).await;
        run(
            &mut client,
            format!("CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(50))"),
        )
        .await;

        // The shape ORMs send: a session option, then the statement
        client
            .execute(
                format!("SET NOCOUNT ON; INSERT INTO {table} VALUES (@P1, @P2)").as_str(),
                &[&1i32, &"one"],
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: option + insert failed: {e}"));

        // Several statements sharing the parameters, in a transaction
        client
            .execute(
                format!(
                    "BEGIN TRAN; INSERT INTO {table} VALUES (@P1, @P2); \
                     UPDATE {table} SET v = @P2 WHERE id = 1; COMMIT TRAN"
                )
                .as_str(),
                &[&2i32, &"two"],
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: parameterised batch failed: {e}"));

        let rows = client
            .simple_query(format!("SELECT id, v FROM {table} ORDER BY id"))
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        let got: Vec<(i32, String)> = rows
            .iter()
            .map(|r| {
                (
                    r.get::<i32, _>(0).unwrap(),
                    r.get::<&str, _>(1).unwrap_or("<NULL>").to_string(),
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![(1, "two".to_string()), (2, "two".to_string())],
            "{name}: values were bound in every statement"
        );

        // A probe in a parameterised statement reflects the session now,
        // not when the statement was first seen.
        let probe = "SELECT @@TRANCOUNT, @P1";
        let trancount = |rows: &[tiberius::Row]| format!("{:?}", rows[0].cells().next().unwrap().1);
        let rows = client
            .query(probe, &[&1i32])
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert!(
            trancount(&rows).contains('0'),
            "{name}: {}",
            trancount(&rows)
        );
        run(&mut client, "BEGIN TRAN".to_string()).await;
        let rows = client
            .query(probe, &[&1i32])
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        assert!(
            trancount(&rows).contains('1'),
            "{name}: {}",
            trancount(&rows)
        );
        run(&mut client, "ROLLBACK".to_string()).await;

        run(&mut client, format!("DROP TABLE {table}")).await;
    }
    server.shutdown().await;
}

/// A rollback issued through an RPC is reported as a rollback.
#[tokio::test]
async fn test_rollback_through_rpc_is_reported_as_rollback() {
    let server = TestServer::start("tds_rpc_rollback").await;
    let mut client = RawTds::login_root(&server).await;

    let tokens = client.batch("BEGIN TRAN").await;
    assert_eq!(env_changes(&tokens), vec![ENV_BEGIN_TRAN], "{tokens:?}");
    let tokens = client
        .rpc(
            SP_EXECUTESQL,
            &[rpc_param(
                "",
                false,
                &nvarchar_value("IF @@TRANCOUNT > 0 ROLLBACK TRAN"),
            )],
        )
        .await;
    assert!(errors(&tokens).is_empty(), "{tokens:?}");
    assert_eq!(env_changes(&tokens), vec![ENV_ROLLBACK_TRAN], "{tokens:?}");

    let tokens = client.batch("BEGIN TRAN").await;
    assert_eq!(env_changes(&tokens), vec![ENV_BEGIN_TRAN]);
    let tokens = client
        .rpc(
            SP_EXECUTESQL,
            &[rpc_param("", false, &nvarchar_value("COMMIT TRAN"))],
        )
        .await;
    assert_eq!(env_changes(&tokens), vec![ENV_COMMIT_TRAN], "{tokens:?}");
    server.shutdown().await;
}
