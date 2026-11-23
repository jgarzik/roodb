//! Databases: a table belongs to one, and a name means the table of that
//! name in the session's current database unless it says otherwise.

use mysql_async::prelude::*;
use mysql_async::{Conn, OptsBuilder};

use crate::roodb_suite::TestServer;
use crate::test_utils::process::ServerProcess;

fn code(err: &mysql_async::Error) -> u16 {
    match err {
        mysql_async::Error::Server(e) => e.code,
        other => panic!("not a server error: {other}"),
    }
}

/// Two databases each have a table of the same name, with their own rows.
/// A name with a database reaches either from anywhere; `USE` decides what
/// a bare name means.
#[tokio::test]
async fn the_same_name_in_two_databases_is_two_tables() {
    let server = TestServer::start("db_two_tables").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE DATABASE dbs_one").await.unwrap();
    conn.query_drop("CREATE DATABASE dbs_two").await.unwrap();
    conn.query_drop("CREATE TABLE dbs_one.items (id INT PRIMARY KEY, v VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("USE dbs_two").await.unwrap();
    conn.query_drop("CREATE TABLE items (id INT PRIMARY KEY, v VARCHAR(20))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO items VALUES (1, 'two')")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO dbs_one.items VALUES (1, 'one'), (2, 'one-b')")
        .await
        .unwrap();

    let here: Vec<String> = conn.query("SELECT v FROM items ORDER BY id").await.unwrap();
    assert_eq!(here, ["two"]);
    let there: Vec<String> = conn
        .query("SELECT v FROM dbs_one.items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(there, ["one", "one-b"]);

    // Across databases in one statement.
    let joined: Vec<(String, String)> = conn
        .query("SELECT a.v, b.v FROM dbs_one.items a JOIN dbs_two.items b ON a.id = b.id")
        .await
        .unwrap();
    assert_eq!(joined, [("one".to_string(), "two".to_string())]);

    // The other way round after USE.
    conn.query_drop("USE dbs_one").await.unwrap();
    let here: Vec<String> = conn.query("SELECT v FROM items ORDER BY id").await.unwrap();
    assert_eq!(here, ["one", "one-b"]);
    conn.query_drop("UPDATE dbs_two.items SET v = 'changed' WHERE id = 1")
        .await
        .unwrap();
    conn.query_drop("DELETE FROM items WHERE id = 2")
        .await
        .unwrap();
    let there: Vec<String> = conn.query("SELECT v FROM dbs_two.items").await.unwrap();
    assert_eq!(there, ["changed"]);
    let here: Vec<String> = conn.query("SELECT v FROM items").await.unwrap();
    assert_eq!(here, ["one"]);

    // Each database lists its own tables.
    let tables: Vec<String> = conn.query("SHOW TABLES").await.unwrap();
    assert_eq!(tables, ["items"]);
    let tables: Vec<String> = conn.query("SHOW TABLES FROM dbs_two").await.unwrap();
    assert_eq!(tables, ["items"]);
    let ddl: Option<(String, String)> = conn.query_first("SHOW CREATE TABLE items").await.unwrap();
    let (name, ddl) = ddl.unwrap();
    assert_eq!(name, "items");
    assert!(ddl.starts_with("CREATE TABLE `items`"), "{ddl}");

    conn.query_drop("DROP DATABASE dbs_one").await.unwrap();
    conn.query_drop("DROP DATABASE dbs_two").await.unwrap();
    server.shutdown().await;
}

/// Dropping a database drops what is in it: its tables (rows and all) and
/// its views. A database created under the name afterwards is empty.
#[tokio::test]
async fn dropping_a_database_drops_its_tables() {
    let server = TestServer::start("db_drop").await;
    let mut conn = server.connect().await;
    conn.query_drop("CREATE DATABASE dbs_gone").await.unwrap();
    conn.query_drop("CREATE TABLE dbs_gone.a (id INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE dbs_gone.b (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO dbs_gone.a VALUES (1), (2)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO dbs_gone.b (v) VALUES (10), (20)")
        .await
        .unwrap();
    conn.query_drop("CREATE VIEW dbs_gone.a_view AS SELECT id FROM dbs_gone.a")
        .await
        .unwrap();

    conn.query_drop("DROP DATABASE dbs_gone").await.unwrap();
    let err = conn
        .query_drop("SELECT * FROM dbs_gone.a")
        .await
        .expect_err("a table outlived its database");
    assert_eq!(code(&err), 1049, "{err}");
    // Dropping it again says there is none; IF EXISTS does not mind.
    let err = conn
        .query_drop("DROP DATABASE dbs_gone")
        .await
        .expect_err("a database was dropped twice");
    assert_eq!(code(&err), 1008, "{err}");
    conn.query_drop("DROP DATABASE IF EXISTS dbs_gone")
        .await
        .unwrap();

    conn.query_drop("CREATE DATABASE dbs_gone").await.unwrap();
    let err = conn
        .query_drop("CREATE DATABASE dbs_gone")
        .await
        .expect_err("a database was created twice");
    assert_eq!(code(&err), 1007, "{err}");
    let tables: Vec<String> = conn.query("SHOW TABLES FROM dbs_gone").await.unwrap();
    assert!(tables.is_empty(), "{tables:?}");
    for gone in ["a", "a_view"] {
        let err = conn
            .query_drop(format!("SELECT * FROM dbs_gone.{gone}"))
            .await
            .expect_err("a table or view outlived its database");
        assert_eq!(code(&err), 1146, "{gone}: {err}");
    }
    conn.query_drop("CREATE TABLE dbs_gone.b (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO dbs_gone.b (v) VALUES (30)")
        .await
        .unwrap();
    let rows: Vec<(i32, i32)> = conn.query("SELECT id, v FROM dbs_gone.b").await.unwrap();
    assert_eq!(rows, [(1, 30)]);
    conn.query_drop("DROP DATABASE dbs_gone").await.unwrap();
    server.shutdown().await;
}

/// A table cannot be made in a database that does not exist, and a bare
/// name needs a current database.
#[tokio::test]
async fn names_need_a_database_that_exists() {
    let mut server = ServerProcess::init(&[]);
    server.start();

    // A session that selected no database.
    let opts = OptsBuilder::from_opts(server.opts()).db_name(None::<String>);
    let mut conn = Conn::new(opts).await.unwrap();
    let err = conn
        .query_drop("CREATE TABLE nowhere (id INT PRIMARY KEY)")
        .await
        .expect_err("a table was created in no database");
    assert_eq!(code(&err), 1046, "{err}");
    let err = conn
        .query_drop("SELECT * FROM nowhere")
        .await
        .expect_err("a bare name was resolved without a database");
    assert_eq!(code(&err), 1046, "{err}");

    let err = conn
        .query_drop("SHOW TABLES")
        .await
        .expect_err("the tables of no database were listed");
    assert_eq!(code(&err), 1046, "{err}");
    let current: Vec<Option<String>> = conn.query("SELECT DATABASE()").await.unwrap();
    assert_eq!(current, [None]);

    // With the database named, no current one is needed. Names may be
    // quoted, and a dot inside the quotes is part of the name.
    conn.query_drop("CREATE TABLE `test`.`some.where` (id INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test.`some.where` VALUES (8)")
        .await
        .unwrap();
    let rows: Vec<i32> = conn
        .query("SELECT id FROM `test`.`some.where`")
        .await
        .unwrap();
    assert_eq!(rows, [8]);
    conn.query_drop("DROP TABLE test.`some.where`")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE test.somewhere (id INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test.somewhere VALUES (7)")
        .await
        .unwrap();
    let rows: Vec<i32> = conn.query("SELECT id FROM test.somewhere").await.unwrap();
    assert_eq!(rows, [7]);

    let err = conn
        .query_drop("CREATE TABLE no_such_db.t (id INT PRIMARY KEY)")
        .await
        .expect_err("a table was created in a database that does not exist");
    assert_eq!(code(&err), 1049, "{err}");
    // Nor can a session begin in one.
    let opts = OptsBuilder::from_opts(server.opts()).db_name(Some("no_such_db"));
    let err = match Conn::new(opts).await {
        Ok(_) => panic!("logged in to a database that does not exist"),
        Err(err) => err,
    };
    assert_eq!(code(&err), 1049, "{err}");
    let err = conn
        .query_drop("USE no_such_db")
        .await
        .expect_err("a database that does not exist was selected");
    assert_eq!(code(&err), 1049, "{err}");

    // After USE, bare names work, and survive a restart.
    conn.query_drop("USE test").await.unwrap();
    let rows: Vec<i32> = conn.query("SELECT id FROM somewhere").await.unwrap();
    assert_eq!(rows, [7]);
    drop(conn);
    server.kill();
    server.start();
    let mut conn = Conn::new(server.opts()).await.unwrap();
    let rows: Vec<i32> = conn.query("SELECT id FROM somewhere").await.unwrap();
    assert_eq!(rows, [7]);
}

/// The system's own databases cannot be dropped, and a database's name
/// distinguishes case.
#[tokio::test]
async fn system_databases_stay_and_names_distinguish_case() {
    let server = TestServer::start("db_sys").await;
    let mut conn = server.connect().await;
    for database in ["mysql", "information_schema"] {
        assert!(
            conn.query_drop(format!("DROP DATABASE {database}"))
                .await
                .is_err(),
            "{database} was dropped"
        );
    }
    let root: Vec<String> = conn
        .query("SELECT username FROM mysql.users WHERE username = 'root'")
        .await
        .unwrap();
    assert_eq!(root, ["root"]);

    conn.query_drop("CREATE DATABASE dbs_Case").await.unwrap();
    conn.query_drop("CREATE DATABASE dbs_case").await.unwrap();
    conn.query_drop("CREATE TABLE dbs_Case.t (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let err = conn
        .query_drop("SELECT * FROM dbs_case.t")
        .await
        .expect_err("a table was found in a database of another case");
    assert_eq!(code(&err), 1146, "{err}");
    conn.query_drop("DROP DATABASE dbs_Case").await.unwrap();
    conn.query_drop("DROP DATABASE dbs_case").await.unwrap();
    server.shutdown().await;
}

/// Views, routines and triggers belong to a database: the same name in two
/// databases is two of them, each works with the tables of its own
/// database whatever database the session is in, and they go with their
/// database.
#[tokio::test]
async fn views_routines_and_triggers_belong_to_a_database() {
    let server = TestServer::start("db_routines").await;
    let mut conn = server.connect().await;
    for (database, value) in [("dbs_r1", 1), ("dbs_r2", 2)] {
        conn.query_drop(format!("CREATE DATABASE {database}"))
            .await
            .unwrap();
        conn.query_drop(format!("USE {database}")).await.unwrap();
        conn.query_drop("CREATE TABLE src (id INT PRIMARY KEY)")
            .await
            .unwrap();
        conn.query_drop("CREATE TABLE log (id INT PRIMARY KEY)")
            .await
            .unwrap();
        conn.query_drop(format!("INSERT INTO src VALUES ({value})"))
            .await
            .unwrap();
        // Each names its tables without a database.
        conn.query_drop("CREATE VIEW seen AS SELECT id FROM src")
            .await
            .unwrap();
        conn.query_drop("CREATE PROCEDURE note(IN n INT) BEGIN INSERT INTO log VALUES (n); END")
            .await
            .unwrap();
        conn.query_drop(format!(
            "CREATE FUNCTION which() RETURNS INT BEGIN RETURN {value}; END"
        ))
        .await
        .unwrap();
        conn.query_drop(
            "CREATE TRIGGER copy AFTER INSERT ON src FOR EACH ROW \
             INSERT INTO log VALUES (NEW.id + 100)",
        )
        .await
        .unwrap();
    }
    // A second routine of the same name in one database is refused; the
    // connection goes on.
    for again in [
        "CREATE PROCEDURE note(IN n INT) BEGIN INSERT INTO log VALUES (n); END",
        "CREATE FUNCTION which() RETURNS INT BEGIN RETURN 9; END",
    ] {
        let err = conn
            .query_drop(again)
            .await
            .expect_err("a routine was created twice");
        assert_eq!(code(&err), 1304, "{again}: {err}");
    }

    // From a third database, each is reached through its own.
    conn.query_drop("USE test").await.unwrap();
    for (database, value) in [("dbs_r1", 1), ("dbs_r2", 2)] {
        let seen: Vec<i32> = conn
            .query(format!("SELECT id FROM {database}.seen"))
            .await
            .unwrap();
        assert_eq!(seen, [value], "{database}.seen");
        let which: Vec<i32> = conn
            .query(format!("SELECT {database}.which()"))
            .await
            .unwrap();
        assert_eq!(which, [value], "{database}.which()");
        conn.query_drop(format!("CALL {database}.note(7)"))
            .await
            .unwrap();
        conn.query_drop(format!("INSERT INTO {database}.src VALUES (50)"))
            .await
            .unwrap();
        let log: Vec<i32> = conn
            .query(format!("SELECT id FROM {database}.log ORDER BY id"))
            .await
            .unwrap();
        assert_eq!(log, [7, 150], "{database}.log");
    }
    // The session's database is where it was.
    let current: Vec<String> = conn.query("SELECT DATABASE()").await.unwrap();
    assert_eq!(current, ["test"]);
    // A routine not named with its database is looked for in the
    // session's.
    let err = conn
        .query_drop("CALL note(1)")
        .await
        .expect_err("a routine of another database was found");
    assert_eq!(code(&err), 1305, "{err}");

    // One database goes; the other's are untouched, and a new database of
    // the name has none of them.
    conn.query_drop("DROP DATABASE dbs_r1").await.unwrap();
    conn.query_drop("CREATE DATABASE dbs_r1").await.unwrap();
    let err = conn
        .query_drop("CALL dbs_r1.note(1)")
        .await
        .expect_err("a routine outlived its database");
    assert_eq!(code(&err), 1305, "{err}");
    conn.query_drop("CREATE TABLE dbs_r1.src (id INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO dbs_r1.src VALUES (1)")
        .await
        .expect("a trigger outlived its database");
    conn.query_drop("CALL dbs_r2.note(8)").await.unwrap();
    let log: Vec<i32> = conn
        .query("SELECT id FROM dbs_r2.log ORDER BY id")
        .await
        .unwrap();
    assert_eq!(log, [7, 8, 150]);

    conn.query_drop("DROP DATABASE dbs_r1").await.unwrap();
    conn.query_drop("DROP DATABASE dbs_r2").await.unwrap();
    server.shutdown().await;
}
