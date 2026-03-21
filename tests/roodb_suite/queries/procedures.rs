//! Stored procedure integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_create_and_call_simple_procedure() {
    let server = TestServer::start("proc_simple").await;
    let mut conn = server.connect().await;

    // Create a simple procedure that sets a user variable
    conn.query_drop("CREATE PROCEDURE set_var() BEGIN SET @result = 42; END")
        .await
        .expect("CREATE PROCEDURE failed");

    // Call the procedure
    conn.query_drop("CALL set_var()")
        .await
        .expect("CALL failed");

    // Verify the user variable was set
    let rows: Vec<(Option<String>,)> = conn
        .query("SELECT @result")
        .await
        .expect("SELECT @result failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, Some("42".to_string()));

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_drop_procedure() {
    let server = TestServer::start("proc_drop").await;
    let mut conn = server.connect().await;

    // Create procedure
    conn.query_drop("CREATE PROCEDURE my_proc() BEGIN SET @x = 1; END")
        .await
        .expect("CREATE PROCEDURE failed");

    // Drop it
    conn.query_drop("DROP PROCEDURE my_proc")
        .await
        .expect("DROP PROCEDURE failed");

    // Calling should fail now
    let result: Result<Vec<(String,)>, _> = conn.query("CALL my_proc()").await;
    assert!(result.is_err(), "Expected error calling dropped procedure");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_drop_procedure_if_exists() {
    let server = TestServer::start("proc_drop_ie").await;
    let mut conn = server.connect().await;

    // DROP IF EXISTS on non-existent should succeed
    conn.query_drop("DROP PROCEDURE IF EXISTS nonexistent")
        .await
        .expect("DROP PROCEDURE IF EXISTS failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_with_in_param() {
    let server = TestServer::start("proc_param").await;
    let mut conn = server.connect().await;

    // Create procedure with IN parameter
    conn.query_drop("CREATE PROCEDURE set_value(IN val INT) BEGIN SET @out = val; END")
        .await
        .expect("CREATE PROCEDURE failed");

    // Call with argument — local var 'val' should be usable in SET @out = val
    conn.query_drop("CALL set_value(99)")
        .await
        .expect("CALL failed");

    // Verify the user variable was set from the IN parameter
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @out").await.expect("SELECT @out failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, Some("99".to_string()));

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_duplicate_error() {
    let server = TestServer::start("proc_dup").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE PROCEDURE dup_proc() BEGIN SET @x = 1; END")
        .await
        .expect("CREATE PROCEDURE failed");

    // Creating again should fail
    let result: Result<(), _> = conn
        .query_drop("CREATE PROCEDURE dup_proc() BEGIN SET @x = 2; END")
        .await;
    assert!(result.is_err(), "Expected error on duplicate procedure");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_with_dml() {
    let server = TestServer::start("proc_dml").await;
    let mut conn = server.connect().await;

    // Create table
    conn.query_drop("CREATE TABLE proc_test (id INT NOT NULL, val VARCHAR(100), PRIMARY KEY (id))")
        .await
        .expect("CREATE TABLE failed");

    // Create procedure that does DML
    conn.query_drop(
        "CREATE PROCEDURE insert_row() BEGIN INSERT INTO proc_test VALUES (1, 'hello'); END",
    )
    .await
    .expect("CREATE PROCEDURE failed");

    // Call the procedure
    conn.query_drop("CALL insert_row()")
        .await
        .expect("CALL failed");

    // Verify the row was inserted
    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, val FROM proc_test")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "hello");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_if_else() {
    let server = TestServer::start("proc_if").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE PROCEDURE check_sign(IN n INT) BEGIN \
         IF n > 0 THEN SET @sign = 'positive'; \
         ELSEIF n < 0 THEN SET @sign = 'negative'; \
         ELSE SET @sign = 'zero'; \
         END IF; \
         END",
    )
    .await
    .expect("CREATE PROCEDURE failed");

    conn.query_drop("CALL check_sign(5)")
        .await
        .expect("CALL failed");
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @sign").await.unwrap();
    assert_eq!(rows[0].0, Some("positive".to_string()));

    conn.query_drop("CALL check_sign(-3)")
        .await
        .expect("CALL failed");
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @sign").await.unwrap();
    assert_eq!(rows[0].0, Some("negative".to_string()));

    conn.query_drop("CALL check_sign(0)")
        .await
        .expect("CALL failed");
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @sign").await.unwrap();
    assert_eq!(rows[0].0, Some("zero".to_string()));

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_while_loop() {
    let server = TestServer::start("proc_while").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE PROCEDURE sum_to(IN n INT) BEGIN \
         DECLARE i INT DEFAULT 0; \
         DECLARE total INT DEFAULT 0; \
         WHILE i < n DO \
           SET i = i + 1; \
           SET total = total + i; \
         END WHILE; \
         SET @result = total; \
         END",
    )
    .await
    .expect("CREATE PROCEDURE failed");

    conn.query_drop("CALL sum_to(10)")
        .await
        .expect("CALL failed");

    let rows: Vec<(Option<String>,)> = conn.query("SELECT @result").await.unwrap();
    assert_eq!(rows[0].0, Some("55".to_string()));

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_declare_and_local_vars() {
    let server = TestServer::start("proc_declare").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE decl_test (id INT NOT NULL, val VARCHAR(100), PRIMARY KEY (id))")
        .await
        .expect("CREATE TABLE failed");

    conn.query_drop(
        "CREATE PROCEDURE use_locals(IN p_id INT, IN p_val TEXT) BEGIN \
         DECLARE local_id INT DEFAULT 0; \
         SET local_id = p_id; \
         INSERT INTO decl_test VALUES (local_id, p_val); \
         END",
    )
    .await
    .expect("CREATE PROCEDURE failed");

    conn.query_drop("CALL use_locals(42, 'hello')")
        .await
        .expect("CALL failed");

    let rows: Vec<(i32, String)> = conn
        .query("SELECT id, val FROM decl_test")
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 42);
    assert_eq!(rows[0].1, "hello");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_out_param() {
    let server = TestServer::start("proc_out").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE PROCEDURE add_nums(IN a INT, IN b INT, OUT result INT) BEGIN \
         SET result = a + b; \
         END",
    )
    .await
    .expect("CREATE PROCEDURE failed");

    conn.query_drop("SET @answer = 0")
        .await
        .expect("SET failed");
    conn.query_drop("CALL add_nums(3, 7, @answer)")
        .await
        .expect("CALL failed");

    let rows: Vec<(Option<String>,)> = conn.query("SELECT @answer").await.unwrap();
    assert_eq!(rows[0].0, Some("10".to_string()));

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_procedure_case_statement() {
    let server = TestServer::start("proc_case").await;
    let mut conn = server.connect().await;

    conn.query_drop(
        "CREATE PROCEDURE day_type(IN d INT) BEGIN \
         CASE \
           WHEN d = 1 THEN SET @dtype = 'Monday'; \
           WHEN d = 7 THEN SET @dtype = 'Sunday'; \
           ELSE SET @dtype = 'other'; \
         END CASE; \
         END",
    )
    .await
    .expect("CREATE PROCEDURE failed");

    conn.query_drop("CALL day_type(1)")
        .await
        .expect("CALL failed");
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @dtype").await.unwrap();
    assert_eq!(rows[0].0, Some("Monday".to_string()));

    conn.query_drop("CALL day_type(7)")
        .await
        .expect("CALL failed");
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @dtype").await.unwrap();
    assert_eq!(rows[0].0, Some("Sunday".to_string()));

    conn.query_drop("CALL day_type(3)")
        .await
        .expect("CALL failed");
    let rows: Vec<(Option<String>,)> = conn.query("SELECT @dtype").await.unwrap();
    assert_eq!(rows[0].0, Some("other".to_string()));

    drop(conn);
    server.shutdown().await;
}
