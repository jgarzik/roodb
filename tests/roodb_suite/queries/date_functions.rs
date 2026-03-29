//! Integration tests for DATE_ADD, DATE_SUB, ADDDATE, SUBDATE functions.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_date_add_days() {
    let server = TestServer::start("date_add_days").await;
    let mut conn = server.connect().await;

    let rows: Vec<(String,)> = conn
        .query("SELECT DATE_ADD('2020-01-01', INTERVAL 1 DAY)")
        .await
        .expect("DATE_ADD DAY failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2020-01-02");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_date_add_months() {
    let server = TestServer::start("date_add_months").await;
    let mut conn = server.connect().await;

    // Adding 1 month to Jan 31 should clamp to Feb 29 (leap year)
    let rows: Vec<(String,)> = conn
        .query("SELECT DATE_ADD('2020-01-31', INTERVAL 1 MONTH)")
        .await
        .expect("DATE_ADD MONTH failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2020-02-29");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_date_sub_days() {
    let server = TestServer::start("date_sub_days").await;
    let mut conn = server.connect().await;

    let rows: Vec<(String,)> = conn
        .query("SELECT DATE_SUB('2020-01-01', INTERVAL 1 DAY)")
        .await
        .expect("DATE_SUB DAY failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2019-12-31");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_adddate_alias() {
    let server = TestServer::start("adddate_alias").await;
    let mut conn = server.connect().await;

    let rows: Vec<(String,)> = conn
        .query("SELECT ADDDATE('2020-06-15', INTERVAL 10 DAY)")
        .await
        .expect("ADDDATE failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2020-06-25");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_subdate_alias() {
    let server = TestServer::start("subdate_alias").await;
    let mut conn = server.connect().await;

    let rows: Vec<(String,)> = conn
        .query("SELECT SUBDATE('2020-06-15', INTERVAL 10 DAY)")
        .await
        .expect("SUBDATE failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2020-06-05");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_date_add_year() {
    let server = TestServer::start("date_add_year").await;
    let mut conn = server.connect().await;

    // Leap year edge case: Feb 29 + 1 year → Feb 28
    let rows: Vec<(String,)> = conn
        .query("SELECT DATE_ADD('2020-02-29', INTERVAL 1 YEAR)")
        .await
        .expect("DATE_ADD YEAR failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2021-02-28");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_date_add_hour() {
    let server = TestServer::start("date_add_hour").await;
    let mut conn = server.connect().await;

    let rows: Vec<(String,)> = conn
        .query("SELECT DATE_ADD('2020-01-01 10:00:00', INTERVAL 3 HOUR)")
        .await
        .expect("DATE_ADD HOUR failed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "2020-01-01 13:00:00");

    drop(conn);
    server.shutdown().await;
}
