//! JOIN integration tests.

use mysql_async::prelude::*;

use crate::roodb_suite::TestServer;

#[tokio::test]
async fn test_inner_join() {
    let server = TestServer::start("inner_join").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE inner_join_users (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE users failed");

    conn.query_drop("CREATE TABLE inner_join_orders (id INT, user_id INT, amount INT)")
        .await
        .expect("CREATE TABLE orders failed");

    conn.query_drop(
        "INSERT INTO inner_join_users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await
    .expect("INSERT users failed");

    conn.query_drop(
        "INSERT INTO inner_join_orders (id, user_id, amount) VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)",
    )
    .await
    .expect("INSERT orders failed");

    // INNER JOIN - only matching rows
    let rows: Vec<(String, i32)> = conn
        .query(
            "SELECT u.name, o.amount FROM inner_join_users u INNER JOIN inner_join_orders o ON u.id = o.user_id ORDER BY o.amount",
        )
        .await
        .expect("INNER JOIN failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ("Alice".to_string(), 100));
    assert_eq!(rows[1], ("Bob".to_string(), 150));
    assert_eq!(rows[2], ("Alice".to_string(), 200));

    conn.query_drop("DROP TABLE inner_join_orders")
        .await
        .expect("DROP TABLE orders failed");
    conn.query_drop("DROP TABLE inner_join_users")
        .await
        .expect("DROP TABLE users failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_left_join() {
    let server = TestServer::start("left_join").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE left_join_users (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE users failed");

    conn.query_drop("CREATE TABLE left_join_orders (id INT, user_id INT, amount INT)")
        .await
        .expect("CREATE TABLE orders failed");

    conn.query_drop(
        "INSERT INTO left_join_users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await
    .expect("INSERT users failed");

    conn.query_drop(
        "INSERT INTO left_join_orders (id, user_id, amount) VALUES (1, 1, 100), (2, 1, 200)",
    )
    .await
    .expect("INSERT orders failed");

    // LEFT JOIN - all left rows, matching right rows or NULL
    let rows: Vec<(i32, String, Option<i32>)> = conn
        .query(
            "SELECT u.id, u.name, o.amount FROM left_join_users u LEFT JOIN left_join_orders o ON u.id = o.user_id ORDER BY u.id, o.amount",
        )
        .await
        .expect("LEFT JOIN failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], (1, "Alice".to_string(), Some(100)));
    assert_eq!(rows[1], (1, "Alice".to_string(), Some(200)));
    assert_eq!(rows[2], (2, "Bob".to_string(), None));
    assert_eq!(rows[3], (3, "Carol".to_string(), None));

    conn.query_drop("DROP TABLE left_join_orders")
        .await
        .expect("DROP TABLE orders failed");
    conn.query_drop("DROP TABLE left_join_users")
        .await
        .expect("DROP TABLE users failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_right_join() {
    let server = TestServer::start("right_join").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE right_join_orders (id INT, user_id INT, amount INT)")
        .await
        .expect("CREATE TABLE orders failed");

    conn.query_drop("CREATE TABLE right_join_users (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE users failed");

    conn.query_drop(
        "INSERT INTO right_join_orders (id, user_id, amount) VALUES (1, 1, 100), (2, 4, 200)",
    )
    .await
    .expect("INSERT orders failed");

    conn.query_drop(
        "INSERT INTO right_join_users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    )
    .await
    .expect("INSERT users failed");

    // RIGHT JOIN - all right rows, matching left rows or NULL
    let rows: Vec<(Option<i32>, i32, String)> = conn
        .query(
            "SELECT o.amount, u.id, u.name FROM right_join_orders o RIGHT JOIN right_join_users u ON o.user_id = u.id ORDER BY u.id",
        )
        .await
        .expect("RIGHT JOIN failed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (Some(100), 1, "Alice".to_string()));
    assert_eq!(rows[1], (None, 2, "Bob".to_string()));
    assert_eq!(rows[2], (None, 3, "Carol".to_string()));

    conn.query_drop("DROP TABLE right_join_orders")
        .await
        .expect("DROP TABLE orders failed");
    conn.query_drop("DROP TABLE right_join_users")
        .await
        .expect("DROP TABLE users failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_cross_join() {
    let server = TestServer::start("cross_join").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE cross_join_a (x INT)")
        .await
        .expect("CREATE TABLE a failed");

    conn.query_drop("CREATE TABLE cross_join_b (y INT)")
        .await
        .expect("CREATE TABLE b failed");

    conn.query_drop("INSERT INTO cross_join_a (x) VALUES (1), (2)")
        .await
        .expect("INSERT a failed");

    conn.query_drop("INSERT INTO cross_join_b (y) VALUES (10), (20)")
        .await
        .expect("INSERT b failed");

    // CROSS JOIN - cartesian product
    let rows: Vec<(i32, i32)> = conn
        .query("SELECT a.x, b.y FROM cross_join_a a CROSS JOIN cross_join_b b ORDER BY a.x, b.y")
        .await
        .expect("CROSS JOIN failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], (1, 10));
    assert_eq!(rows[1], (1, 20));
    assert_eq!(rows[2], (2, 10));
    assert_eq!(rows[3], (2, 20));

    conn.query_drop("DROP TABLE cross_join_a")
        .await
        .expect("DROP TABLE a failed");
    conn.query_drop("DROP TABLE cross_join_b")
        .await
        .expect("DROP TABLE b failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_join_with_where() {
    let server = TestServer::start("join_where").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE join_where_users (id INT, name VARCHAR(50), active INT)")
        .await
        .expect("CREATE TABLE users failed");

    conn.query_drop("CREATE TABLE join_where_orders (id INT, user_id INT, amount INT)")
        .await
        .expect("CREATE TABLE orders failed");

    conn.query_drop(
        "INSERT INTO join_where_users (id, name, active) VALUES (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Carol', 1)",
    )
    .await
    .expect("INSERT users failed");

    conn.query_drop(
        "INSERT INTO join_where_orders (id, user_id, amount) VALUES (1, 1, 100), (2, 2, 200), (3, 3, 150)",
    )
    .await
    .expect("INSERT orders failed");

    // JOIN with WHERE filter
    let rows: Vec<(String, i32)> = conn
        .query(
            "SELECT u.name, o.amount FROM join_where_users u INNER JOIN join_where_orders o ON u.id = o.user_id WHERE u.active = 1 ORDER BY o.amount",
        )
        .await
        .expect("JOIN with WHERE failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], ("Alice".to_string(), 100));
    assert_eq!(rows[1], ("Carol".to_string(), 150));

    conn.query_drop("DROP TABLE join_where_orders")
        .await
        .expect("DROP TABLE orders failed");
    conn.query_drop("DROP TABLE join_where_users")
        .await
        .expect("DROP TABLE users failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_self_join() {
    let server = TestServer::start("self_join").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE self_join_employees (id INT, name VARCHAR(50), manager_id INT)")
        .await
        .expect("CREATE TABLE employees failed");

    conn.query_drop(
        "INSERT INTO self_join_employees (id, name, manager_id) VALUES (1, 'Alice', NULL), (2, 'Bob', 1), (3, 'Carol', 1), (4, 'Dave', 2)",
    )
    .await
    .expect("INSERT employees failed");

    // Self join - employees with their managers
    let rows: Vec<(String, Option<String>)> = conn
        .query(
            "SELECT e.name, m.name FROM self_join_employees e LEFT JOIN self_join_employees m ON e.manager_id = m.id ORDER BY e.id",
        )
        .await
        .expect("Self JOIN failed");
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], ("Alice".to_string(), None));
    assert_eq!(rows[1], ("Bob".to_string(), Some("Alice".to_string())));
    assert_eq!(rows[2], ("Carol".to_string(), Some("Alice".to_string())));
    assert_eq!(rows[3], ("Dave".to_string(), Some("Bob".to_string())));

    conn.query_drop("DROP TABLE self_join_employees")
        .await
        .expect("DROP TABLE employees failed");

    drop(conn);
    server.shutdown().await;
}

#[tokio::test]
async fn test_join_empty_table() {
    let server = TestServer::start("join_empty").await;
    let mut conn = server.connect().await;

    conn.query_drop("CREATE TABLE join_empty_users (id INT, name VARCHAR(50))")
        .await
        .expect("CREATE TABLE users failed");

    conn.query_drop("CREATE TABLE join_empty_orders (id INT, user_id INT)")
        .await
        .expect("CREATE TABLE orders failed");

    conn.query_drop("INSERT INTO join_empty_users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .expect("INSERT users failed");

    // Empty right table - INNER JOIN returns empty
    let rows: Vec<(i32, String)> = conn
        .query("SELECT u.id, u.name FROM join_empty_users u INNER JOIN join_empty_orders o ON u.id = o.user_id")
        .await
        .expect("JOIN empty inner failed");
    assert_eq!(rows.len(), 0);

    // Empty right table - LEFT JOIN returns all left rows
    let rows: Vec<(i32, String, Option<i32>)> = conn
        .query("SELECT u.id, u.name, o.id FROM join_empty_users u LEFT JOIN join_empty_orders o ON u.id = o.user_id ORDER BY u.id")
        .await
        .expect("JOIN empty left failed");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string(), None));
    assert_eq!(rows[1], (2, "Bob".to_string(), None));

    conn.query_drop("DROP TABLE join_empty_orders")
        .await
        .expect("DROP TABLE orders failed");
    conn.query_drop("DROP TABLE join_empty_users")
        .await
        .expect("DROP TABLE users failed");

    drop(conn);
    server.shutdown().await;
}
