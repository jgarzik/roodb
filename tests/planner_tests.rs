//! Query planner integration tests

use roodb::catalog::{Catalog, ColumnDef, Constraint, DataType, TableDef};
use roodb::planner::{
    CostEstimator, ExplainOutput, LogicalPlan, LogicalPlanBuilder, Optimizer, PhysicalPlan,
    PhysicalPlanner,
};
use roodb::sql::{Parser, Resolver, TypeChecker};

/// Create a test catalog with sample tables
fn test_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    let users = TableDef::new("users")
        .column(ColumnDef::new("id", DataType::Int).nullable(false))
        .column(ColumnDef::new("name", DataType::Varchar(100)))
        .column(ColumnDef::new("email", DataType::Varchar(255)))
        .column(ColumnDef::new("age", DataType::Int))
        .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));

    let orders = TableDef::new("orders")
        .column(ColumnDef::new("id", DataType::Int).nullable(false))
        .column(ColumnDef::new("user_id", DataType::Int))
        .column(ColumnDef::new("total", DataType::Double))
        .column(ColumnDef::new("status", DataType::Varchar(50)))
        .constraint(Constraint::PrimaryKey(vec!["id".to_string()]));

    catalog.create_table(users).unwrap();
    catalog.create_table(orders).unwrap();

    catalog
}

/// Helper to run full planning pipeline
fn plan_query(catalog: &Catalog, sql: &str) -> PhysicalPlan {
    let stmt = Parser::parse_one(sql).unwrap();
    let resolver = Resolver::new(catalog);
    let resolved = resolver.resolve(stmt).unwrap();
    TypeChecker::check(&resolved).unwrap();

    let logical = LogicalPlanBuilder::build(resolved).unwrap();
    let optimized = Optimizer::new().optimize(logical);
    PhysicalPlanner::plan(optimized, catalog).unwrap()
}

// ============ Basic SELECT Tests ============

#[test]
fn test_plan_simple_select() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT id, name FROM users");

    // Should be: Project -> TableScan
    match physical {
        PhysicalPlan::Project { input, expressions } => {
            assert_eq!(expressions.len(), 2);
            assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
        }
        _ => panic!("Expected Project"),
    }
}

#[test]
fn test_plan_select_star() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT * FROM users");

    // Should be: Project -> TableScan
    match physical {
        PhysicalPlan::Project { input, expressions } => {
            // Wildcard expands to all columns
            assert_eq!(expressions.len(), 4); // id, name, email, age
            assert!(matches!(*input, PhysicalPlan::TableScan { .. }));
        }
        _ => panic!("Expected Project"),
    }
}

#[test]
fn test_plan_select_with_filter() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT id FROM users WHERE age > 18");

    // After optimization, filter should be pushed to scan
    // Plan should be: Project -> TableScan(with filter)
    match physical {
        PhysicalPlan::Project { input, .. } => match *input {
            PhysicalPlan::TableScan { filter, .. } => {
                assert!(filter.is_some(), "Filter should be pushed to scan");
            }
            _ => panic!("Expected TableScan after optimization"),
        },
        _ => panic!("Expected Project"),
    }
}

#[test]
fn test_plan_select_with_multiple_filters() {
    let catalog = test_catalog();
    let physical = plan_query(
        &catalog,
        "SELECT id FROM users WHERE age > 18 AND name = 'Alice'",
    );

    // Filters should be combined and pushed to scan
    match physical {
        PhysicalPlan::Project { input, .. } => match *input {
            PhysicalPlan::TableScan { filter, .. } => {
                assert!(filter.is_some());
            }
            _ => panic!("Expected TableScan"),
        },
        _ => panic!("Expected Project"),
    }
}

// ============ ORDER BY / LIMIT Tests ============

#[test]
fn test_plan_select_with_order_by() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT id, name FROM users ORDER BY name ASC");

    // Should be: Sort -> Project -> TableScan
    match physical {
        PhysicalPlan::Sort { input, order_by } => {
            assert_eq!(order_by.len(), 1);
            assert!(order_by[0].1); // ascending
            assert!(matches!(*input, PhysicalPlan::Project { .. }));
        }
        _ => panic!("Expected Sort"),
    }
}

#[test]
fn test_plan_select_with_limit() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT * FROM users LIMIT 10 OFFSET 5");

    // Should be: Limit -> Project -> TableScan
    match physical {
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            assert_eq!(limit, Some(10));
            assert_eq!(offset, Some(5));
            assert!(matches!(*input, PhysicalPlan::Project { .. }));
        }
        _ => panic!("Expected Limit"),
    }
}

#[test]
fn test_plan_select_distinct() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT DISTINCT name FROM users");

    // Should be: HashDistinct -> Project -> TableScan
    match physical {
        PhysicalPlan::HashDistinct { input } => {
            assert!(matches!(*input, PhysicalPlan::Project { .. }));
        }
        _ => panic!("Expected HashDistinct"),
    }
}

// ============ Aggregate Tests ============

#[test]
fn test_plan_count_star() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT COUNT(*) FROM users");

    // Should contain HashAggregate
    fn contains_aggregate(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::HashAggregate { .. } => true,
            PhysicalPlan::Project { input, .. } => contains_aggregate(input),
            PhysicalPlan::Filter { input, .. } => contains_aggregate(input),
            _ => false,
        }
    }
    assert!(contains_aggregate(&physical), "Should contain HashAggregate");
}

#[test]
fn test_plan_aggregate_with_group_by() {
    let catalog = test_catalog();
    let physical = plan_query(
        &catalog,
        "SELECT status, COUNT(*) FROM orders GROUP BY status",
    );

    // Should contain HashAggregate with group_by
    fn find_aggregate(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        match plan {
            PhysicalPlan::HashAggregate { .. } => Some(plan),
            PhysicalPlan::Project { input, .. } => find_aggregate(input),
            PhysicalPlan::Filter { input, .. } => find_aggregate(input),
            _ => None,
        }
    }

    if let Some(PhysicalPlan::HashAggregate { group_by, .. }) = find_aggregate(&physical) {
        assert!(!group_by.is_empty(), "Should have GROUP BY expressions");
    } else {
        panic!("Should contain HashAggregate");
    }
}

// ============ Join Tests ============

#[test]
fn test_plan_cross_join() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT * FROM users, orders");

    // Should contain NestedLoopJoin
    fn contains_join(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::NestedLoopJoin { .. } => true,
            PhysicalPlan::Project { input, .. } => contains_join(input),
            PhysicalPlan::Filter { input, .. } => contains_join(input),
            _ => false,
        }
    }
    assert!(contains_join(&physical), "Should contain NestedLoopJoin");
}

// ============ DML Tests ============

#[test]
fn test_plan_insert() {
    let catalog = test_catalog();
    let physical = plan_query(
        &catalog,
        "INSERT INTO users (id, name) VALUES (1, 'Alice')",
    );

    match physical {
        PhysicalPlan::Insert { table, values, .. } => {
            assert_eq!(table, "users");
            assert_eq!(values.len(), 1);
        }
        _ => panic!("Expected Insert"),
    }
}

#[test]
fn test_plan_update() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "UPDATE users SET name = 'Bob' WHERE id = 1");

    match physical {
        PhysicalPlan::Update {
            table,
            assignments,
            filter,
        } => {
            assert_eq!(table, "users");
            assert_eq!(assignments.len(), 1);
            assert!(filter.is_some());
        }
        _ => panic!("Expected Update"),
    }
}

#[test]
fn test_plan_delete() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "DELETE FROM users WHERE id = 1");

    match physical {
        PhysicalPlan::Delete { table, filter } => {
            assert_eq!(table, "users");
            assert!(filter.is_some());
        }
        _ => panic!("Expected Delete"),
    }
}

// ============ DDL Tests ============

#[test]
fn test_plan_create_table() {
    let catalog = test_catalog();
    let stmt =
        Parser::parse_one("CREATE TABLE test (id INT NOT NULL, name VARCHAR(100))").unwrap();
    let resolver = Resolver::new(&catalog);
    let resolved = resolver.resolve(stmt).unwrap();

    let logical = LogicalPlanBuilder::build(resolved).unwrap();
    let physical = PhysicalPlanner::plan(logical, &catalog).unwrap();

    match physical {
        PhysicalPlan::CreateTable { name, columns, .. } => {
            assert_eq!(name, "test");
            assert_eq!(columns.len(), 2);
        }
        _ => panic!("Expected CreateTable"),
    }
}

#[test]
fn test_plan_drop_table() {
    let catalog = test_catalog();
    let stmt = Parser::parse_one("DROP TABLE IF EXISTS users").unwrap();
    let resolver = Resolver::new(&catalog);
    let resolved = resolver.resolve(stmt).unwrap();

    let logical = LogicalPlanBuilder::build(resolved).unwrap();
    let physical = PhysicalPlanner::plan(logical, &catalog).unwrap();

    match physical {
        PhysicalPlan::DropTable { name, if_exists } => {
            assert_eq!(name, "users");
            assert!(if_exists);
        }
        _ => panic!("Expected DropTable"),
    }
}

// ============ Optimizer Tests ============

#[test]
fn test_optimizer_predicate_pushdown() {
    let catalog = test_catalog();

    let stmt = Parser::parse_one("SELECT id FROM users WHERE age > 18").unwrap();
    let resolver = Resolver::new(&catalog);
    let resolved = resolver.resolve(stmt).unwrap();
    TypeChecker::check(&resolved).unwrap();

    let logical = LogicalPlanBuilder::build(resolved).unwrap();

    // Before optimization, filter is separate from scan
    let has_filter_node = match &logical {
        LogicalPlan::Project { input, .. } => matches!(**input, LogicalPlan::Filter { .. }),
        _ => false,
    };
    assert!(has_filter_node, "Before optimization: Filter should be separate");

    let optimized = Optimizer::new().optimize(logical);

    // After optimization, filter should be pushed to scan
    match optimized {
        LogicalPlan::Project { input, .. } => match *input {
            LogicalPlan::Scan { filter, .. } => {
                assert!(filter.is_some(), "Filter should be pushed to scan");
            }
            _ => panic!("Expected Scan after optimization"),
        },
        _ => panic!("Expected Project"),
    }
}

// ============ Cost Estimator Tests ============

#[test]
fn test_cost_estimator_basic() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT * FROM users");

    let cost = CostEstimator::estimate(&physical, &catalog);
    assert!(cost.rows > 0.0);
    assert!(cost.cpu > 0.0);
    assert!(cost.total() > 0.0);
}

#[test]
fn test_cost_filter_reduces_rows() {
    let catalog = test_catalog();

    let no_filter = plan_query(&catalog, "SELECT * FROM users");
    let with_filter = plan_query(&catalog, "SELECT * FROM users WHERE age > 50");

    let cost_no_filter = CostEstimator::estimate(&no_filter, &catalog);
    let cost_with_filter = CostEstimator::estimate(&with_filter, &catalog);

    // Filter should reduce estimated row count
    assert!(
        cost_with_filter.rows < cost_no_filter.rows,
        "Filter should reduce estimated rows"
    );
}

// ============ EXPLAIN Tests ============

#[test]
fn test_explain_output_basic() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT id, name FROM users WHERE age > 18");

    let explain = ExplainOutput::format(&physical);

    assert!(explain.contains("Project"), "Should show Project node");
    assert!(explain.contains("TableScan"), "Should show TableScan node");
    assert!(explain.contains("users"), "Should show table name");
}

#[test]
fn test_explain_output_with_limit() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT * FROM users LIMIT 10");

    let explain = ExplainOutput::format(&physical);

    assert!(explain.contains("Limit"), "Should show Limit node");
    assert!(explain.contains("limit=10"), "Should show limit value");
}

#[test]
fn test_explain_output_with_sort() {
    let catalog = test_catalog();
    let physical = plan_query(&catalog, "SELECT * FROM users ORDER BY name DESC");

    let explain = ExplainOutput::format(&physical);

    assert!(explain.contains("Sort"), "Should show Sort node");
    assert!(explain.contains("DESC"), "Should show sort direction");
}
