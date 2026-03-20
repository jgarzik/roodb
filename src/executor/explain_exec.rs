//! EXPLAIN executor — produces MySQL-format EXPLAIN output
//!
//! Walks the physical plan tree and produces rows matching MySQL's 12-column
//! EXPLAIN format: id, select_type, table, partitions, type, possible_keys,
//! key, key_len, ref, rows, filtered, Extra.

use async_trait::async_trait;

use crate::planner::physical::PhysicalPlan;

use super::datum::Datum;
use super::error::ExecutorResult;
use super::row::Row;
use super::Executor;

pub struct ExplainExecutor {
    inner: PhysicalPlan,
    rows: Vec<Row>,
    cursor: usize,
}

impl ExplainExecutor {
    pub fn new(inner: PhysicalPlan) -> Self {
        Self {
            inner,
            rows: Vec::new(),
            cursor: 0,
        }
    }
}

#[async_trait]
impl Executor for ExplainExecutor {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.rows = walk_plan(&self.inner);
        if self.rows.is_empty() {
            // Expression-only queries (e.g., SELECT 1) get a single row
            self.rows.push(make_explain_row([
                "1",
                "SIMPLE",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "No tables used",
            ]));
        }
        self.cursor = 0;
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.cursor < self.rows.len() {
            let row = self.rows[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        Ok(())
    }
}

/// Build an EXPLAIN row from 12 column values.
/// "NULL" string is converted to Datum::Null, everything else to Datum::String.
fn make_explain_row(cols: [&str; 12]) -> Row {
    Row::new(
        cols.iter()
            .map(|s| {
                if *s == "NULL" {
                    Datum::Null
                } else {
                    Datum::String(s.to_string())
                }
            })
            .collect(),
    )
}

/// Walk the physical plan tree and produce MySQL-format EXPLAIN rows.
fn walk_plan(plan: &PhysicalPlan) -> Vec<Row> {
    let mut rows = Vec::new();
    let mut extra_parts: Vec<String> = Vec::new();
    collect_explain_rows(plan, &mut rows, &mut extra_parts);
    rows
}

fn extra_str(extra: &[String]) -> String {
    if extra.is_empty() {
        "NULL".to_string()
    } else {
        extra.join("; ")
    }
}

/// Push an extra annotation only if it's not already present (dedup).
fn push_extra(extra: &mut Vec<String>, value: &str) {
    if !extra.iter().any(|s| s == value) {
        extra.push(value.to_string());
    }
}

fn collect_explain_rows(plan: &PhysicalPlan, rows: &mut Vec<Row>, extra: &mut Vec<String>) {
    match plan {
        PhysicalPlan::TableScan { table, filter, .. } => {
            let mut e = extra.clone();
            if filter.is_some() {
                push_extra(&mut e, "Using where");
            }
            let filtered = if filter.is_some() { "10.00" } else { "100.00" };
            rows.push(make_explain_row([
                "1",
                "SIMPLE",
                table,
                "NULL",
                "ALL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "1000",
                filtered,
                &extra_str(&e),
            ]));
        }

        PhysicalPlan::PointGet { table, .. } => {
            rows.push(make_explain_row([
                "1",
                "SIMPLE",
                table,
                "NULL",
                "const",
                "PRIMARY",
                "PRIMARY",
                "NULL",
                "const",
                "1",
                "100.00",
                &extra_str(extra),
            ]));
        }

        PhysicalPlan::RangeScan {
            table,
            remaining_filter,
            ..
        } => {
            let mut e = extra.clone();
            if remaining_filter.is_some() {
                push_extra(&mut e, "Using where");
            }
            let es = if e.is_empty() {
                "Using index condition".to_string()
            } else {
                e.join("; ")
            };
            let filtered = if remaining_filter.is_some() {
                "10.00"
            } else {
                "100.00"
            };
            rows.push(make_explain_row([
                "1", "SIMPLE", table, "NULL", "range", "PRIMARY", "PRIMARY", "NULL", "NULL", "100",
                filtered, &es,
            ]));
        }

        PhysicalPlan::Filter { input, .. } => {
            push_extra(extra, "Using where");
            collect_explain_rows(input, rows, extra);
            extra.pop();
        }

        PhysicalPlan::Project { input, .. } => {
            collect_explain_rows(input, rows, extra);
        }

        PhysicalPlan::Sort { input, .. } => {
            push_extra(extra, "Using filesort");
            collect_explain_rows(input, rows, extra);
            extra.pop();
        }

        PhysicalPlan::Limit { input, .. } => {
            collect_explain_rows(input, rows, extra);
        }

        PhysicalPlan::HashDistinct { input } => {
            push_extra(extra, "Using temporary");
            collect_explain_rows(input, rows, extra);
            extra.pop();
        }

        PhysicalPlan::HashAggregate { input, .. } => {
            push_extra(extra, "Using temporary");
            collect_explain_rows(input, rows, extra);
            extra.pop();
        }

        PhysicalPlan::NestedLoopJoin { left, right, .. }
        | PhysicalPlan::HashJoin { left, right, .. } => {
            collect_explain_rows(left, rows, extra);
            collect_explain_rows(right, rows, extra);
        }

        PhysicalPlan::SingleRow => {
            rows.push(make_explain_row([
                "1",
                "SIMPLE",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "No tables used",
            ]));
        }

        PhysicalPlan::Insert { table, .. }
        | PhysicalPlan::Update { table, .. }
        | PhysicalPlan::Delete { table, .. } => {
            rows.push(make_explain_row([
                "1", "SIMPLE", table, "NULL", "ALL", "NULL", "NULL", "NULL", "NULL", "NULL",
                "100.00", "NULL",
            ]));
        }

        PhysicalPlan::Explain { inner } => {
            collect_explain_rows(inner, rows, extra);
        }

        // DDL, Auth, Analyze — no EXPLAIN rows
        PhysicalPlan::CreateTable { .. }
        | PhysicalPlan::DropTable { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::DropIndex { .. }
        | PhysicalPlan::CreateDatabase { .. }
        | PhysicalPlan::DropDatabase { .. }
        | PhysicalPlan::CreateUser { .. }
        | PhysicalPlan::DropUser { .. }
        | PhysicalPlan::AlterUser { .. }
        | PhysicalPlan::SetPassword { .. }
        | PhysicalPlan::Grant { .. }
        | PhysicalPlan::Revoke { .. }
        | PhysicalPlan::ShowGrants { .. }
        | PhysicalPlan::AnalyzeTable { .. } => {}
    }
}
