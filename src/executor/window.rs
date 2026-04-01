//! Window function executor.
//!
//! Buffers all input rows, groups by PARTITION BY, sorts each partition
//! by ORDER BY, computes window function values, and appends them to rows.

use async_trait::async_trait;

use super::error::ExecutorResult;
use super::eval::evaluate;
use super::row::Row;
use crate::executor::datum::Datum;
use crate::planner::logical::WindowFuncDesc;
use crate::server::session::UserVariables;

use super::Executor;

/// A window function executor
pub struct Window {
    input: Box<dyn Executor>,
    window_funcs: Vec<WindowFuncDesc>,
    user_variables: UserVariables,
    /// Buffered output rows (computed in open())
    output: Vec<Row>,
    output_idx: usize,
}

impl Window {
    pub fn new(
        input: Box<dyn Executor>,
        window_funcs: Vec<WindowFuncDesc>,
        user_variables: UserVariables,
    ) -> Self {
        Window {
            input,
            window_funcs,
            user_variables,
            output: Vec::new(),
            output_idx: 0,
        }
    }
}

#[async_trait]
impl Executor for Window {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.input.open().await?;

        // Buffer all input rows
        let mut rows = Vec::new();
        while let Some(row) = self.input.next().await? {
            rows.push(row);
        }

        if rows.is_empty() {
            return Ok(());
        }

        // For each window function, compute values and store them
        // We'll process one window function at a time and append columns
        let num_input_cols = rows[0].len();
        let num_window_funcs = self.window_funcs.len();

        // Initialize result: each row gets extra columns for window functions
        let mut result_rows: Vec<Vec<Datum>> = rows
            .iter()
            .map(|r| {
                let mut vals: Vec<Datum> = (0..num_input_cols)
                    .map(|i| r.get(i).cloned().unwrap_or(Datum::Null))
                    .collect();
                vals.extend(std::iter::repeat_n(Datum::Null, num_window_funcs));
                vals
            })
            .collect();

        for (wf_idx, (_, name, args, partition_by, order_by, _)) in
            self.window_funcs.iter().enumerate()
        {
            // Compute partition keys for all rows
            let mut partition_keys: Vec<Vec<Datum>> = Vec::new();
            for row in &rows {
                let key: Vec<Datum> = partition_by
                    .iter()
                    .map(|e| evaluate(e, row, &self.user_variables).unwrap_or(Datum::Null))
                    .collect();
                partition_keys.push(key);
            }

            // Group row indices by partition key
            let mut partitions: Vec<Vec<usize>> = Vec::new();
            let mut partition_map: std::collections::HashMap<Vec<u8>, usize> =
                std::collections::HashMap::new();
            for (i, key) in partition_keys.iter().enumerate() {
                let key_bytes: Vec<u8> = key
                    .iter()
                    .flat_map(|d| d.to_display_string().into_bytes())
                    .collect();
                let partition_idx = partition_map.len();
                let idx = *partition_map.entry(key_bytes).or_insert(partition_idx);
                if idx == partitions.len() {
                    partitions.push(Vec::new());
                }
                partitions[idx].push(i);
            }

            // Sort each partition by ORDER BY
            if !order_by.is_empty() {
                for partition in &mut partitions {
                    partition.sort_by(|&a, &b| {
                        for (expr, ascending) in order_by {
                            let va = evaluate(expr, &rows[a], &self.user_variables)
                                .unwrap_or(Datum::Null);
                            let vb = evaluate(expr, &rows[b], &self.user_variables)
                                .unwrap_or(Datum::Null);
                            let cmp = va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal);
                            let cmp = if *ascending { cmp } else { cmp.reverse() };
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }

            // Compute window function for each row in each partition
            let col_offset = num_input_cols + wf_idx;
            let name_upper = name.to_uppercase();

            for partition in &partitions {
                match name_upper.as_str() {
                    "ROW_NUMBER" => {
                        for (rank, &row_idx) in partition.iter().enumerate() {
                            result_rows[row_idx][col_offset] = Datum::Int((rank + 1) as i64);
                        }
                    }
                    "RANK" => {
                        let mut rank = 1i64;
                        let mut prev_key: Option<Vec<Datum>> = None;
                        for (pos, &row_idx) in partition.iter().enumerate() {
                            let key: Vec<Datum> = order_by
                                .iter()
                                .map(|(e, _)| {
                                    evaluate(e, &rows[row_idx], &self.user_variables)
                                        .unwrap_or(Datum::Null)
                                })
                                .collect();
                            if let Some(ref prev) = prev_key {
                                if key != *prev {
                                    rank = (pos + 1) as i64;
                                }
                            }
                            result_rows[row_idx][col_offset] = Datum::Int(rank);
                            prev_key = Some(key);
                        }
                    }
                    "DENSE_RANK" => {
                        let mut rank = 1i64;
                        let mut prev_key: Option<Vec<Datum>> = None;
                        for &row_idx in partition {
                            let key: Vec<Datum> = order_by
                                .iter()
                                .map(|(e, _)| {
                                    evaluate(e, &rows[row_idx], &self.user_variables)
                                        .unwrap_or(Datum::Null)
                                })
                                .collect();
                            if let Some(ref prev) = prev_key {
                                if key != *prev {
                                    rank += 1;
                                }
                            }
                            result_rows[row_idx][col_offset] = Datum::Int(rank);
                            prev_key = Some(key);
                        }
                    }
                    "NTILE" => {
                        let n = if !args.is_empty() {
                            evaluate(&args[0], &rows[partition[0]], &self.user_variables)
                                .ok()
                                .and_then(|d| d.as_int())
                                .unwrap_or(1) as usize
                        } else {
                            1
                        };
                        let n = n.max(1);
                        let total = partition.len();
                        for (pos, &row_idx) in partition.iter().enumerate() {
                            let bucket = (pos * n / total) + 1;
                            result_rows[row_idx][col_offset] = Datum::Int(bucket as i64);
                        }
                    }
                    "SUM" | "COUNT" | "AVG" | "MIN" | "MAX" => {
                        // Aggregate window: compute over entire partition
                        let mut sum = 0.0f64;
                        let mut count = 0i64;
                        let mut min_val: Option<Datum> = None;
                        let mut max_val: Option<Datum> = None;

                        // Check if this is COUNT(*) — arg is a NULL literal placeholder
                        let is_count_star = name_upper == "COUNT"
                            && (args.is_empty()
                                || matches!(
                                    args.first(),
                                    Some(crate::planner::logical::ResolvedExpr::Literal(
                                        crate::planner::logical::Literal::Null
                                    ))
                                ));

                        for &row_idx in partition {
                            if is_count_star {
                                count += 1;
                            } else if !args.is_empty() {
                                let val = evaluate(&args[0], &rows[row_idx], &self.user_variables)
                                    .unwrap_or(Datum::Null);
                                if !val.is_null() {
                                    count += 1;
                                    if let Some(f) = val.as_float() {
                                        sum += f;
                                    } else if let Some(i) = val.as_int() {
                                        sum += i as f64;
                                    }
                                    if min_val.is_none() || val < *min_val.as_ref().unwrap() {
                                        min_val = Some(val.clone());
                                    }
                                    if max_val.is_none() || val > *max_val.as_ref().unwrap() {
                                        max_val = Some(val.clone());
                                    }
                                }
                            }
                        }

                        let result = match name_upper.as_str() {
                            "SUM" => {
                                if count == 0 {
                                    Datum::Null
                                } else if sum == (sum as i64) as f64 {
                                    Datum::Int(sum as i64)
                                } else {
                                    Datum::Float(sum)
                                }
                            }
                            "COUNT" => Datum::Int(count),
                            "AVG" => {
                                if count == 0 {
                                    Datum::Null
                                } else {
                                    Datum::Float(sum / count as f64)
                                }
                            }
                            "MIN" => min_val.unwrap_or(Datum::Null),
                            "MAX" => max_val.unwrap_or(Datum::Null),
                            _ => Datum::Null,
                        };

                        for &row_idx in partition {
                            result_rows[row_idx][col_offset] = result.clone();
                        }
                    }
                    "LEAD" | "LAG" => {
                        let offset = if args.len() > 1 {
                            evaluate(&args[1], &rows[partition[0]], &self.user_variables)
                                .ok()
                                .and_then(|d| d.as_int())
                                .unwrap_or(1) as isize
                        } else {
                            1
                        };
                        let default_val = if args.len() > 2 {
                            evaluate(&args[2], &rows[partition[0]], &self.user_variables)
                                .unwrap_or(Datum::Null)
                        } else {
                            Datum::Null
                        };

                        for (pos, &row_idx) in partition.iter().enumerate() {
                            let target_pos = if name_upper == "LEAD" {
                                pos as isize + offset
                            } else {
                                pos as isize - offset
                            };
                            let val = if target_pos >= 0
                                && (target_pos as usize) < partition.len()
                                && !args.is_empty()
                            {
                                let target_row_idx = partition[target_pos as usize];
                                evaluate(&args[0], &rows[target_row_idx], &self.user_variables)
                                    .unwrap_or(default_val.clone())
                            } else {
                                default_val.clone()
                            };
                            result_rows[row_idx][col_offset] = val;
                        }
                    }
                    "FIRST_VALUE" => {
                        if !partition.is_empty() && !args.is_empty() {
                            let first_row = partition[0];
                            let val = evaluate(&args[0], &rows[first_row], &self.user_variables)
                                .unwrap_or(Datum::Null);
                            for &row_idx in partition {
                                result_rows[row_idx][col_offset] = val.clone();
                            }
                        }
                    }
                    "LAST_VALUE" => {
                        if !partition.is_empty() && !args.is_empty() {
                            let last_row = *partition.last().unwrap();
                            let val = evaluate(&args[0], &rows[last_row], &self.user_variables)
                                .unwrap_or(Datum::Null);
                            for &row_idx in partition {
                                result_rows[row_idx][col_offset] = val.clone();
                            }
                        }
                    }
                    _ => {
                        // Unknown window function — leave as NULL
                    }
                }
            }
        }

        // Build output rows
        self.output = result_rows.into_iter().map(Row::new).collect();
        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.output_idx < self.output.len() {
            let row = self.output[self.output_idx].clone();
            self.output_idx += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.input.close().await
    }

    fn is_ignore_duplicates(&self) -> bool {
        false
    }
}
