//! Aggregate executor
//!
//! Implements GROUP BY with aggregate functions (COUNT, SUM, AVG, MIN, MAX).

use std::collections::HashMap;

use async_trait::async_trait;

use crate::planner::logical::expr::AggregateFunc;
use crate::sql::ResolvedExpr;

use super::datum::Datum;
use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Accumulator for a single aggregate function
#[derive(Debug, Clone)]
enum Accumulator {
    Count(i64),
    Sum(Option<f64>),
    Avg { sum: f64, count: i64 },
    Min(Option<Datum>),
    Max(Option<Datum>),
}

impl Accumulator {
    fn new(func_name: &str) -> Self {
        match func_name.to_uppercase().as_str() {
            "COUNT" => Accumulator::Count(0),
            "SUM" => Accumulator::Sum(None),
            "AVG" => Accumulator::Avg { sum: 0.0, count: 0 },
            "MIN" => Accumulator::Min(None),
            "MAX" => Accumulator::Max(None),
            _ => Accumulator::Count(0), // fallback
        }
    }

    fn accumulate(&mut self, value: &Datum) {
        match self {
            Accumulator::Count(n) => {
                if !value.is_null() {
                    *n += 1;
                }
            }
            Accumulator::Sum(sum) => {
                if let Some(v) = value.as_float() {
                    *sum = Some(sum.unwrap_or(0.0) + v);
                } else if let Some(v) = value.as_int() {
                    *sum = Some(sum.unwrap_or(0.0) + v as f64);
                }
            }
            Accumulator::Avg { sum, count } => {
                if let Some(v) = value.as_float() {
                    *sum += v;
                    *count += 1;
                } else if let Some(v) = value.as_int() {
                    *sum += v as f64;
                    *count += 1;
                }
            }
            Accumulator::Min(min) => {
                if !value.is_null() {
                    match min {
                        None => *min = Some(value.clone()),
                        Some(m) if value < m => *min = Some(value.clone()),
                        _ => {}
                    }
                }
            }
            Accumulator::Max(max) => {
                if !value.is_null() {
                    match max {
                        None => *max = Some(value.clone()),
                        Some(m) if value > m => *max = Some(value.clone()),
                        _ => {}
                    }
                }
            }
        }
    }

    fn finalize(&self) -> Datum {
        match self {
            Accumulator::Count(n) => Datum::Int(*n),
            Accumulator::Sum(sum) => match sum {
                Some(s) => Datum::Float(*s),
                None => Datum::Null,
            },
            Accumulator::Avg { sum, count } => {
                if *count == 0 {
                    Datum::Null
                } else {
                    Datum::Float(*sum / *count as f64)
                }
            }
            Accumulator::Min(min) => min.clone().unwrap_or(Datum::Null),
            Accumulator::Max(max) => max.clone().unwrap_or(Datum::Null),
        }
    }
}

/// Hash aggregate executor
pub struct HashAggregate {
    /// Input executor
    input: Box<dyn Executor>,
    /// Group by expressions
    group_by: Vec<ResolvedExpr>,
    /// Aggregate functions with output aliases
    aggregates: Vec<(AggregateFunc, String)>,
    /// Grouped results: group_key -> (group_values, accumulators)
    groups: HashMap<Vec<u8>, (Vec<Datum>, Vec<Accumulator>)>,
    /// Iterator over groups for output
    output: Vec<Row>,
    /// Current position in output
    position: usize,
}

impl HashAggregate {
    /// Create a new hash aggregate executor
    pub fn new(
        input: Box<dyn Executor>,
        group_by: Vec<ResolvedExpr>,
        aggregates: Vec<(AggregateFunc, String)>,
    ) -> Self {
        HashAggregate {
            input,
            group_by,
            aggregates,
            groups: HashMap::new(),
            output: Vec::new(),
            position: 0,
        }
    }

    /// Create a hash key from group values
    fn make_group_key(values: &[Datum]) -> Vec<u8> {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for v in values {
            v.hash(&mut hasher);
        }
        hasher.finish().to_le_bytes().to_vec()
    }

    /// Check if this is a COUNT(*) aggregate
    /// COUNT(*) is parsed with a single Literal::Null argument
    fn is_count_star(agg: &AggregateFunc) -> bool {
        if agg.name.to_uppercase() != "COUNT" {
            return false;
        }
        // COUNT(*) has a single null literal argument from the resolver
        agg.args.len() == 1
            && matches!(
                &agg.args[0],
                ResolvedExpr::Literal(crate::sql::Literal::Null)
            )
    }
}

#[async_trait]
impl Executor for HashAggregate {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.groups.clear();
        self.output.clear();
        self.position = 0;

        self.input.open().await?;

        // Process all input rows
        while let Some(row) = self.input.next().await? {
            // Evaluate group by expressions
            let mut group_values = Vec::with_capacity(self.group_by.len());
            for expr in &self.group_by {
                group_values.push(eval(expr, &row)?);
            }

            let group_key = Self::make_group_key(&group_values);

            // Get or create group entry
            let (_, accumulators) = self.groups.entry(group_key).or_insert_with(|| {
                let accs: Vec<_> = self
                    .aggregates
                    .iter()
                    .map(|(agg, _)| Accumulator::new(&agg.name))
                    .collect();
                (group_values.clone(), accs)
            });

            // Accumulate values for each aggregate
            for (i, (agg, _)) in self.aggregates.iter().enumerate() {
                // Evaluate aggregate argument
                let value = if agg.args.is_empty() || Self::is_count_star(agg) {
                    // COUNT(*) case - count every row
                    Datum::Int(1)
                } else {
                    eval(&agg.args[0], &row)?
                };
                accumulators[i].accumulate(&value);
            }
        }

        // Build output rows
        for (group_values, accumulators) in self.groups.values() {
            let mut row_values = group_values.clone();
            for acc in accumulators {
                row_values.push(acc.finalize());
            }
            self.output.push(Row::new(row_values));
        }

        // Handle case with no groups (aggregates without GROUP BY over empty input)
        if self.output.is_empty() && self.group_by.is_empty() && !self.aggregates.is_empty() {
            let mut row_values = Vec::new();
            for (agg, _) in &self.aggregates {
                let acc = Accumulator::new(&agg.name);
                row_values.push(acc.finalize());
            }
            self.output.push(Row::new(row_values));
        }

        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if self.position >= self.output.len() {
            return Ok(None);
        }
        let row = self.output[self.position].clone();
        self.position += 1;
        Ok(Some(row))
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.groups.clear();
        self.output.clear();
        self.position = 0;
        self.input.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::sql::ResolvedColumn;

    struct MockExecutor {
        rows: Vec<Row>,
        position: usize,
    }

    #[async_trait]
    impl Executor for MockExecutor {
        async fn open(&mut self) -> ExecutorResult<()> {
            self.position = 0;
            Ok(())
        }

        async fn next(&mut self) -> ExecutorResult<Option<Row>> {
            if self.position >= self.rows.len() {
                return Ok(None);
            }
            let row = self.rows[self.position].clone();
            self.position += 1;
            Ok(Some(row))
        }

        async fn close(&mut self) -> ExecutorResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_count_star() {
        let rows = vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(2)]),
            Row::new(vec![Datum::Int(3)]),
        ];
        let input = Box::new(MockExecutor { rows, position: 0 });

        let aggregates = vec![(
            AggregateFunc {
                name: "COUNT".to_string(),
                args: vec![],
                distinct: false,
                result_type: DataType::BigInt,
            },
            "count".to_string(),
        )];

        let mut agg = HashAggregate::new(input, vec![], aggregates);
        agg.open().await.unwrap();

        let result = agg.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_int(), Some(3));

        assert!(agg.next().await.unwrap().is_none());
        agg.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sum() {
        let rows = vec![
            Row::new(vec![Datum::Int(10)]),
            Row::new(vec![Datum::Int(20)]),
            Row::new(vec![Datum::Int(30)]),
        ];
        let input = Box::new(MockExecutor { rows, position: 0 });

        let aggregates = vec![(
            AggregateFunc {
                name: "SUM".to_string(),
                args: vec![ResolvedExpr::Column(ResolvedColumn {
                    table: "t".to_string(),
                    name: "c".to_string(),
                    index: 0,
                    data_type: DataType::Int,
                    nullable: false,
                })],
                distinct: false,
                result_type: DataType::Double,
            },
            "sum".to_string(),
        )];

        let mut agg = HashAggregate::new(input, vec![], aggregates);
        agg.open().await.unwrap();

        let result = agg.next().await.unwrap().unwrap();
        assert_eq!(result.get(0).unwrap().as_float(), Some(60.0));

        agg.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_group_by() {
        // (category, value)
        let rows = vec![
            Row::new(vec![Datum::String("a".to_string()), Datum::Int(10)]),
            Row::new(vec![Datum::String("b".to_string()), Datum::Int(20)]),
            Row::new(vec![Datum::String("a".to_string()), Datum::Int(30)]),
        ];
        let input = Box::new(MockExecutor { rows, position: 0 });

        let group_by = vec![ResolvedExpr::Column(ResolvedColumn {
            table: "t".to_string(),
            name: "category".to_string(),
            index: 0,
            data_type: DataType::Text,
            nullable: false,
        })];

        let aggregates = vec![(
            AggregateFunc {
                name: "SUM".to_string(),
                args: vec![ResolvedExpr::Column(ResolvedColumn {
                    table: "t".to_string(),
                    name: "value".to_string(),
                    index: 1,
                    data_type: DataType::Int,
                    nullable: false,
                })],
                distinct: false,
                result_type: DataType::Double,
            },
            "sum".to_string(),
        )];

        let mut agg = HashAggregate::new(input, group_by, aggregates);
        agg.open().await.unwrap();

        let mut results: HashMap<String, f64> = HashMap::new();
        while let Some(row) = agg.next().await.unwrap() {
            let cat = row.get(0).unwrap().as_str().unwrap().to_string();
            let sum = row.get(1).unwrap().as_float().unwrap();
            results.insert(cat, sum);
        }

        assert_eq!(results.get("a"), Some(&40.0));
        assert_eq!(results.get("b"), Some(&20.0));

        agg.close().await.unwrap();
    }
}
