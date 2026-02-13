//! Hash join executor
//!
//! Builds a hash table from the right (build) side, then probes with each
//! left (probe) row for O(n+m) equi-join performance.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::planner::logical::{JoinType, ResolvedExpr};

use super::datum::Datum;
use super::error::ExecutorResult;
use super::eval::eval;
use super::row::Row;
use super::Executor;

/// Hash join executor
///
/// Algorithm:
/// 1. Build phase: materialize right side into a hash table keyed by equi-join columns
/// 2. Probe phase: for each left row, look up matching right rows via hash
/// 3. Emit matched pairs (applying any remaining non-equi condition)
/// 4. For outer joins, emit unmatched rows with NULLs
pub struct HashJoin {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_type: JoinType,
    /// Column indices in left input that form the equi-join key
    left_keys: Vec<usize>,
    /// Column indices in right input that form the equi-join key
    right_keys: Vec<usize>,
    /// Remaining non-equi condition (evaluated on combined row)
    condition: Option<ResolvedExpr>,
    left_width: usize,
    right_width: usize,

    // Build side
    /// Hash table: equi-key → indices into right_rows
    hash_table: HashMap<Vec<Datum>, Vec<usize>>,
    /// All right-side rows
    right_rows: Vec<Row>,
    /// Tracks which right rows have been matched (for right/full outer join)
    right_matched: Vec<bool>,

    // Probe state
    current_left: Option<Row>,
    /// Indices into right_rows that match current left key
    current_matches: Vec<usize>,
    match_pos: usize,
    /// Whether current left row has matched any right row
    left_matched: bool,
    /// Whether the main probe loop is done
    main_loop_done: bool,
    /// Position for emitting unmatched right rows (right/full outer)
    unmatched_right_pos: usize,
}

impl HashJoin {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_type: JoinType,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        condition: Option<ResolvedExpr>,
        left_width: usize,
        right_width: usize,
    ) -> Self {
        HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            condition,
            left_width,
            right_width,
            hash_table: HashMap::new(),
            right_rows: Vec::new(),
            right_matched: Vec::new(),
            current_left: None,
            current_matches: Vec::new(),
            match_pos: 0,
            left_matched: false,
            main_loop_done: false,
            unmatched_right_pos: 0,
        }
    }

    /// Extract equi-join key from a row using the given column indices
    fn extract_key(row: &Row, key_indices: &[usize]) -> Vec<Datum> {
        key_indices
            .iter()
            .map(|&idx| row.get(idx).cloned().unwrap_or(Datum::Null))
            .collect()
    }

    /// Returns true if any datum in the key is NULL.
    /// Per SQL semantics, NULL keys must never match in joins.
    fn key_has_null(key: &[Datum]) -> bool {
        key.iter().any(|d| matches!(d, Datum::Null))
    }

    fn null_row(width: usize) -> Row {
        Row::new(vec![Datum::Null; width])
    }

    /// Check remaining non-equi condition on combined row
    fn check_condition(&self, combined: &Row) -> ExecutorResult<bool> {
        match &self.condition {
            Some(cond) => {
                let result = eval(cond, combined)?;
                Ok(result.as_bool().unwrap_or(false))
            }
            None => Ok(true),
        }
    }
}

#[async_trait]
impl Executor for HashJoin {
    async fn open(&mut self) -> ExecutorResult<()> {
        self.left.open().await?;
        self.right.open().await?;

        // Build phase: materialize right side into hash table
        self.right_rows.clear();
        self.hash_table.clear();

        while let Some(row) = self.right.next().await? {
            let key = Self::extract_key(&row, &self.right_keys);
            let idx = self.right_rows.len();
            self.right_rows.push(row);
            // SQL: NULL keys never match, so skip indexing them
            if !Self::key_has_null(&key) {
                self.hash_table.entry(key).or_default().push(idx);
            }
        }

        self.right_matched = vec![false; self.right_rows.len()];

        // Start probe phase
        self.current_left = self.left.next().await?;
        if let Some(ref left_row) = self.current_left {
            let key = Self::extract_key(left_row, &self.left_keys);
            // SQL: NULL keys never match
            self.current_matches = if Self::key_has_null(&key) {
                Vec::new()
            } else {
                self.hash_table.get(&key).cloned().unwrap_or_default()
            };
        }
        self.match_pos = 0;
        self.left_matched = false;
        self.main_loop_done = false;
        self.unmatched_right_pos = 0;

        Ok(())
    }

    async fn next(&mut self) -> ExecutorResult<Option<Row>> {
        if !self.main_loop_done {
            loop {
                let left_row = match &self.current_left {
                    Some(row) => row.clone(),
                    None => {
                        self.main_loop_done = true;
                        break;
                    }
                };

                // Try remaining matches for current left row
                while self.match_pos < self.current_matches.len() {
                    let right_idx = self.current_matches[self.match_pos];
                    self.match_pos += 1;

                    let right_row = &self.right_rows[right_idx];
                    let combined = Row::concat_ref(&left_row, right_row);

                    if self.check_condition(&combined)? {
                        self.left_matched = true;
                        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                            self.right_matched[right_idx] = true;
                        }
                        return Ok(Some(combined));
                    }
                }

                // No more matches for this left row
                // For left/full outer join, emit unmatched left row with NULL right
                if !self.left_matched && matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    let null_right = Self::null_row(self.right_width);
                    let combined = Row::concat_ref(&left_row, &null_right);

                    // Advance to next left row before returning
                    self.current_left = self.left.next().await?;
                    if let Some(ref lr) = self.current_left {
                        let key = Self::extract_key(lr, &self.left_keys);
                        self.current_matches = if Self::key_has_null(&key) {
                            Vec::new()
                        } else {
                            self.hash_table.get(&key).cloned().unwrap_or_default()
                        };
                    }
                    self.match_pos = 0;
                    self.left_matched = false;

                    return Ok(Some(combined));
                }

                // Advance to next left row
                self.current_left = self.left.next().await?;
                if let Some(ref lr) = self.current_left {
                    let key = Self::extract_key(lr, &self.left_keys);
                    self.current_matches = if Self::key_has_null(&key) {
                        Vec::new()
                    } else {
                        self.hash_table.get(&key).cloned().unwrap_or_default()
                    };
                }
                self.match_pos = 0;
                self.left_matched = false;
            }
        }

        // Emit unmatched right rows for right/full outer join
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            while self.unmatched_right_pos < self.right_rows.len() {
                let pos = self.unmatched_right_pos;
                self.unmatched_right_pos += 1;

                if !self.right_matched[pos] {
                    let null_left = Self::null_row(self.left_width);
                    let right_row = self.right_rows[pos].clone();
                    return Ok(Some(null_left.concat(right_row)));
                }
            }
        }

        Ok(None)
    }

    async fn close(&mut self) -> ExecutorResult<()> {
        self.hash_table.clear();
        self.right_rows.clear();
        self.right_matched.clear();
        self.current_left = None;
        self.current_matches.clear();
        self.left.close().await?;
        self.right.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::DataType;
    use crate::planner::logical::{BinaryOp, ResolvedColumn};

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

    fn mock(rows: Vec<Row>) -> Box<dyn Executor> {
        Box::new(MockExecutor { rows, position: 0 })
    }

    #[tokio::test]
    async fn test_hash_inner_join() {
        // Left: (id, name)  Right: (user_id, order)
        let left = mock(vec![
            Row::new(vec![Datum::Int(1), Datum::String("alice".into())]),
            Row::new(vec![Datum::Int(2), Datum::String("bob".into())]),
            Row::new(vec![Datum::Int(3), Datum::String("carol".into())]),
        ]);
        let right = mock(vec![
            Row::new(vec![Datum::Int(1), Datum::String("order1".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("order2".into())]),
            Row::new(vec![Datum::Int(2), Datum::String("order3".into())]),
        ]);

        // Equi: left.col0 = right.col0
        let mut join = HashJoin::new(
            left,
            right,
            JoinType::Inner,
            vec![0], // left key: col 0
            vec![0], // right key: col 0
            None,
            2,
            2,
        );
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        // alice matches 2, bob matches 1, carol matches 0
        assert_eq!(results.len(), 3);
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_left_join() {
        let left = mock(vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(2)]),
            Row::new(vec![Datum::Int(3)]), // no match
        ]);
        let right = mock(vec![
            Row::new(vec![Datum::Int(1), Datum::String("a".into())]),
            Row::new(vec![Datum::Int(2), Datum::String("b".into())]),
        ]);

        let mut join = HashJoin::new(left, right, JoinType::Left, vec![0], vec![0], None, 1, 2);
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        assert_eq!(results.len(), 3); // 1→a, 2→b, 3→NULL,NULL

        // Row with id=3 should have NULL right side
        let row3 = &results[2];
        assert_eq!(row3.get(0).unwrap().as_int(), Some(3));
        assert!(row3.get(1).unwrap().is_null());
        assert!(row3.get(2).unwrap().is_null());

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_right_join() {
        let left = mock(vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(2)]),
        ]);
        let right = mock(vec![
            Row::new(vec![Datum::Int(1), Datum::String("a".into())]),
            Row::new(vec![Datum::Int(3), Datum::String("c".into())]), // no match
        ]);

        let mut join = HashJoin::new(left, right, JoinType::Right, vec![0], vec![0], None, 1, 2);
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        assert_eq!(results.len(), 2); // 1→a, NULL→c
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_full_join() {
        let left = mock(vec![
            Row::new(vec![Datum::Int(1)]),
            Row::new(vec![Datum::Int(3)]), // no match on right
        ]);
        let right = mock(vec![
            Row::new(vec![Datum::Int(1), Datum::String("a".into())]),
            Row::new(vec![Datum::Int(2), Datum::String("b".into())]), // no match on left
        ]);

        let mut join = HashJoin::new(left, right, JoinType::Full, vec![0], vec![0], None, 1, 2);
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        // 1→a, 3→NULL,NULL, NULL→2,b
        assert_eq!(results.len(), 3);
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_join_with_remaining_condition() {
        // Left: (id, val)  Right: (id, threshold)
        // Equi on id, remaining: left.val > right.threshold
        let left = mock(vec![
            Row::new(vec![Datum::Int(1), Datum::Int(10)]),
            Row::new(vec![Datum::Int(1), Datum::Int(5)]),
        ]);
        let right = mock(vec![Row::new(vec![Datum::Int(1), Datum::Int(7)])]);

        // Remaining condition: combined col 1 (left.val) > combined col 3 (right.threshold)
        let remaining = ResolvedExpr::BinaryOp {
            left: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "l".into(),
                name: "val".into(),
                index: 1,
                data_type: DataType::Int,
                nullable: false,
            })),
            op: BinaryOp::Gt,
            right: Box::new(ResolvedExpr::Column(ResolvedColumn {
                table: "r".into(),
                name: "threshold".into(),
                index: 3,
                data_type: DataType::Int,
                nullable: false,
            })),
            result_type: DataType::Boolean,
        };

        let mut join = HashJoin::new(
            left,
            right,
            JoinType::Inner,
            vec![0],
            vec![0],
            Some(remaining),
            2,
            2,
        );
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        // Only (1,10) matches because 10 > 7, (1,5) fails because 5 > 7 is false
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get(1).unwrap().as_int(), Some(10));

        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_join_empty_right() {
        let left = mock(vec![Row::new(vec![Datum::Int(1)])]);
        let right = mock(vec![]);

        let mut join = HashJoin::new(left, right, JoinType::Inner, vec![0], vec![0], None, 1, 1);
        join.open().await.unwrap();

        let mut count = 0;
        while join.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 0);
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_join_empty_left() {
        let left = mock(vec![]);
        let right = mock(vec![Row::new(vec![Datum::Int(1)])]);

        let mut join = HashJoin::new(left, right, JoinType::Inner, vec![0], vec![0], None, 1, 1);
        join.open().await.unwrap();

        let mut count = 0;
        while join.next().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 0);
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_join_multi_key() {
        // Composite equi-join key: (col0, col1)
        let left = mock(vec![
            Row::new(vec![Datum::Int(1), Datum::String("a".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("b".into())]),
        ]);
        let right = mock(vec![
            Row::new(vec![
                Datum::Int(1),
                Datum::String("a".into()),
                Datum::Int(100),
            ]),
            Row::new(vec![
                Datum::Int(1),
                Datum::String("b".into()),
                Datum::Int(200),
            ]),
        ]);

        let mut join = HashJoin::new(
            left,
            right,
            JoinType::Inner,
            vec![0, 1], // left keys
            vec![0, 1], // right keys
            None,
            2,
            3,
        );
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        assert_eq!(results.len(), 2);
        // (1,"a") matches (1,"a",100)
        assert_eq!(results[0].get(4).unwrap().as_int(), Some(100));
        // (1,"b") matches (1,"b",200)
        assert_eq!(results[1].get(4).unwrap().as_int(), Some(200));

        join.close().await.unwrap();
    }

    // --- NULL key handling tests (SQL: NULL != NULL in join keys) ---

    #[tokio::test]
    async fn test_hash_join_inner_null_keys_do_not_match() {
        let left = mock(vec![
            Row::new(vec![Datum::Null, Datum::String("left_null".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("left_one".into())]),
        ]);
        let right = mock(vec![
            Row::new(vec![Datum::Null, Datum::String("right_null".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("right_one".into())]),
        ]);

        let mut join = HashJoin::new(
            left, right, JoinType::Inner, vec![0], vec![0], None, 2, 2,
        );
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        // Only Int(1) matches; NULL keys must not join
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get(0).unwrap().as_int(), Some(1));
        assert_eq!(results[0].get(2).unwrap().as_int(), Some(1));
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_join_left_outer_null_keys() {
        let left = mock(vec![
            Row::new(vec![Datum::Null, Datum::String("left_null".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("left_one".into())]),
        ]);
        let right = mock(vec![
            Row::new(vec![Datum::Null, Datum::String("right_null".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("right_one".into())]),
        ]);

        let mut join = HashJoin::new(
            left, right, JoinType::Left, vec![0], vec![0], None, 2, 2,
        );
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        // Both left rows appear; NULL-keyed left row gets NULL right columns
        assert_eq!(results.len(), 2);
        for row in &results {
            if row.get(0).unwrap().is_null() {
                // Unmatched: right side must be NULL
                assert!(row.get(2).unwrap().is_null());
                assert!(row.get(3).unwrap().is_null());
            } else {
                // Matched: right side has real values
                assert_eq!(row.get(2).unwrap().as_int(), Some(1));
            }
        }
        join.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_hash_join_full_outer_null_keys() {
        let left = mock(vec![
            Row::new(vec![Datum::Null, Datum::String("left_null".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("left_one".into())]),
        ]);
        let right = mock(vec![
            Row::new(vec![Datum::Null, Datum::String("right_null".into())]),
            Row::new(vec![Datum::Int(1), Datum::String("right_one".into())]),
        ]);

        let mut join = HashJoin::new(
            left, right, JoinType::Full, vec![0], vec![0], None, 2, 2,
        );
        join.open().await.unwrap();

        let mut results = Vec::new();
        while let Some(row) = join.next().await.unwrap() {
            results.push(row);
        }

        // 3 rows: joined(1,1), unmatched left(NULL), unmatched right(NULL)
        assert_eq!(results.len(), 3);

        let mut saw_joined = false;
        let mut saw_left_null = false;
        let mut saw_right_null = false;
        for row in &results {
            match (row.get(0).unwrap().is_null(), row.get(2).unwrap().is_null()) {
                (false, false) => {
                    // Joined row
                    assert_eq!(row.get(0).unwrap().as_int(), Some(1));
                    assert_eq!(row.get(2).unwrap().as_int(), Some(1));
                    saw_joined = true;
                }
                (false, true) => {
                    // Unmatched left (NULL-keyed), right side padded with NULLs
                    // But left key is NULL here... actually this is left_null row
                    panic!("non-null left with null right shouldn't happen in this dataset");
                }
                (true, true) => {
                    // Unmatched left NULL-key row (right side padded) or
                    // unmatched right NULL-key row (left side padded)
                    let left_payload = row.get(1).unwrap();
                    let right_payload = row.get(3).unwrap();
                    if left_payload.as_str() == Some("left_null") && right_payload.is_null() {
                        saw_left_null = true;
                    } else if left_payload.is_null()
                        && right_payload.as_str() == Some("right_null")
                    {
                        saw_right_null = true;
                    } else {
                        panic!("Unexpected NULL/NULL row: {row:?}");
                    }
                }
                (true, false) => {
                    // Unmatched right row (left padded with NULLs)
                    // right key is NULL but right payload present
                    if row.get(3).unwrap().as_str() == Some("right_null") {
                        saw_right_null = true;
                    } else {
                        panic!("Unexpected row");
                    }
                }
            }
        }
        assert!(saw_joined, "missing joined row");
        assert!(saw_left_null, "missing unmatched left NULL-key row");
        assert!(saw_right_null, "missing unmatched right NULL-key row");
        join.close().await.unwrap();
    }
}
