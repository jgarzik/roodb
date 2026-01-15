//! Row type - a collection of datums

use std::hash::{Hash, Hasher};

use super::datum::Datum;
use super::error::{ExecutorError, ExecutorResult};

/// A row of datums
#[derive(Debug, Clone, Default)]
pub struct Row {
    /// The values in this row
    values: Vec<Datum>,
}

impl Row {
    /// Create a new row with the given values
    pub fn new(values: Vec<Datum>) -> Self {
        Row { values }
    }

    /// Create an empty row
    pub fn empty() -> Self {
        Row { values: vec![] }
    }

    /// Get the number of columns in this row
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a datum by index
    pub fn get(&self, index: usize) -> ExecutorResult<&Datum> {
        self.values
            .get(index)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds {
                index,
                row_len: self.values.len(),
            })
    }

    /// Get a datum by index, returns None if out of bounds
    pub fn get_opt(&self, index: usize) -> Option<&Datum> {
        self.values.get(index)
    }

    /// Get a mutable reference to a datum by index
    pub fn get_mut(&mut self, index: usize) -> ExecutorResult<&mut Datum> {
        let len = self.values.len();
        self.values
            .get_mut(index)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds {
                index,
                row_len: len,
            })
    }

    /// Set a datum by index
    pub fn set(&mut self, index: usize, value: Datum) -> ExecutorResult<()> {
        if index >= self.values.len() {
            return Err(ExecutorError::ColumnIndexOutOfBounds {
                index,
                row_len: self.values.len(),
            });
        }
        self.values[index] = value;
        Ok(())
    }

    /// Push a datum to the end of the row
    pub fn push(&mut self, value: Datum) {
        self.values.push(value);
    }

    /// Get all values as a slice
    pub fn values(&self) -> &[Datum] {
        &self.values
    }

    /// Take ownership of values
    pub fn into_values(self) -> Vec<Datum> {
        self.values
    }

    /// Concatenate two rows (for joins)
    pub fn concat(self, other: Row) -> Row {
        let mut values = self.values;
        values.extend(other.values);
        Row { values }
    }

    /// Project specific columns by indices
    pub fn project(&self, indices: &[usize]) -> ExecutorResult<Row> {
        let mut values = Vec::with_capacity(indices.len());
        for &idx in indices {
            values.push(self.get(idx)?.clone());
        }
        Ok(Row { values })
    }

    /// Create an iterator over the datums
    pub fn iter(&self) -> impl Iterator<Item = &Datum> {
        self.values.iter()
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Eq for Row {}

impl Hash for Row {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for datum in &self.values {
            datum.hash(state);
        }
    }
}

impl From<Vec<Datum>> for Row {
    fn from(values: Vec<Datum>) -> Self {
        Row { values }
    }
}

impl IntoIterator for Row {
    type Item = Datum;
    type IntoIter = std::vec::IntoIter<Datum>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a Datum;
    type IntoIter = std::slice::Iter<'a, Datum>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_new() {
        let row = Row::new(vec![Datum::Int(1), Datum::String("hello".to_string())]);
        assert_eq!(row.len(), 2);
    }

    #[test]
    fn test_row_get() {
        let row = Row::new(vec![Datum::Int(42)]);
        assert!(matches!(row.get(0), Ok(Datum::Int(42))));
        assert!(row.get(1).is_err());
    }

    #[test]
    fn test_row_set() {
        let mut row = Row::new(vec![Datum::Int(1)]);
        row.set(0, Datum::Int(2)).unwrap();
        assert!(matches!(row.get(0), Ok(Datum::Int(2))));
    }

    #[test]
    fn test_row_concat() {
        let r1 = Row::new(vec![Datum::Int(1)]);
        let r2 = Row::new(vec![Datum::Int(2)]);
        let combined = r1.concat(r2);
        assert_eq!(combined.len(), 2);
    }

    #[test]
    fn test_row_project() {
        let row = Row::new(vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]);
        let projected = row.project(&[0, 2]).unwrap();
        assert_eq!(projected.len(), 2);
        assert!(matches!(projected.get(0), Ok(Datum::Int(1))));
        assert!(matches!(projected.get(1), Ok(Datum::Int(3))));
    }

    #[test]
    fn test_row_hash_eq() {
        use std::collections::HashSet;

        let r1 = Row::new(vec![Datum::Int(1), Datum::Int(2)]);
        let r2 = Row::new(vec![Datum::Int(1), Datum::Int(2)]);
        let r3 = Row::new(vec![Datum::Int(1), Datum::Int(3)]);

        let mut set = HashSet::new();
        set.insert(r1.clone());
        assert!(set.contains(&r2));
        assert!(!set.contains(&r3));
    }
}
