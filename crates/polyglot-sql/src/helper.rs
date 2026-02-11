//! Helper utilities for SQL processing
//!
//! This module provides various utility functions used throughout the codebase:
//! - Safe sequence access
//! - Collection normalization
//! - String manipulation
//! - Fixed-point transformation
//! - Topological sorting
//!
//! Based on the Python implementation in `sqlglot/helper.py`.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// Interval units that operate on date components
pub const DATE_UNITS: &[&str] = &["day", "week", "month", "quarter", "year", "year_month"];

/// Returns the value in `seq` at position `index`, or `None` if `index` is out of bounds.
///
/// Supports negative indexing like Python (e.g., -1 for last element).
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::seq_get;
///
/// let v = vec![1, 2, 3];
/// assert_eq!(seq_get(&v, 0), Some(&1));
/// assert_eq!(seq_get(&v, -1), Some(&3));
/// assert_eq!(seq_get(&v, 10), None);
/// ```
pub fn seq_get<T>(seq: &[T], index: isize) -> Option<&T> {
    let len = seq.len() as isize;
    if len == 0 {
        return None;
    }

    let actual_index = if index < 0 { len + index } else { index };

    if actual_index < 0 || actual_index >= len {
        None
    } else {
        seq.get(actual_index as usize)
    }
}

/// Ensures that a value is wrapped in a Vec if it isn't already a collection.
///
/// This is a generic trait that can be implemented for different types.
pub trait EnsureList {
    type Item;
    fn ensure_list(self) -> Vec<Self::Item>;
}

impl<T> EnsureList for Vec<T> {
    type Item = T;
    fn ensure_list(self) -> Vec<Self::Item> {
        self
    }
}

impl<T> EnsureList for Option<T> {
    type Item = T;
    fn ensure_list(self) -> Vec<Self::Item> {
        match self {
            Some(v) => vec![v],
            None => vec![],
        }
    }
}

/// Wrap a single value in a Vec
pub fn ensure_list<T>(value: T) -> Vec<T> {
    vec![value]
}

/// Wrap an Option in a Vec (empty if None)
pub fn ensure_list_option<T>(value: Option<T>) -> Vec<T> {
    match value {
        Some(v) => vec![v],
        None => vec![],
    }
}

/// Formats any number of string arguments as CSV.
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::csv;
///
/// assert_eq!(csv(&["a", "b", "c"], ", "), "a, b, c");
/// assert_eq!(csv(&["a", "", "c"], ", "), "a, c");
/// ```
pub fn csv(args: &[&str], sep: &str) -> String {
    args.iter()
        .filter(|s| !s.is_empty())
        .copied()
        .collect::<Vec<_>>()
        .join(sep)
}

/// Formats strings as CSV with default separator ", "
pub fn csv_default(args: &[&str]) -> String {
    csv(args, ", ")
}

/// Applies a transformation to a given expression until a fix point is reached.
///
/// # Arguments
/// * `value` - The initial value to transform
/// * `func` - The transformation function
///
/// # Returns
/// The transformed value when it stops changing
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::while_changing;
///
/// // Example: keep dividing by 2 until odd
/// let result = while_changing(16, |n| if n % 2 == 0 { n / 2 } else { n });
/// assert_eq!(result, 1);
/// ```
pub fn while_changing<T, F>(mut value: T, func: F) -> T
where
    T: Clone + PartialEq,
    F: Fn(T) -> T,
{
    loop {
        let new_value = func(value.clone());
        if new_value == value {
            return new_value;
        }
        value = new_value;
    }
}

/// Applies a transformation until a fix point, using a hash function for comparison.
///
/// More efficient than `while_changing` when equality comparison is expensive.
pub fn while_changing_hash<T, F, H>(mut value: T, func: F, hasher: H) -> T
where
    F: Fn(T) -> T,
    H: Fn(&T) -> u64,
{
    loop {
        let start_hash = hasher(&value);
        value = func(value);
        let end_hash = hasher(&value);
        if start_hash == end_hash {
            return value;
        }
    }
}

/// Sorts a directed acyclic graph in topological order.
///
/// # Arguments
/// * `dag` - A map from node to its dependencies (nodes it depends on)
///
/// # Returns
/// A sorted list of nodes, or an error if there's a cycle
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::tsort;
/// use std::collections::{HashMap, HashSet};
///
/// let mut dag = HashMap::new();
/// dag.insert("a", HashSet::from(["b", "c"]));
/// dag.insert("b", HashSet::from(["c"]));
/// dag.insert("c", HashSet::new());
///
/// let sorted = tsort(dag).unwrap();
/// // c comes before b, b comes before a
/// assert!(sorted.iter().position(|x| x == &"c") < sorted.iter().position(|x| x == &"b"));
/// assert!(sorted.iter().position(|x| x == &"b") < sorted.iter().position(|x| x == &"a"));
/// ```
pub fn tsort<T>(mut dag: HashMap<T, HashSet<T>>) -> Result<Vec<T>, TsortError>
where
    T: Clone + Eq + Hash + Ord,
{
    let mut result = Vec::new();

    // Add any missing nodes that appear only as dependencies
    let all_deps: Vec<T> = dag
        .values()
        .flat_map(|deps| deps.iter().cloned())
        .collect();

    for dep in all_deps {
        dag.entry(dep).or_insert_with(HashSet::new);
    }

    while !dag.is_empty() {
        // Find nodes with no dependencies
        let mut current: Vec<T> = dag
            .iter()
            .filter(|(_, deps)| deps.is_empty())
            .map(|(node, _)| node.clone())
            .collect();

        if current.is_empty() {
            return Err(TsortError::CycleDetected);
        }

        // Sort for deterministic output
        current.sort();

        // Remove these nodes from the graph
        for node in &current {
            dag.remove(node);
        }

        // Remove these nodes from all dependency lists
        let current_set: HashSet<_> = current.iter().cloned().collect();
        for deps in dag.values_mut() {
            *deps = deps.difference(&current_set).cloned().collect();
        }

        result.extend(current);
    }

    Ok(result)
}

/// Error type for topological sort
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TsortError {
    CycleDetected,
}

impl std::fmt::Display for TsortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TsortError::CycleDetected => write!(f, "Cycle detected in DAG"),
        }
    }
}

impl std::error::Error for TsortError {}

/// Searches for a new name that doesn't conflict with taken names.
///
/// # Arguments
/// * `taken` - A set of names that are already taken
/// * `base` - The base name to use
///
/// # Returns
/// The original name if available, otherwise `base_2`, `base_3`, etc.
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::find_new_name;
/// use std::collections::HashSet;
///
/// let taken = HashSet::from(["col".to_string(), "col_2".to_string()]);
/// assert_eq!(find_new_name(&taken, "col"), "col_3");
/// assert_eq!(find_new_name(&taken, "other"), "other");
/// ```
pub fn find_new_name(taken: &HashSet<String>, base: &str) -> String {
    if !taken.contains(base) {
        return base.to_string();
    }

    let mut i = 2;
    loop {
        let new_name = format!("{}_{}", base, i);
        if !taken.contains(&new_name) {
            return new_name;
        }
        i += 1;
    }
}

/// Creates a name generator that produces sequential names.
///
/// Returns a closure that generates names like `prefix0`, `prefix1`, etc.
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::name_sequence;
///
/// let mut gen = name_sequence("col");
/// assert_eq!(gen(), "col0");
/// assert_eq!(gen(), "col1");
/// assert_eq!(gen(), "col2");
/// ```
pub fn name_sequence(prefix: &str) -> impl FnMut() -> String {
    let prefix = prefix.to_string();
    let mut counter = 0usize;
    move || {
        let name = format!("{}{}", prefix, counter);
        counter += 1;
        name
    }
}

/// Check if a string can be parsed as an integer
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::is_int;
///
/// assert!(is_int("123"));
/// assert!(is_int("-456"));
/// assert!(!is_int("12.34"));
/// assert!(!is_int("abc"));
/// ```
pub fn is_int(text: &str) -> bool {
    text.parse::<i64>().is_ok()
}

/// Check if a string can be parsed as a float
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::is_float;
///
/// assert!(is_float("12.34"));
/// assert!(is_float("123"));
/// assert!(is_float("-1.5e10"));
/// assert!(!is_float("abc"));
/// ```
pub fn is_float(text: &str) -> bool {
    text.parse::<f64>().is_ok()
}

/// Check if a string is a valid ISO date (YYYY-MM-DD)
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::is_iso_date;
///
/// assert!(is_iso_date("2023-01-15"));
/// assert!(!is_iso_date("01-15-2023"));
/// assert!(!is_iso_date("not a date"));
/// ```
pub fn is_iso_date(text: &str) -> bool {
    // Simple validation: YYYY-MM-DD format
    if text.len() != 10 {
        return false;
    }
    let parts: Vec<&str> = text.split('-').collect();
    if parts.len() != 3 {
        return false;
    }
    if parts[0].len() != 4 || parts[1].len() != 2 || parts[2].len() != 2 {
        return false;
    }

    let year: u32 = match parts[0].parse() {
        Ok(y) => y,
        Err(_) => return false,
    };
    let month: u32 = match parts[1].parse() {
        Ok(m) => m,
        Err(_) => return false,
    };
    let day: u32 = match parts[2].parse() {
        Ok(d) => d,
        Err(_) => return false,
    };

    if month < 1 || month > 12 {
        return false;
    }
    if day < 1 || day > 31 {
        return false;
    }

    // Basic validation of days per month
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            // Leap year check
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => return false,
    };

    day <= days_in_month
}

/// Check if a string is a valid ISO datetime
///
/// Accepts formats like:
/// - `2023-01-15T10:30:00`
/// - `2023-01-15 10:30:00`
/// - `2023-01-15T10:30:00.123456`
/// - `2023-01-15T10:30:00+00:00`
pub fn is_iso_datetime(text: &str) -> bool {
    // Try to find the date portion
    if text.len() < 10 {
        return false;
    }

    // Check date portion
    if !is_iso_date(&text[..10]) {
        return false;
    }

    // If there's a time portion, validate it
    if text.len() > 10 {
        // Must have separator
        let sep = text.chars().nth(10).expect("length checked above");
        if sep != 'T' && sep != ' ' {
            return false;
        }

        // Get time portion (everything after the separator, excluding timezone)
        let time_str = &text[11..];

        // Find where the timezone or fractional seconds end
        let time_end = time_str
            .find('+')
            .or_else(|| time_str.rfind('-'))
            .or_else(|| time_str.find('Z'))
            .unwrap_or(time_str.len());

        let time_without_tz = &time_str[..time_end];

        // Split by '.' to handle fractional seconds
        let (time_part, _frac_part) = match time_without_tz.find('.') {
            Some(idx) => (&time_without_tz[..idx], Some(&time_without_tz[idx + 1..])),
            None => (time_without_tz, None),
        };

        // Validate HH:MM:SS
        if time_part.len() < 8 {
            // Allow HH:MM format
            if time_part.len() != 5 {
                return false;
            }
        }

        let parts: Vec<&str> = time_part.split(':').collect();
        if parts.len() < 2 || parts.len() > 3 {
            return false;
        }

        let hour: u32 = match parts[0].parse() {
            Ok(h) => h,
            Err(_) => return false,
        };
        let minute: u32 = match parts[1].parse() {
            Ok(m) => m,
            Err(_) => return false,
        };

        if hour > 23 || minute > 59 {
            return false;
        }

        if parts.len() == 3 {
            let second: u32 = match parts[2].parse() {
                Ok(s) => s,
                Err(_) => return false,
            };
            if second > 59 {
                return false;
            }
        }
    }

    true
}

/// Converts a camelCase string to UPPER_SNAKE_CASE
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::camel_to_snake_case;
///
/// assert_eq!(camel_to_snake_case("camelCase"), "CAMEL_CASE");
/// assert_eq!(camel_to_snake_case("MyHTTPServer"), "MY_H_T_T_P_SERVER");
/// ```
pub fn camel_to_snake_case(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_ascii_uppercase());
    }
    result
}

/// Converts a snake_case string to CamelCase
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::snake_to_camel_case;
///
/// assert_eq!(snake_to_camel_case("snake_case"), "SnakeCase");
/// assert_eq!(snake_to_camel_case("my_http_server"), "MyHttpServer");
/// ```
pub fn snake_to_camel_case(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut capitalize_next = true;

    for ch in name.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch.to_ascii_lowercase());
        }
    }
    result
}

/// Get the nesting depth of a nested HashMap
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::dict_depth;
/// use std::collections::HashMap;
///
/// let empty: HashMap<String, ()> = HashMap::new();
/// assert_eq!(dict_depth(&empty), 1);
///
/// let mut nested: HashMap<String, HashMap<String, ()>> = HashMap::new();
/// nested.insert("a".into(), HashMap::new());
/// // Note: This returns 1 because we can't traverse into nested hashmaps generically
/// ```
pub fn dict_depth<K, V>(d: &HashMap<K, V>) -> usize
where
    K: std::hash::Hash + Eq,
{
    // In Rust, we can't easily traverse nested hashmaps generically
    // This is a simplified version that returns 1 for any non-empty hashmap
    if d.is_empty() {
        1
    } else {
        1
    }
}

/// Returns the first element from an iterator
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::first;
/// use std::collections::HashSet;
///
/// let set = HashSet::from([1, 2, 3]);
/// let f = first(set.iter());
/// assert!(f.is_some());
/// ```
pub fn first<I, T>(mut iter: I) -> Option<T>
where
    I: Iterator<Item = T>,
{
    iter.next()
}

/// Perform a split on a value and return N words with `None` for missing parts.
///
/// # Arguments
/// * `value` - The string to split
/// * `sep` - The separator
/// * `min_num_words` - Minimum number of words in result
/// * `fill_from_start` - If true, pad with None at start; otherwise at end
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::split_num_words;
///
/// assert_eq!(
///     split_num_words("db.table", ".", 3, true),
///     vec![None, Some("db".to_string()), Some("table".to_string())]
/// );
/// assert_eq!(
///     split_num_words("db.table", ".", 3, false),
///     vec![Some("db".to_string()), Some("table".to_string()), None]
/// );
/// ```
pub fn split_num_words(
    value: &str,
    sep: &str,
    min_num_words: usize,
    fill_from_start: bool,
) -> Vec<Option<String>> {
    let words: Vec<String> = value.split(sep).map(|s| s.to_string()).collect();
    let num_words = words.len();

    if num_words >= min_num_words {
        return words.into_iter().map(Some).collect();
    }

    let padding = min_num_words - num_words;
    let mut result = Vec::with_capacity(min_num_words);

    if fill_from_start {
        result.extend(std::iter::repeat(None).take(padding));
        result.extend(words.into_iter().map(Some));
    } else {
        result.extend(words.into_iter().map(Some));
        result.extend(std::iter::repeat(None).take(padding));
    }

    result
}

/// Flattens a nested collection into a flat iterator
///
/// Due to Rust's type system, this is implemented as a recursive function
/// that works on specific types rather than a generic flatten.
pub fn flatten<T: Clone>(values: &[Vec<T>]) -> Vec<T> {
    values.iter().flat_map(|v| v.iter().cloned()).collect()
}

/// Merge overlapping ranges
///
/// # Arguments
/// * `ranges` - A list of (start, end) tuples representing ranges
///
/// # Returns
/// A list of merged, non-overlapping ranges
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::merge_ranges;
///
/// let ranges = vec![(1, 3), (2, 6), (8, 10)];
/// assert_eq!(merge_ranges(ranges), vec![(1, 6), (8, 10)]);
/// ```
pub fn merge_ranges<T: Ord + Copy>(mut ranges: Vec<(T, T)>) -> Vec<(T, T)> {
    if ranges.is_empty() {
        return vec![];
    }

    ranges.sort_by(|a, b| a.0.cmp(&b.0));

    let mut merged = vec![ranges[0]];

    for (start, end) in ranges.into_iter().skip(1) {
        let last = merged.last_mut().expect("merged initialized with at least one element");
        if start <= last.1 {
            last.1 = std::cmp::max(last.1, end);
        } else {
            merged.push((start, end));
        }
    }

    merged
}

/// Check if a unit is a date unit (operates on date components)
pub fn is_date_unit(unit: &str) -> bool {
    DATE_UNITS.contains(&unit.to_lowercase().as_str())
}

/// Applies an offset to a given integer literal expression for array indexing.
///
/// This is used for dialects that have different array indexing conventions
/// (0-based vs 1-based indexing).
///
/// # Arguments
/// * `expression` - The index expression (should be an integer literal)
/// * `offset` - The offset to apply (e.g., 1 for 0-based to 1-based conversion)
///
/// # Returns
/// The expression with the offset applied if it's an integer literal,
/// otherwise returns the original expression unchanged.
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::apply_index_offset;
///
/// // Convert 0-based index to 1-based
/// assert_eq!(apply_index_offset("0", 1), Some("1".to_string()));
/// assert_eq!(apply_index_offset("5", 1), Some("6".to_string()));
///
/// // Not an integer, return None
/// assert_eq!(apply_index_offset("col", 1), None);
/// ```
pub fn apply_index_offset(expression: &str, offset: i64) -> Option<String> {
    if offset == 0 {
        return Some(expression.to_string());
    }

    // Try to parse as integer
    if let Ok(value) = expression.parse::<i64>() {
        return Some((value + offset).to_string());
    }

    // Not an integer literal, can't apply offset
    None
}

/// A mapping where all keys return the same value.
///
/// This is an optimization for cases like column qualification where many columns
/// from the same table all map to the same table name. Instead of storing
/// N copies of the value, we store it once.
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::SingleValuedMapping;
/// use std::collections::HashSet;
///
/// let columns = HashSet::from(["id".to_string(), "name".to_string(), "email".to_string()]);
/// let mapping = SingleValuedMapping::new(columns, "users".to_string());
///
/// assert_eq!(mapping.get(&"id".to_string()), Some(&"users".to_string()));
/// assert_eq!(mapping.get(&"name".to_string()), Some(&"users".to_string()));
/// assert_eq!(mapping.get(&"unknown".to_string()), None);
/// assert_eq!(mapping.len(), 3);
/// ```
#[derive(Debug, Clone)]
pub struct SingleValuedMapping<K, V>
where
    K: Eq + Hash,
{
    keys: HashSet<K>,
    value: V,
}

impl<K, V> SingleValuedMapping<K, V>
where
    K: Eq + Hash,
{
    /// Create a new SingleValuedMapping from a set of keys and a single value.
    pub fn new(keys: HashSet<K>, value: V) -> Self {
        Self { keys, value }
    }

    /// Create from an iterator of keys and a single value.
    pub fn from_iter<I: IntoIterator<Item = K>>(keys: I, value: V) -> Self {
        Self {
            keys: keys.into_iter().collect(),
            value,
        }
    }

    /// Get the value for a key, if the key exists.
    pub fn get(&self, key: &K) -> Option<&V> {
        if self.keys.contains(key) {
            Some(&self.value)
        } else {
            None
        }
    }

    /// Check if a key exists in the mapping.
    pub fn contains_key(&self, key: &K) -> bool {
        self.keys.contains(key)
    }

    /// Get the number of keys in the mapping.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Check if the mapping is empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Iterate over all keys.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.keys.iter()
    }

    /// Get a reference to the single value.
    pub fn value(&self) -> &V {
        &self.value
    }

    /// Iterate over key-value pairs (all values are the same).
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.keys.iter().map(move |k| (k, &self.value))
    }
}

/// Convert a boolean-like string to an actual boolean.
///
/// # Example
///
/// ```
/// use polyglot_sql::helper::to_bool;
///
/// assert_eq!(to_bool("true"), Some(true));
/// assert_eq!(to_bool("1"), Some(true));
/// assert_eq!(to_bool("false"), Some(false));
/// assert_eq!(to_bool("0"), Some(false));
/// assert_eq!(to_bool("maybe"), None);
/// ```
pub fn to_bool(value: &str) -> Option<bool> {
    let lower = value.to_lowercase();
    match lower.as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_get() {
        let v = vec![1, 2, 3, 4, 5];
        assert_eq!(seq_get(&v, 0), Some(&1));
        assert_eq!(seq_get(&v, 4), Some(&5));
        assert_eq!(seq_get(&v, 5), None);
        assert_eq!(seq_get(&v, -1), Some(&5));
        assert_eq!(seq_get(&v, -5), Some(&1));
        assert_eq!(seq_get(&v, -6), None);

        let empty: Vec<i32> = vec![];
        assert_eq!(seq_get(&empty, 0), None);
        assert_eq!(seq_get(&empty, -1), None);
    }

    #[test]
    fn test_csv() {
        assert_eq!(csv(&["a", "b", "c"], ", "), "a, b, c");
        assert_eq!(csv(&["a", "", "c"], ", "), "a, c");
        assert_eq!(csv(&["", "", ""], ", "), "");
        assert_eq!(csv(&["a"], ", "), "a");
    }

    #[test]
    fn test_while_changing() {
        // Halve until odd
        let result = while_changing(16, |n| if n % 2 == 0 { n / 2 } else { n });
        assert_eq!(result, 1);

        // Already at fixed point
        let result = while_changing(5, |n| if n % 2 == 0 { n / 2 } else { n });
        assert_eq!(result, 5);
    }

    #[test]
    fn test_tsort() {
        let mut dag = HashMap::new();
        dag.insert("a", HashSet::from(["b", "c"]));
        dag.insert("b", HashSet::from(["c"]));
        dag.insert("c", HashSet::new());

        let sorted = tsort(dag).unwrap();
        assert_eq!(sorted, vec!["c", "b", "a"]);
    }

    #[test]
    fn test_tsort_cycle() {
        let mut dag = HashMap::new();
        dag.insert("a", HashSet::from(["b"]));
        dag.insert("b", HashSet::from(["a"]));

        let result = tsort(dag);
        assert!(result.is_err());
    }

    #[test]
    fn test_find_new_name() {
        let taken = HashSet::from(["col".to_string(), "col_2".to_string()]);
        assert_eq!(find_new_name(&taken, "col"), "col_3");
        assert_eq!(find_new_name(&taken, "other"), "other");

        let empty = HashSet::new();
        assert_eq!(find_new_name(&empty, "col"), "col");
    }

    #[test]
    fn test_name_sequence() {
        let mut gen = name_sequence("a");
        assert_eq!(gen(), "a0");
        assert_eq!(gen(), "a1");
        assert_eq!(gen(), "a2");
    }

    #[test]
    fn test_is_int() {
        assert!(is_int("123"));
        assert!(is_int("-456"));
        assert!(is_int("0"));
        assert!(!is_int("12.34"));
        assert!(!is_int("abc"));
        assert!(!is_int(""));
    }

    #[test]
    fn test_is_float() {
        assert!(is_float("12.34"));
        assert!(is_float("123"));
        assert!(is_float("-1.5e10"));
        assert!(is_float("0.0"));
        assert!(!is_float("abc"));
        assert!(!is_float(""));
    }

    #[test]
    fn test_is_iso_date() {
        assert!(is_iso_date("2023-01-15"));
        assert!(is_iso_date("2024-02-29")); // Leap year
        assert!(!is_iso_date("2023-02-29")); // Not a leap year
        assert!(!is_iso_date("01-15-2023"));
        assert!(!is_iso_date("2023-13-01")); // Invalid month
        assert!(!is_iso_date("2023-01-32")); // Invalid day
        assert!(!is_iso_date("not a date"));
    }

    #[test]
    fn test_is_iso_datetime() {
        assert!(is_iso_datetime("2023-01-15T10:30:00"));
        assert!(is_iso_datetime("2023-01-15 10:30:00"));
        assert!(is_iso_datetime("2023-01-15T10:30:00.123456"));
        assert!(is_iso_datetime("2023-01-15T10:30:00+00:00"));
        assert!(is_iso_datetime("2023-01-15"));
        assert!(!is_iso_datetime("not a datetime"));
        assert!(!is_iso_datetime("2023-01-15X10:30:00")); // Invalid separator
    }

    #[test]
    fn test_camel_to_snake_case() {
        assert_eq!(camel_to_snake_case("camelCase"), "CAMEL_CASE");
        assert_eq!(camel_to_snake_case("PascalCase"), "PASCAL_CASE");
        assert_eq!(camel_to_snake_case("simple"), "SIMPLE");
    }

    #[test]
    fn test_snake_to_camel_case() {
        assert_eq!(snake_to_camel_case("snake_case"), "SnakeCase");
        assert_eq!(snake_to_camel_case("my_http_server"), "MyHttpServer");
        assert_eq!(snake_to_camel_case("simple"), "Simple");
    }

    #[test]
    fn test_split_num_words() {
        assert_eq!(
            split_num_words("db.table", ".", 3, true),
            vec![None, Some("db".to_string()), Some("table".to_string())]
        );
        assert_eq!(
            split_num_words("db.table", ".", 3, false),
            vec![Some("db".to_string()), Some("table".to_string()), None]
        );
        assert_eq!(
            split_num_words("catalog.db.table", ".", 3, true),
            vec![Some("catalog".to_string()), Some("db".to_string()), Some("table".to_string())]
        );
        assert_eq!(
            split_num_words("db.table", ".", 1, true),
            vec![Some("db".to_string()), Some("table".to_string())]
        );
    }

    #[test]
    fn test_merge_ranges() {
        assert_eq!(merge_ranges(vec![(1, 3), (2, 6)]), vec![(1, 6)]);
        assert_eq!(
            merge_ranges(vec![(1, 3), (2, 6), (8, 10)]),
            vec![(1, 6), (8, 10)]
        );
        assert_eq!(merge_ranges(vec![(1, 5), (2, 3)]), vec![(1, 5)]);
        assert_eq!(merge_ranges::<i32>(vec![]), vec![]);
    }

    #[test]
    fn test_is_date_unit() {
        assert!(is_date_unit("day"));
        assert!(is_date_unit("MONTH"));
        assert!(is_date_unit("Year"));
        assert!(!is_date_unit("hour"));
        assert!(!is_date_unit("minute"));
    }

    #[test]
    fn test_apply_index_offset() {
        // Basic offset application
        assert_eq!(apply_index_offset("0", 1), Some("1".to_string()));
        assert_eq!(apply_index_offset("5", 1), Some("6".to_string()));
        assert_eq!(apply_index_offset("10", -1), Some("9".to_string()));

        // No offset
        assert_eq!(apply_index_offset("5", 0), Some("5".to_string()));

        // Negative numbers
        assert_eq!(apply_index_offset("-1", 1), Some("0".to_string()));

        // Not an integer - returns None
        assert_eq!(apply_index_offset("col", 1), None);
        assert_eq!(apply_index_offset("1.5", 1), None);
        assert_eq!(apply_index_offset("abc", 1), None);
    }

    #[test]
    fn test_single_valued_mapping() {
        let columns = HashSet::from([
            "id".to_string(),
            "name".to_string(),
            "email".to_string(),
        ]);
        let mapping = SingleValuedMapping::new(columns, "users".to_string());

        // Get existing keys
        assert_eq!(mapping.get(&"id".to_string()), Some(&"users".to_string()));
        assert_eq!(mapping.get(&"name".to_string()), Some(&"users".to_string()));
        assert_eq!(mapping.get(&"email".to_string()), Some(&"users".to_string()));

        // Get non-existing key
        assert_eq!(mapping.get(&"unknown".to_string()), None);

        // Length
        assert_eq!(mapping.len(), 3);
        assert!(!mapping.is_empty());

        // Contains key
        assert!(mapping.contains_key(&"id".to_string()));
        assert!(!mapping.contains_key(&"unknown".to_string()));

        // Value access
        assert_eq!(mapping.value(), &"users".to_string());
    }

    #[test]
    fn test_single_valued_mapping_from_iter() {
        let mapping = SingleValuedMapping::from_iter(
            vec!["a".to_string(), "b".to_string()],
            42,
        );

        assert_eq!(mapping.get(&"a".to_string()), Some(&42));
        assert_eq!(mapping.get(&"b".to_string()), Some(&42));
        assert_eq!(mapping.len(), 2);
    }

    #[test]
    fn test_to_bool() {
        assert_eq!(to_bool("true"), Some(true));
        assert_eq!(to_bool("TRUE"), Some(true));
        assert_eq!(to_bool("True"), Some(true));
        assert_eq!(to_bool("1"), Some(true));

        assert_eq!(to_bool("false"), Some(false));
        assert_eq!(to_bool("FALSE"), Some(false));
        assert_eq!(to_bool("False"), Some(false));
        assert_eq!(to_bool("0"), Some(false));

        assert_eq!(to_bool("maybe"), None);
        assert_eq!(to_bool("yes"), None);
        assert_eq!(to_bool("no"), None);
    }
}
