//! Trie data structure for efficient prefix matching
//!
//! This module provides a trie implementation used for:
//! - Efficient keyword matching in the tokenizer
//! - Time format conversion with overlapping patterns
//! - Schema table name resolution
//!
//! Based on the Python implementation in `sqlglot/trie.py`.

use std::collections::HashMap;

/// Result of searching for a key in a trie
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrieResult {
    /// Key not found in trie
    Failed,
    /// Key is a prefix of an existing key
    Prefix,
    /// Key exists in trie
    Exists,
}

/// A trie (prefix tree) data structure
///
/// Generic over the value type `V`. If no value is needed, use `()`.
///
/// # Example
///
/// ```
/// use polyglot_sql::trie::{Trie, TrieResult};
///
/// let mut trie = Trie::new();
/// trie.insert("cat", 1);
/// trie.insert("car", 2);
///
/// assert_eq!(trie.in_trie("cat"), (TrieResult::Exists, Some(&1)));
/// assert_eq!(trie.in_trie("ca").0, TrieResult::Prefix);
/// assert_eq!(trie.in_trie("dog").0, TrieResult::Failed);
/// ```
#[derive(Debug, Clone)]
pub struct Trie<V> {
    children: HashMap<char, Trie<V>>,
    value: Option<V>,
}

impl<V> Default for Trie<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Trie<V> {
    /// Create a new empty trie
    pub fn new() -> Self {
        Self {
            children: HashMap::new(),
            value: None,
        }
    }

    /// Insert a key-value pair into the trie
    ///
    /// # Arguments
    /// * `key` - The key to insert (a string slice)
    /// * `value` - The value to associate with the key
    pub fn insert(&mut self, key: &str, value: V) {
        let mut current = self;
        for ch in key.chars() {
            current = current.children.entry(ch).or_insert_with(Trie::new);
        }
        current.value = Some(value);
    }

    /// Get the value associated with a key
    ///
    /// Returns `None` if the key doesn't exist or only exists as a prefix.
    pub fn get(&self, key: &str) -> Option<&V> {
        let mut current = self;
        for ch in key.chars() {
            match current.children.get(&ch) {
                Some(child) => current = child,
                None => return None,
            }
        }
        current.value.as_ref()
    }

    /// Check if a key exists in the trie
    ///
    /// Returns a tuple of (TrieResult, Option<&V>) where:
    /// - `TrieResult::Failed` - key not found
    /// - `TrieResult::Prefix` - key is a prefix of an existing key
    /// - `TrieResult::Exists` - key exists in trie
    ///
    /// When the result is `Exists`, the Option will contain the value.
    pub fn in_trie(&self, key: &str) -> (TrieResult, Option<&V>) {
        if key.is_empty() {
            return (TrieResult::Failed, None);
        }

        let mut current = self;
        for ch in key.chars() {
            match current.children.get(&ch) {
                Some(child) => current = child,
                None => return (TrieResult::Failed, None),
            }
        }

        if current.value.is_some() {
            (TrieResult::Exists, current.value.as_ref())
        } else {
            (TrieResult::Prefix, None)
        }
    }

    /// Check if a key exists in the trie, following one character at a time
    ///
    /// This is useful for streaming/incremental matching. Returns:
    /// - `TrieResult::Failed` - character not found from current position
    /// - `TrieResult::Prefix` - character found, but not at end of a word
    /// - `TrieResult::Exists` - character found and at end of a word
    ///
    /// Also returns the subtrie at this position (if any).
    pub fn in_trie_char(&self, ch: char) -> (TrieResult, Option<&Trie<V>>) {
        match self.children.get(&ch) {
            Some(child) => {
                if child.value.is_some() {
                    (TrieResult::Exists, Some(child))
                } else {
                    (TrieResult::Prefix, Some(child))
                }
            }
            None => (TrieResult::Failed, None),
        }
    }

    /// Get the subtrie for a given character
    pub fn get_child(&self, ch: char) -> Option<&Trie<V>> {
        self.children.get(&ch)
    }

    /// Check if this node has a value (is a complete word)
    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    /// Get the value at this node
    pub fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }

    /// Check if the trie is empty
    pub fn is_empty(&self) -> bool {
        self.children.is_empty() && self.value.is_none()
    }

    /// Get all keys in the trie
    pub fn keys(&self) -> Vec<String> {
        let mut result = Vec::new();
        self.collect_keys(String::new(), &mut result);
        result
    }

    fn collect_keys(&self, prefix: String, result: &mut Vec<String>) {
        if self.value.is_some() {
            result.push(prefix.clone());
        }
        for (ch, child) in &self.children {
            let mut new_prefix = prefix.clone();
            new_prefix.push(*ch);
            child.collect_keys(new_prefix, result);
        }
    }
}

/// Create a new trie from an iterator of (key, value) pairs
///
/// # Example
///
/// ```
/// use polyglot_sql::trie::new_trie;
///
/// let trie = new_trie([
///     ("foo".to_string(), 1),
///     ("bar".to_string(), 2),
/// ]);
/// assert_eq!(trie.get("foo"), Some(&1));
/// ```
pub fn new_trie<V, I>(keywords: I) -> Trie<V>
where
    I: IntoIterator<Item = (String, V)>,
{
    let mut trie = Trie::new();
    for (key, value) in keywords {
        trie.insert(&key, value);
    }
    trie
}

/// Create a new trie from an iterator of keys (values are unit type)
///
/// Useful when you only need to check for key presence.
///
/// # Example
///
/// ```
/// use polyglot_sql::trie::{new_trie_from_keys, TrieResult};
///
/// let trie = new_trie_from_keys(["SELECT", "FROM", "WHERE"]);
/// assert_eq!(trie.in_trie("SELECT").0, TrieResult::Exists);
/// ```
pub fn new_trie_from_keys<I, S>(keywords: I) -> Trie<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut trie = Trie::new();
    for key in keywords {
        trie.insert(key.as_ref(), ());
    }
    trie
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_trie() {
        let trie = new_trie([
            ("bla".to_string(), ()),
            ("foo".to_string(), ()),
            ("blab".to_string(), ()),
        ]);

        assert_eq!(trie.in_trie("bla").0, TrieResult::Exists);
        assert_eq!(trie.in_trie("blab").0, TrieResult::Exists);
        assert_eq!(trie.in_trie("foo").0, TrieResult::Exists);
    }

    #[test]
    fn test_in_trie_failed() {
        let trie = new_trie_from_keys(["cat"]);
        assert_eq!(trie.in_trie("bob").0, TrieResult::Failed);
    }

    #[test]
    fn test_in_trie_prefix() {
        let trie = new_trie_from_keys(["cat"]);
        assert_eq!(trie.in_trie("ca").0, TrieResult::Prefix);
    }

    #[test]
    fn test_in_trie_exists() {
        let trie = new_trie_from_keys(["cat"]);
        assert_eq!(trie.in_trie("cat").0, TrieResult::Exists);
    }

    #[test]
    fn test_empty_key() {
        let trie = new_trie_from_keys(["cat"]);
        assert_eq!(trie.in_trie("").0, TrieResult::Failed);
    }

    #[test]
    fn test_get_value() {
        let trie = new_trie([("foo".to_string(), 42), ("bar".to_string(), 100)]);

        assert_eq!(trie.get("foo"), Some(&42));
        assert_eq!(trie.get("bar"), Some(&100));
        assert_eq!(trie.get("baz"), None);
        assert_eq!(trie.get("fo"), None); // Prefix only
    }

    #[test]
    fn test_in_trie_char() {
        let trie = new_trie_from_keys(["cat", "car"]);

        // Start from root
        let (result, subtrie) = trie.in_trie_char('c');
        assert_eq!(result, TrieResult::Prefix);
        assert!(subtrie.is_some());

        // Continue with 'a'
        let subtrie = subtrie.unwrap();
        let (result, subtrie) = subtrie.in_trie_char('a');
        assert_eq!(result, TrieResult::Prefix);
        assert!(subtrie.is_some());

        // Continue with 't' (reaches 'cat')
        let subtrie = subtrie.unwrap();
        let (result, _) = subtrie.in_trie_char('t');
        assert_eq!(result, TrieResult::Exists);

        // Try 'd' which doesn't exist
        let (result, subtrie) = trie.in_trie_char('d');
        assert_eq!(result, TrieResult::Failed);
        assert!(subtrie.is_none());
    }

    #[test]
    fn test_keys() {
        let trie = new_trie_from_keys(["cat", "car", "card"]);
        let mut keys = trie.keys();
        keys.sort();
        assert_eq!(keys, vec!["car", "card", "cat"]);
    }

    #[test]
    fn test_unicode() {
        let trie = new_trie_from_keys(["cafe", "caf\u{00e9}"]); // "caf\u{00e9}" = "cafe" with accent
        assert_eq!(trie.in_trie("cafe").0, TrieResult::Exists);
        assert_eq!(trie.in_trie("caf\u{00e9}").0, TrieResult::Exists);
    }

    #[test]
    fn test_overlapping_prefixes() {
        // Test case from sqlglot: "bla" and "blab"
        let trie = new_trie_from_keys(["bla", "blab"]);

        // "bla" should exist
        assert_eq!(trie.in_trie("bla").0, TrieResult::Exists);

        // "blab" should exist
        assert_eq!(trie.in_trie("blab").0, TrieResult::Exists);

        // "bl" should be prefix
        assert_eq!(trie.in_trie("bl").0, TrieResult::Prefix);
    }
}
