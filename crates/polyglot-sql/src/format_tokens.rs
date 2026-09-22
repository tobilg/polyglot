//! Internal helpers for converting date/time format token strings.

/// A format cursor whose remaining input always starts on a UTF-8 boundary.
///
/// Token matching never case-folds Unicode literals into ASCII format directives.
#[cfg(any(test, feature = "generate"))]
pub(crate) struct FormatTokenCursor<'a> {
    remaining: &'a str,
}

#[cfg(any(test, feature = "generate"))]
impl<'a> FormatTokenCursor<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        Self { remaining: input }
    }

    pub(crate) fn peek(&self) -> Option<char> {
        self.peek_nth(0)
    }

    pub(crate) fn peek_nth(&self, index: usize) -> Option<char> {
        self.remaining.chars().nth(index)
    }

    pub(crate) fn next_char(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.remaining = &self.remaining[ch.len_utf8()..];
        Some(ch)
    }

    /// Consume a nonempty token, optionally ignoring ASCII letter case.
    /// A failed match leaves the cursor unchanged, including when the requested
    /// prefix length would split a multibyte character.
    pub(crate) fn consume_prefix(&mut self, prefix: &str, ignore_ascii_case: bool) -> bool {
        if prefix.is_empty() {
            return false;
        }
        let Some(candidate) = self.remaining.get(..prefix.len()) else {
            return false;
        };
        let matches = if ignore_ascii_case {
            candidate.eq_ignore_ascii_case(prefix)
        } else {
            candidate == prefix
        };
        if matches {
            self.remaining = &self.remaining[prefix.len()..];
        }
        matches
    }
}

/// Convert format tokens using longest-token matching.
///
/// Unknown text is copied through unchanged. Empty input returns `None`,
/// matching the public `time::format_time` helper.
#[cfg(any(test, feature = "dialect-tsql", feature = "dialect-fabric"))]
pub(crate) fn convert_format_tokens(input: &str, mapping: &[(&str, &str)]) -> Option<String> {
    if input.is_empty() {
        return None;
    }

    let chars: Vec<char> = input.chars().collect();
    let mut result = String::with_capacity(input.len());
    let mut index = 0;

    while index < chars.len() {
        let mut matched: Option<(&str, usize)> = None;

        for (source, target) in mapping {
            let source_len = source.chars().count();
            if source_len == 0 || index + source_len > chars.len() {
                continue;
            }

            if chars[index..index + source_len]
                .iter()
                .copied()
                .eq(source.chars())
                && matched
                    .map(|(_, matched_len)| source_len > matched_len)
                    .unwrap_or(true)
            {
                matched = Some((*target, source_len));
            }
        }

        if let Some((target, source_len)) = matched {
            result.push_str(target);
            index += source_len;
        } else {
            result.push(chars[index]);
            index += 1;
        }
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use super::{convert_format_tokens, FormatTokenCursor};

    #[test]
    fn cursor_preserves_unicode_and_match_boundaries() {
        let mut cursor = FormatTokenCursor::new("é年🦀e\u{301}ßſyyyy");
        assert!(!cursor.consume_prefix("", false));
        assert!(!cursor.consume_prefix("x", false));
        assert!(!cursor.consume_prefix("yyy", true));
        assert_eq!(cursor.peek(), Some('é'));
        assert_eq!(cursor.peek_nth(2), Some('🦀'));
        assert!(cursor.consume_prefix("é", false));
        for ch in ['年', '🦀', 'e', '\u{301}', 'ß', 'ſ'] {
            assert!(!cursor.consume_prefix("SS", true));
            assert!(!cursor.consume_prefix("S", true));
            assert_eq!(cursor.next_char(), Some(ch));
        }
        assert!(!cursor.consume_prefix("YYYY", false));
        assert!(cursor.consume_prefix("YYYY", true));
        assert_eq!(cursor.next_char(), None);
        assert!(!cursor.consume_prefix("Y", true));
        assert_eq!(FormatTokenCursor::new("").peek(), None);
    }

    #[test]
    fn format_tokens_preserve_unicode_literals() {
        assert_eq!(
            convert_format_tokens("éYYYY年MM🦀ßSS", TEST_MAPPING),
            Some("é%Y年MM🦀ß%S".into())
        );
    }

    const TEST_MAPPING: &[(&str, &str)] = &[
        ("TMMonth", "%B"),
        ("TMMon", "%b"),
        ("FMHH24", "%-H"),
        ("HH24", "%H"),
        ("FMDD", "%-d"),
        ("DD", "%d"),
        ("D", "%u"),
        ("YYYY", "%Y"),
        ("YY", "%y"),
        ("MI", "%M"),
        ("SS", "%S"),
    ];

    #[test]
    fn longest_token_match_wins() {
        assert_eq!(
            convert_format_tokens("FMHH24 HH24 FMDD DD D TMMonth TMMon", TEST_MAPPING),
            Some("%-H %H %-d %d %u %B %b".to_string())
        );
    }

    #[test]
    fn unknown_text_passes_through() {
        assert_eq!(
            convert_format_tokens("YYYY-mm-DD literal", TEST_MAPPING),
            Some("%Y-mm-%d literal".to_string())
        );
    }

    #[test]
    fn empty_input_returns_none() {
        assert_eq!(convert_format_tokens("", TEST_MAPPING), None);
    }
}

/// Verified HANA datetime tokens shared by the percent-format function families.
/// Unknown alphabetic tokens, precision masks, and unterminated quoted literals
/// are rejected rather than copied into a different format language.
#[cfg(feature = "generate")]
pub(crate) fn hana_datetime_format(input: &str, mysql_style: bool) -> Option<String> {
    let mut cursor = FormatTokenCursor::new(input);
    let mut result = String::new();
    while let Some(ch) = cursor.peek() {
        if ch == '"' {
            cursor.next_char();
            loop {
                let ch = cursor.next_char()?;
                if ch == '"' {
                    break;
                }
                if ch == '%' {
                    result.push('%');
                }
                result.push(ch);
            }
            continue;
        }
        let mut matched = false;
        for (source, target) in [
            ("HH24", "%H"),
            ("YYYY", "%Y"),
            ("MM", "%m"),
            ("DD", "%d"),
            ("MI", if mysql_style { "%i" } else { "%M" }),
            ("SS", if mysql_style { "%s" } else { "%S" }),
        ] {
            if cursor.consume_prefix(source, true) {
                result.push_str(target);
                matched = true;
                break;
            }
        }
        if !matched {
            if ch.is_ascii_alphabetic() || ch.is_ascii_digit() {
                return None;
            }
            cursor.next_char();
            if ch == '%' {
                result.push('%');
            }
            result.push(ch);
        }
    }
    Some(result)
}
