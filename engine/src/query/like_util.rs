//! SQL `LIKE` pattern helpers for future index-aware execution (prefix range, trigram, etc.).

/// If `pattern` is a conservative **prefix-only** SQL `LIKE` pattern, returns the literal prefix.
///
/// Recognized today: exactly one trailing `%`, no other `%`, no `_` (single-char wildcard).
/// Examples: `post-999%` → `Some("post-999")`; `%foo`, `foo_bar%`, `foo%bar%` → `None`.
///
/// Used as the first hook toward **prefix search** (range on sort / trie) without full trigram.
pub fn sql_like_prefix_literal(pattern: &str) -> Option<&str> {
    if pattern.is_empty() || pattern == "%" {
        return None;
    }
    if !pattern.ends_with('%') {
        return None;
    }
    if pattern.contains('_') {
        return None;
    }
    let body = &pattern[..pattern.len() - 1];
    if body.contains('%') {
        return None;
    }
    Some(body)
}

/// If `pattern` is a conservative **contains** SQL `LIKE` pattern, returns the inner literal.
///
/// Recognized: one leading `%` and one trailing `%`, no other `%`, no `_`.
/// Examples: `%gmail.com%` -> `Some("gmail.com")`; `%x`, `x%`, `%a_b%` -> `None`.
pub fn sql_like_contains_literal(pattern: &str) -> Option<&str> {
    if pattern.len() < 3 || !pattern.starts_with('%') || !pattern.ends_with('%') {
        return None;
    }
    if pattern.contains('_') {
        return None;
    }
    let inner = &pattern[1..pattern.len() - 1];
    if inner.is_empty() || inner.contains('%') {
        return None;
    }
    Some(inner)
}

#[cfg(test)]
mod tests {
    use super::{sql_like_contains_literal, sql_like_prefix_literal};

    #[test]
    fn prefix_only_patterns() {
        assert_eq!(sql_like_prefix_literal("post-999%"), Some("post-999"));
        assert_eq!(sql_like_prefix_literal("a%"), Some("a"));
    }

    #[test]
    fn rejects_non_prefix_patterns() {
        assert_eq!(sql_like_prefix_literal("%x"), None);
        assert_eq!(sql_like_prefix_literal("foo%bar%"), None);
        assert_eq!(sql_like_prefix_literal("foo_bar%"), None);
        assert_eq!(sql_like_prefix_literal("%"), None);
        assert_eq!(sql_like_prefix_literal(""), None);
        assert_eq!(sql_like_prefix_literal("exact"), None);
    }

    #[test]
    fn contains_only_patterns() {
        assert_eq!(sql_like_contains_literal("%gmail.com%"), Some("gmail.com"));
        assert_eq!(sql_like_contains_literal("%abc%"), Some("abc"));
    }

    #[test]
    fn rejects_non_contains_patterns() {
        assert_eq!(sql_like_contains_literal("abc%"), None);
        assert_eq!(sql_like_contains_literal("%abc"), None);
        assert_eq!(sql_like_contains_literal("abc"), None);
        assert_eq!(sql_like_contains_literal("%a_b%"), None);
        assert_eq!(sql_like_contains_literal("%%"), None);
        assert_eq!(sql_like_contains_literal("%a%b%"), None);
    }
}
