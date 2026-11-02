use polyglot_sql::{parse_one, DialectType, Tokenizer};

#[test]
fn unicode_text_keeps_character_offsets_and_cursor_locations() {
    let sql = "SELECT café, δelta, 'été\n😀', \"naïve\" FROM données";
    let tokens = Tokenizer::default().tokenize(sql).unwrap();
    assert_eq!(
        tokens
            .iter()
            .map(|token| token.text.as_str())
            .collect::<Vec<_>>(),
        [
            "SELECT",
            "café",
            ",",
            "δelta",
            ",",
            "été\n😀",
            ",",
            "naïve",
            "FROM",
            "données"
        ]
    );
    let characters: Vec<_> = sql.chars().collect();
    for token in &tokens {
        let prefix: String = characters[..token.span.end].iter().collect();
        let line = prefix
            .chars()
            .filter(|&character| character == '\n')
            .count()
            + 1;
        let column = prefix.rsplit('\n').next().unwrap().chars().count() + 1;
        assert_eq!(
            (token.span.line, token.span.column),
            (line, column),
            "{token:?}"
        );
        let raw: String = characters[token.span.start..token.span.end]
            .iter()
            .collect();
        assert!(raw.contains(&token.text), "{token:?}: {raw}");
    }
    parse_one(sql, DialectType::PostgreSQL).unwrap();
}

#[test]
fn mixed_identifiers_and_escaped_quotes_make_progress() {
    for sql in [
        "SELECT abcδ, δabc, 中文 FROM t",
        "SELECT 'it''s été', '😀', \"naï\"\"ve\" FROM t",
        "SELECT $$été\n😀$$, $tag$δ$tag$, 1.25e-2 /* π */",
    ] {
        let tokens = Tokenizer::default().tokenize(sql).unwrap();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|token| token.span.end > token.span.start));
        parse_one(sql, DialectType::PostgreSQL).unwrap();
    }
}
