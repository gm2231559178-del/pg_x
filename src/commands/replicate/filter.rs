//! The `--where` row filter: a grammar layer (tokenizer + parser producing an
//! inspectable AST with SQL precedence) and a semantics layer (a pure
//! three-valued evaluator against a row).
//!
//! Grammar: `expr := or`, `or := and (OR and)*`, `and := primary (AND primary)*`,
//! `primary := '(' expr ')' | comparison`, `comparison := col IS [NOT] NULL |
//! col OP value`. `AND` binds tighter than `OR`, and parentheses override it.
//!
//! NULL semantics (three-valued): a comparison with a NULL operand is NULL
//! (never true), so `col = NULL` and `col != NULL` never match. A missing
//! column and an `Unchanged` column both count as NULL. `IS NULL` / `IS NOT
//! NULL` never propagate NULL. `AND`/`OR` follow Kleene logic. Numeric
//! comparison parses both sides as numbers when both parse, else falls back to
//! a string comparison.

use anyhow::{bail, Context, Result};

use super::table_prefix::{split_table_prefix, TableKey};
use crate::replication::event::{ColVal, Row, WalEvent};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value {
    Str(String),
    Num(f64),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operator {
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FilterExpr {
    Compare {
        col: String,
        op: Operator,
        val: Value,
    },
    IsNull(String),
    IsNotNull(String),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
}

impl FilterExpr {
    /// Three-valued evaluation against a row: `Some(true)` / `Some(false)` when
    /// the outcome is known, `None` when it is NULL / unknown.
    ///
    /// | condition                                  | outcome      |
    /// |--------------------------------------------|--------------|
    /// | comparison with a NULL / missing operand   | `None`       |
    /// | `col = NULL` / `col != NULL`               | `None`       |
    /// | `col IS NULL` / `col IS NOT NULL`          | `Some(bool)` |
    /// | `AND` with a NULL operand                  | `None` unless the other operand is `false` |
    /// | `OR` with a NULL operand                   | `None` unless the other operand is `true`  |
    pub(crate) fn eval(&self, row: &Row) -> Option<bool> {
        match self {
            FilterExpr::Compare { col, op, val } => compare(row, col, *op, val),
            FilterExpr::IsNull(col) => Some(is_null_value(row, col)),
            FilterExpr::IsNotNull(col) => Some(!is_null_value(row, col)),
            FilterExpr::And(a, b) => match (a.eval(row), b.eval(row)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            FilterExpr::Or(a, b) => match (a.eval(row), b.eval(row)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
        }
    }

    /// Convenience for the session gate: `None` (NULL) counts as `false`, so a
    /// row is only forwarded when the filter is known to be true.
    pub(crate) fn evaluate(&self, row: &Row) -> bool {
        self.eval(row) == Some(true)
    }
}

fn is_null_value(row: &Row, col: &str) -> bool {
    matches!(row.get(col), None | Some(ColVal::Null | ColVal::Unchanged))
}

fn compare(row: &Row, col: &str, op: Operator, val: &Value) -> Option<bool> {
    let cell = row.get(col)?;
    let lhs = match cell {
        ColVal::Text(s) => s.as_str(),
        ColVal::Null | ColVal::Unchanged => return None,
    };
    if matches!(val, Value::Null) {
        return None;
    }
    match numeric_pair(lhs, val) {
        Some((a, b)) => Some(apply(op, a, b)),
        None => Some(apply_str(op, lhs, &literal_str(val))),
    }
}

fn numeric_pair(lhs: &str, val: &Value) -> Option<(f64, f64)> {
    let b = match val {
        Value::Num(n) => *n,
        Value::Str(s) => s.parse().ok()?,
        Value::Null => return None,
    };
    let a = lhs.parse().ok()?;
    Some((a, b))
}

fn apply(op: Operator, a: f64, b: f64) -> bool {
    match op {
        Operator::Eq => a == b,
        Operator::Neq => a != b,
        Operator::Gt => a > b,
        Operator::Lt => a < b,
        Operator::Ge => a >= b,
        Operator::Le => a <= b,
    }
}

fn apply_str(op: Operator, a: &str, b: &str) -> bool {
    match op {
        Operator::Eq => a == b,
        Operator::Neq => a != b,
        Operator::Gt => a > b,
        Operator::Lt => a < b,
        Operator::Ge => a >= b,
        Operator::Le => a <= b,
    }
}

fn literal_str(val: &Value) -> String {
    match val {
        Value::Str(s) => s.clone(),
        Value::Num(n) => n.to_string(),
        Value::Null => String::new(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tokenizer
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Number(f64),
    Eq,
    Neq,
    Gt,
    Lt,
    Ge,
    Le,
    And,
    Or,
    Not,
    Is,
    Null,
    LParen,
    RParen,
}

struct Lexer<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().peekable(),
        }
    }

    fn skip_ws(&mut self) {
        while self.chars.peek().is_some_and(|c| c.is_ascii_whitespace()) {
            self.chars.next();
        }
    }

    fn next_token(&mut self) -> Result<Option<Token>> {
        self.skip_ws();
        match self.chars.peek() {
            None => Ok(None),
            Some('(') => {
                self.chars.next();
                Ok(Some(Token::LParen))
            }
            Some(')') => {
                self.chars.next();
                Ok(Some(Token::RParen))
            }
            Some('\'') => Ok(Some(self.lex_string()?)),
            Some('=') => {
                self.chars.next();
                Ok(Some(Token::Eq))
            }
            Some('!') => {
                self.chars.next();
                match self.chars.peek() {
                    Some('=') => {
                        self.chars.next();
                        Ok(Some(Token::Neq))
                    }
                    _ => bail!("expected '=' after '!'"),
                }
            }
            Some('<') => {
                self.chars.next();
                match self.chars.peek() {
                    Some('=') => {
                        self.chars.next();
                        Ok(Some(Token::Le))
                    }
                    Some('>') => {
                        self.chars.next();
                        Ok(Some(Token::Neq))
                    }
                    _ => Ok(Some(Token::Lt)),
                }
            }
            Some('>') => {
                self.chars.next();
                match self.chars.peek() {
                    Some('=') => {
                        self.chars.next();
                        Ok(Some(Token::Ge))
                    }
                    _ => Ok(Some(Token::Gt)),
                }
            }
            Some('-') => {
                if self
                    .chars
                    .clone()
                    .nth(1)
                    .is_some_and(|c| c.is_ascii_digit())
                {
                    Ok(Some(Token::Number(self.lex_number()?)))
                } else {
                    bail!("unexpected '-' in filter expression")
                }
            }
            Some(c) if c.is_ascii_digit() => Ok(Some(Token::Number(self.lex_number()?))),
            Some(c) if c.is_alphanumeric() || *c == '_' => Ok(Some(self.lex_word()?)),
            Some(c) => bail!("unexpected character '{c}' in filter expression"),
        }
    }

    fn lex_string(&mut self) -> Result<Token> {
        self.chars.next();
        let mut s = String::new();
        loop {
            match self.chars.next() {
                Some('\'') => {
                    if self.chars.peek() == Some(&'\'') {
                        self.chars.next();
                        s.push('\'');
                    } else {
                        return Ok(Token::Str(s));
                    }
                }
                Some(c) => s.push(c),
                None => bail!("unterminated string literal"),
            }
        }
    }

    fn lex_number(&mut self) -> Result<f64> {
        let mut s = String::new();
        if self.chars.peek() == Some(&'-') {
            s.push(self.chars.next().unwrap());
        }
        let mut has_dot = false;
        while let Some(c) = self.chars.peek() {
            if c.is_ascii_digit() {
                s.push(self.chars.next().unwrap());
            } else if *c == '.' && !has_dot {
                has_dot = true;
                s.push(self.chars.next().unwrap());
            } else {
                break;
            }
        }
        s.parse::<f64>()
            .with_context(|| format!("invalid number literal '{s}'"))
    }

    fn lex_word(&mut self) -> Result<Token> {
        let mut s = String::new();
        while let Some(c) = self.chars.peek() {
            if c.is_alphanumeric() || *c == '_' {
                s.push(self.chars.next().unwrap());
            } else {
                break;
            }
        }
        Ok(match s.to_uppercase().as_str() {
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            _ => Token::Ident(s),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Parser
// ─────────────────────────────────────────────────────────────────────────────

type BinaryOp = fn(FilterExpr, FilterExpr) -> FilterExpr;

fn binary_precedence(tok: &Token) -> Option<(u8, BinaryOp)> {
    match tok {
        Token::Or => Some((1, |a, b| FilterExpr::Or(Box::new(a), Box::new(b)))),
        Token::And => Some((2, |a, b| FilterExpr::And(Box::new(a), Box::new(b)))),
        _ => None,
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn parse(&mut self) -> Result<FilterExpr> {
        if self.tokens.is_empty() {
            bail!("empty filter expression");
        }
        let expr = self.parse_binary(1)?;
        if let Some(tok) = self.peek() {
            bail!(
                "unexpected trailing {} after filter expression",
                token_desc(Some(tok))
            );
        }
        Ok(expr)
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let tok = self.tokens.get(self.pos).cloned();
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn parse_binary(&mut self, min_prec: u8) -> Result<FilterExpr> {
        let mut lhs = self.parse_primary()?;
        while let Some((prec, combine)) = self.peek().and_then(binary_precedence) {
            if prec < min_prec {
                break;
            }
            self.pos += 1;
            let rhs = self.parse_binary(prec + 1)?;
            lhs = combine(lhs, rhs);
        }
        Ok(lhs)
    }

    fn parse_primary(&mut self) -> Result<FilterExpr> {
        match self.peek() {
            Some(Token::LParen) => {
                self.pos += 1;
                let inner = self.parse_binary(1)?;
                match self.next() {
                    Some(Token::RParen) => Ok(inner),
                    _ => bail!("expected ')' to close parenthesized expression"),
                }
            }
            Some(Token::Ident(_)) => self.parse_comparison(),
            tok => bail!(
                "expected a column comparison or '(' at the start of an expression, found {}",
                token_desc(tok)
            ),
        }
    }

    fn parse_comparison(&mut self) -> Result<FilterExpr> {
        let col = match self.next() {
            Some(Token::Ident(c)) => c,
            _ => bail!("expected a column name"),
        };
        match self.peek() {
            Some(Token::Is) => {
                self.pos += 1;
                let negated = matches!(self.peek(), Some(Token::Not));
                if negated {
                    self.pos += 1;
                }
                match self.next() {
                    Some(Token::Null) => Ok(if negated {
                        FilterExpr::IsNotNull(col)
                    } else {
                        FilterExpr::IsNull(col)
                    }),
                    _ => bail!(
                        "expected NULL after IS{}, found {}",
                        if negated { " NOT" } else { "" },
                        token_desc(self.peek())
                    ),
                }
            }
            Some(
                tok @ (Token::Eq | Token::Neq | Token::Gt | Token::Lt | Token::Ge | Token::Le),
            ) => {
                let op = match tok {
                    Token::Eq => Operator::Eq,
                    Token::Neq => Operator::Neq,
                    Token::Gt => Operator::Gt,
                    Token::Lt => Operator::Lt,
                    Token::Ge => Operator::Ge,
                    Token::Le => Operator::Le,
                    _ => unreachable!(),
                };
                self.pos += 1;
                let val = self.parse_literal()?;
                Ok(FilterExpr::Compare { col, op, val })
            }
            _ => bail!(
                "expected a comparison operator or IS after column '{col}', found {}",
                token_desc(self.peek())
            ),
        }
    }

    fn parse_literal(&mut self) -> Result<Value> {
        let tok = self.next();
        match tok {
            Some(Token::Str(s)) => Ok(Value::Str(s)),
            Some(Token::Number(n)) => Ok(Value::Num(n)),
            Some(Token::Null) => Ok(Value::Null),
            other => bail!(
                "expected a string, number, or NULL after the comparison operator, found {}",
                token_desc(other.as_ref())
            ),
        }
    }
}

fn token_desc(tok: Option<&Token>) -> String {
    match tok {
        None => "end of expression".to_string(),
        Some(Token::Ident(s)) => format!("identifier '{s}'"),
        Some(Token::Str(_)) => "a string literal".to_string(),
        Some(Token::Number(n)) => format!("number '{n}'"),
        Some(Token::LParen) => "'('".to_string(),
        Some(Token::RParen) => "')'".to_string(),
        Some(Token::Is) => "'IS'".to_string(),
        Some(Token::Not) => "'NOT'".to_string(),
        Some(Token::Null) => "'NULL'".to_string(),
        Some(Token::And) => "'AND'".to_string(),
        Some(Token::Or) => "'OR'".to_string(),
        Some(Token::Eq) => "'='".to_string(),
        Some(Token::Neq) => "'!=' or '<>'".to_string(),
        Some(Token::Gt) => "'>'".to_string(),
        Some(Token::Lt) => "'<'".to_string(),
        Some(Token::Ge) => "'>='".to_string(),
        Some(Token::Le) => "'<='".to_string(),
    }
}

pub(crate) fn parse_filter_expr(input: &str) -> Result<FilterExpr> {
    let mut tokens = Vec::new();
    let mut lexer = Lexer::new(input);
    while let Some(tok) = lexer.next_token()? {
        tokens.push(tok);
    }
    Parser { tokens, pos: 0 }.parse()
}

// ─────────────────────────────────────────────────────────────────────────────
// RowFilter — the set of filters applied to each decoded WAL event
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct RowFilter {
    filters: Vec<(TableKey, FilterExpr)>,
}

impl RowFilter {
    pub(crate) fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Assemble the row filters from config then CLI sources. Config rules are
    /// parsed first and CLI rules second; every rule that applies to a row must
    /// hold for the row to be forwarded (AND semantics), so rule order does not
    /// change the outcome — the ordering is still fixed and deterministic.
    pub(crate) fn from_sources(config_filters: &[String], cli_filters: &[String]) -> Result<Self> {
        let mut rf = RowFilter::new();
        for arg in config_filters.iter().chain(cli_filters) {
            let (table_key, expr) = parse_where_arg(arg)?;
            rf.add(table_key, expr);
        }
        Ok(rf)
    }

    pub(crate) fn add(&mut self, table_key: TableKey, expr: FilterExpr) {
        self.filters.push((table_key, expr));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub(crate) fn should_forward(&self, event: &WalEvent) -> bool {
        if self.filters.is_empty() {
            return true;
        }
        let (schema, table, row_option) = match event {
            WalEvent::Insert {
                schema, table, new, ..
            } => (schema, table, Some(new)),
            WalEvent::Update {
                schema, table, new, ..
            } => (schema, table, Some(new)),
            WalEvent::Delete {
                schema, table, old, ..
            } => (schema, table, Some(old)),
            _ => return true,
        };
        let row = match row_option {
            Some(r) => r,
            None => return true,
        };
        for (key, expr) in &self.filters {
            let applies = match key {
                Some((s, t)) => s == schema && t == table,
                None => true,
            };
            if !applies {
                continue;
            }
            if !expr.evaluate(row) {
                return false;
            }
        }
        true
    }
}

pub(crate) fn parse_where_arg(arg: &str) -> Result<(TableKey, FilterExpr)> {
    let (table_key, expr_str) = split_table_prefix(arg)?;
    let expr_str = expr_str.trim();
    if expr_str.is_empty() {
        bail!("empty filter expression");
    }
    let expr = parse_filter_expr(expr_str)?;
    Ok((table_key, expr))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp(col: &str, op: Operator, val: Value) -> FilterExpr {
        FilterExpr::Compare {
            col: col.to_string(),
            op,
            val,
        }
    }

    fn row_cv(pairs: &[(&str, ColVal)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn text_row(pairs: &[(&str, &str)]) -> Row {
        row_cv(
            &pairs
                .iter()
                .map(|(k, v)| (*k, ColVal::Text(v.to_string())))
                .collect::<Vec<_>>(),
        )
    }

    // ── Precedence / AST shape ───────────────────────────────────────────────

    #[test]
    fn and_binds_tighter_than_or() {
        let expr = parse_filter_expr("a = 1 OR b = 2 AND c = 3").unwrap();
        assert_eq!(
            expr,
            FilterExpr::Or(
                Box::new(cmp("a", Operator::Eq, Value::Num(1.0))),
                Box::new(FilterExpr::And(
                    Box::new(cmp("b", Operator::Eq, Value::Num(2.0))),
                    Box::new(cmp("c", Operator::Eq, Value::Num(3.0))),
                )),
            )
        );
    }

    #[test]
    fn parentheses_override_precedence() {
        let expr = parse_filter_expr("(a = 1 OR b = 2) AND c = 3").unwrap();
        assert_eq!(
            expr,
            FilterExpr::And(
                Box::new(FilterExpr::Or(
                    Box::new(cmp("a", Operator::Eq, Value::Num(1.0))),
                    Box::new(cmp("b", Operator::Eq, Value::Num(2.0))),
                )),
                Box::new(cmp("c", Operator::Eq, Value::Num(3.0))),
            )
        );
    }

    #[test]
    fn and_is_left_associative() {
        let expr = parse_filter_expr("a = 1 AND b = 2 AND c = 3").unwrap();
        assert_eq!(
            expr,
            FilterExpr::And(
                Box::new(FilterExpr::And(
                    Box::new(cmp("a", Operator::Eq, Value::Num(1.0))),
                    Box::new(cmp("b", Operator::Eq, Value::Num(2.0))),
                )),
                Box::new(cmp("c", Operator::Eq, Value::Num(3.0))),
            )
        );
    }

    #[test]
    fn is_null_and_is_not_null_ast() {
        let expr = parse_filter_expr("deleted_at IS NULL AND status IS NOT NULL").unwrap();
        assert_eq!(
            expr,
            FilterExpr::And(
                Box::new(FilterExpr::IsNull("deleted_at".to_string())),
                Box::new(FilterExpr::IsNotNull("status".to_string())),
            )
        );
    }

    // ── Numeric vs string comparison ─────────────────────────────────────────

    #[test]
    fn numeric_literal_matches_decimal_text() {
        let expr = parse_filter_expr("amount = 100").unwrap();
        assert!(expr.evaluate(&text_row(&[("amount", "100.0")])));
        assert!(!expr.evaluate(&text_row(&[("amount", "100.5")])));
    }

    #[test]
    fn string_literal_that_is_numeric_compares_numerically() {
        let expr = parse_filter_expr("amount = '100'").unwrap();
        assert!(expr.evaluate(&text_row(&[("amount", "100.0")])));
    }

    #[test]
    fn non_numeric_falls_back_to_string() {
        let expr = parse_filter_expr("status = 'active'").unwrap();
        assert!(expr.evaluate(&text_row(&[("status", "active")])));
        assert!(!expr.evaluate(&text_row(&[("status", "inactive")])));
    }

    #[test]
    fn numeric_ordering() {
        let gt = parse_filter_expr("amount > 100").unwrap();
        assert!(gt.evaluate(&text_row(&[("amount", "150")])));
        assert!(!gt.evaluate(&text_row(&[("amount", "50")])));
        assert!(!gt.evaluate(&text_row(&[("amount", "100.0")])));

        let ge = parse_filter_expr("amount >= 100").unwrap();
        assert!(ge.evaluate(&text_row(&[("amount", "100.0")])));
    }

    #[test]
    fn string_ordering_falls_back_lexically() {
        let expr = parse_filter_expr("name > 'b'").unwrap();
        assert!(expr.evaluate(&text_row(&[("name", "c")])));
        assert!(!expr.evaluate(&text_row(&[("name", "a")])));
    }

    #[test]
    fn neq_is_numeric_when_both_parse() {
        let expr = parse_filter_expr("amount != 100").unwrap();
        assert!(!expr.evaluate(&text_row(&[("amount", "100.0")])));
        assert!(expr.evaluate(&text_row(&[("amount", "101")])));
    }

    // ── Three-valued NULL semantics ──────────────────────────────────────────

    #[test]
    fn comparison_with_null_or_missing_column_is_unknown() {
        let expr = parse_filter_expr("status = 'active'").unwrap();
        assert_eq!(expr.eval(&row_cv(&[("status", ColVal::Null)])), None);
        assert_eq!(expr.eval(&text_row(&[])), None);
        assert_eq!(expr.eval(&row_cv(&[("status", ColVal::Unchanged)])), None);
        assert!(!expr.evaluate(&row_cv(&[("status", ColVal::Null)])));
    }

    #[test]
    fn null_literal_comparison_is_never_true() {
        let eq = parse_filter_expr("col = NULL").unwrap();
        let neq = parse_filter_expr("col != NULL").unwrap();
        for r in [
            text_row(&[]),
            text_row(&[("col", "x")]),
            row_cv(&[("col", ColVal::Null)]),
        ] {
            assert_eq!(eq.eval(&r), None, "col = NULL must be unknown for {r:?}");
            assert_eq!(neq.eval(&r), None, "col != NULL must be unknown for {r:?}");
            assert!(!eq.evaluate(&r));
            assert!(!neq.evaluate(&r));
        }
    }

    #[test]
    fn is_null_matches_null_and_missing() {
        let expr = parse_filter_expr("deleted_at IS NULL").unwrap();
        assert!(expr.evaluate(&text_row(&[])));
        assert!(expr.evaluate(&row_cv(&[("deleted_at", ColVal::Null)])));
        assert!(expr.evaluate(&row_cv(&[("deleted_at", ColVal::Unchanged)])));
        assert!(!expr.evaluate(&text_row(&[("deleted_at", "2026-01-01")])));
    }

    #[test]
    fn is_not_null_requires_a_value() {
        let expr = parse_filter_expr("status IS NOT NULL").unwrap();
        assert!(expr.evaluate(&text_row(&[("status", "active")])));
        assert!(!expr.evaluate(&text_row(&[])));
        assert!(!expr.evaluate(&row_cv(&[("status", ColVal::Null)])));
    }

    #[test]
    fn and_propagates_null_unless_false() {
        let expr = parse_filter_expr("a = 1 AND b = 2").unwrap();
        assert_eq!(expr.eval(&text_row(&[("a", "1"), ("b", "3")])), Some(false));
        assert_eq!(
            expr.eval(&text_row(&[("a", "1")])),
            None,
            "unknown AND true is unknown"
        );
        assert_eq!(
            expr.eval(&text_row(&[("a", "2")])),
            Some(false),
            "false AND unknown is false"
        );
    }

    #[test]
    fn or_propagates_null_unless_true() {
        let expr = parse_filter_expr("a = 1 OR b = 2").unwrap();
        assert_eq!(expr.eval(&text_row(&[("a", "1")])), Some(true));
        assert_eq!(
            expr.eval(&text_row(&[("a", "2")])),
            None,
            "false OR unknown is unknown"
        );
        assert_eq!(expr.eval(&text_row(&[("a", "2"), ("b", "3")])), Some(false));
    }

    #[test]
    fn precedence_behavior_or_and() {
        let expr = parse_filter_expr("a = 1 OR b = 2 AND c = 3").unwrap();
        assert!(expr.evaluate(&text_row(&[("a", "1"), ("b", "9"), ("c", "9")])));
        assert!(expr.evaluate(&text_row(&[("a", "9"), ("b", "2"), ("c", "3")])));
        assert!(!expr.evaluate(&text_row(&[("a", "9"), ("b", "2"), ("c", "4")])));
    }

    // ── Parser errors ────────────────────────────────────────────────────────

    #[test]
    fn empty_expression_is_an_error() {
        assert!(parse_filter_expr("").is_err());
        assert!(parse_filter_expr("   ").is_err());
    }

    #[test]
    fn trailing_tokens_are_rejected() {
        assert!(parse_filter_expr("a = 1 )").is_err());
        assert!(parse_filter_expr("a = 1 AND").is_err());
        assert!(parse_filter_expr("a = 1 b = 2").is_err());
    }

    #[test]
    fn unclosed_parenthesis_is_an_error() {
        assert!(parse_filter_expr("(a = 1").is_err());
    }

    #[test]
    fn missing_value_or_operator_is_an_error() {
        assert!(parse_filter_expr("a =").is_err());
        assert!(parse_filter_expr("a 1").is_err());
        assert!(parse_filter_expr("a").is_err());
        assert!(parse_filter_expr("a IS").is_err());
    }

    #[test]
    fn unknown_character_is_an_error() {
        assert!(parse_filter_expr("a @ 1").is_err());
        assert!(parse_filter_expr("a = 'unterminated").is_err());
    }

    // ── parse_where_arg / table prefix ───────────────────────────────────────

    #[test]
    fn where_arg_without_prefix_is_global() {
        let (key, expr) = parse_where_arg("amount > 100").unwrap();
        assert_eq!(key, None);
        assert_eq!(expr, cmp("amount", Operator::Gt, Value::Num(100.0)));
    }

    #[test]
    fn where_arg_with_schema_qualified_prefix() {
        let (key, expr) = parse_where_arg("public.orders:status = 'active'").unwrap();
        assert_eq!(key, Some(("public".to_string(), "orders".to_string())));
        assert_eq!(
            expr,
            cmp("status", Operator::Eq, Value::Str("active".to_string()))
        );
    }

    #[test]
    fn where_arg_empty_expression_is_an_error() {
        assert!(parse_where_arg("public.orders:  ").is_err());
    }

    // ── RowFilter::from_sources ──────────────────────────────────────────────

    fn insert_event(schema: &str, table: &str, pairs: &[(&str, &str)]) -> WalEvent {
        WalEvent::Insert {
            rel_id: 1,
            schema: schema.to_string(),
            table: table.to_string(),
            new: text_row(pairs),
        }
    }

    #[test]
    fn from_sources_parses_config_then_cli() {
        let rf = RowFilter::from_sources(
            &["status = 'active'".to_string()],
            &["public.orders:amount > 100".to_string()],
        )
        .unwrap();
        assert_eq!(rf.filters.len(), 2);

        let orders_ok = insert_event(
            "public",
            "orders",
            &[("status", "active"), ("amount", "150")],
        );
        let orders_low = insert_event(
            "public",
            "orders",
            &[("status", "active"), ("amount", "50")],
        );
        let other_ok = insert_event("public", "users", &[("status", "active")]);
        let other_bad = insert_event("public", "users", &[("status", "inactive")]);
        assert!(rf.should_forward(&orders_ok));
        assert!(
            !rf.should_forward(&orders_low),
            "specific rule must gate its table"
        );
        assert!(
            rf.should_forward(&other_ok),
            "specific rule must not leak onto other tables"
        );
        assert!(
            !rf.should_forward(&other_bad),
            "global rule must apply to every table"
        );
    }

    #[test]
    fn from_sources_error_propagates() {
        assert!(RowFilter::from_sources(&["public.orders:".to_string()], &[]).is_err());
        assert!(RowFilter::from_sources(&[], &["a = 1 AND".to_string()]).is_err());
    }

    #[test]
    fn no_filters_forwards_everything() {
        let rf = RowFilter::new();
        assert!(rf.should_forward(&insert_event("public", "t", &[("x", "1")])));
    }
}
