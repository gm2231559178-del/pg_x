use anyhow::{bail, Context, Result};

use crate::replication::event::{ColVal, Row, WalEvent};

#[derive(Debug, Clone)]
pub(crate) enum FilterExpr {
    Eq(String, String),
    Neq(String, String),
    Gt(String, f64),
    Lt(String, f64),
    Ge(String, f64),
    Le(String, f64),
    IsNull(String),
    IsNotNull(String),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
}

impl FilterExpr {
    pub(crate) fn evaluate(&self, row: &Row) -> bool {
        match self {
            FilterExpr::Eq(col, val) => row.get(col).is_some_and(|cv| match cv {
                ColVal::Text(s) => s == val,
                _ => false,
            }),
            FilterExpr::Neq(col, val) => !row.get(col).is_some_and(|cv| match cv {
                ColVal::Text(s) => s == val,
                _ => false,
            }),
            FilterExpr::Gt(col, val) => cmp_numeric(row, col, |a, b| a > b, *val),
            FilterExpr::Lt(col, val) => cmp_numeric(row, col, |a, b| a < b, *val),
            FilterExpr::Ge(col, val) => cmp_numeric(row, col, |a, b| a >= b, *val),
            FilterExpr::Le(col, val) => cmp_numeric(row, col, |a, b| a <= b, *val),
            FilterExpr::IsNull(col) => row.get(col).is_some_and(|cv| matches!(cv, ColVal::Null)),
            FilterExpr::IsNotNull(col) => {
                !row.get(col).is_some_and(|cv| matches!(cv, ColVal::Null))
            }
            FilterExpr::And(a, b) => a.evaluate(row) && b.evaluate(row),
            FilterExpr::Or(a, b) => a.evaluate(row) || b.evaluate(row),
        }
    }
}

fn cmp_numeric(row: &Row, col: &str, cmp: fn(f64, f64) -> bool, rhs: f64) -> bool {
    row.get(col)
        .and_then(|cv| match cv {
            ColVal::Text(s) => s.parse::<f64>().ok(),
            _ => None,
        })
        .is_some_and(|lhs| cmp(lhs, rhs))
}

struct Parser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().peekable(),
        }
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.chars.peek() {
            if c.is_ascii_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }
    }

    fn expect_word(&mut self) -> Result<String> {
        self.skip_ws();
        let mut s = String::new();
        while let Some(c) = self.chars.peek() {
            if c.is_alphanumeric() || *c == '_' {
                s.push(self.chars.next().unwrap());
            } else {
                break;
            }
        }
        if s.is_empty() {
            bail!("expected identifier");
        }
        Ok(s)
    }

    fn expect_string_literal(&mut self) -> Result<String> {
        self.skip_ws();
        match self.chars.next() {
            Some('\'') => {
                let mut s = String::new();
                loop {
                    match self.chars.next() {
                        Some('\'') => {
                            if self.chars.peek() == Some(&'\'') {
                                self.chars.next();
                                s.push('\'');
                            } else {
                                return Ok(s);
                            }
                        }
                        Some(c) => s.push(c),
                        None => bail!("unterminated string literal"),
                    }
                }
            }
            Some(c) => bail!("expected ' to start string literal, got '{c}'"),
            None => bail!("expected string literal"),
        }
    }

    fn expect_number(&mut self) -> Result<f64> {
        self.skip_ws();
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
        if s.is_empty() || s == "-" {
            bail!("expected number, got '{s}'");
        }
        s.parse::<f64>()
            .with_context(|| format!("invalid number literal: '{s}'"))
    }

    fn parse_literal(&mut self) -> Result<FilterExpr> {
        self.skip_ws();
        if self.chars.peek() == Some(&'\'') {
            self.expect_string_literal()?;
            bail!("unexpected string literal without comparison")
        } else {
            let saved = self.chars.clone();
            match self.expect_number() {
                Ok(_) => {
                    bail!("unexpected number literal without comparison")
                }
                _ => {
                    self.chars = saved;
                    let ident = self.expect_word()?;
                    self.skip_ws();
                    let upper: String = self
                        .chars
                        .clone()
                        .take(2)
                        .collect::<String>()
                        .to_uppercase();
                    if upper == "IS" {
                        self.chars.next();
                        self.chars.next();
                        self.skip_ws();
                        let neg = {
                            let next: String = self
                                .chars
                                .clone()
                                .take(3)
                                .collect::<String>()
                                .to_uppercase();
                            if next == "NOT" {
                                self.chars.next();
                                self.chars.next();
                                self.chars.next();
                                true
                            } else {
                                false
                            }
                        };
                        self.skip_ws();
                        let null = self.expect_word()?;
                        if null.to_uppercase() != "NULL" {
                            bail!("expected NULL after IS{}", if neg { " NOT" } else { "" });
                        }
                        if neg {
                            Ok(FilterExpr::IsNotNull(ident))
                        } else {
                            Ok(FilterExpr::IsNull(ident))
                        }
                    } else {
                        Err(anyhow::anyhow!(
                            "expected comparison operator after identifier '{ident}'"
                        ))
                    }
                }
            }
        }
    }

    fn parse_comparison(&mut self) -> Result<FilterExpr> {
        self.skip_ws();

        let saved = self.chars.clone();
        let ident = self.expect_word()?;
        self.skip_ws();

        let op = self.parse_operator();
        if let Some(op) = op {
            self.skip_ws();
            if self.chars.peek() == Some(&'\'') {
                let val = self.expect_string_literal()?;
                return Ok(match op {
                    "=" => FilterExpr::Eq(ident, val),
                    "!=" | "<>" => FilterExpr::Neq(ident, val),
                    _ => bail!("string comparison does not support operator '{op}'"),
                });
            } else {
                let n = self.expect_number()?;
                return Ok(match op {
                    "=" => FilterExpr::Eq(ident, n.to_string()),
                    "!=" | "<>" => FilterExpr::Neq(ident, n.to_string()),
                    ">" => FilterExpr::Gt(ident, n),
                    "<" => FilterExpr::Lt(ident, n),
                    ">=" => FilterExpr::Ge(ident, n),
                    "<=" => FilterExpr::Le(ident, n),
                    _ => bail!("unknown operator '{op}'"),
                });
            }
        }

        self.chars = saved;
        self.parse_literal()
    }

    fn parse_operator(&mut self) -> Option<&'static str> {
        self.skip_ws();
        let mut two = String::new();
        two.push(*self.chars.peek()?);
        let c2 = {
            let mut it = self.chars.clone();
            it.next();
            it.peek().copied()
        };
        if let Some(c2) = c2 {
            two.push(c2);
        }
        match two.as_str() {
            ">=" => {
                self.chars.next();
                self.chars.next();
                Some(">=")
            }
            "<=" => {
                self.chars.next();
                self.chars.next();
                Some("<=")
            }
            "<>" => {
                self.chars.next();
                self.chars.next();
                Some("<>")
            }
            "!=" => {
                self.chars.next();
                self.chars.next();
                Some("!=")
            }
            _ => {
                let one = self.chars.peek().copied()?;
                match one {
                    '=' => {
                        self.chars.next();
                        Some("=")
                    }
                    '>' => {
                        self.chars.next();
                        Some(">")
                    }
                    '<' => {
                        self.chars.next();
                        Some("<")
                    }
                    _ => None,
                }
            }
        }
    }

    fn parse_expression(&mut self) -> Result<FilterExpr> {
        let mut left = self.parse_comparison()?;
        loop {
            self.skip_ws();
            let peek: String = self
                .chars
                .clone()
                .take(4)
                .collect::<String>()
                .to_uppercase();
            if peek.starts_with("AND") {
                self.chars.next();
                self.chars.next();
                self.chars.next();
                let right = self.parse_comparison()?;
                left = FilterExpr::And(Box::new(left), Box::new(right));
            } else if peek.starts_with("OR") {
                self.chars.next();
                self.chars.next();
                let right = self.parse_comparison()?;
                left = FilterExpr::Or(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }
}

pub(crate) fn parse_filter_expr(input: &str) -> Result<FilterExpr> {
    let mut parser = Parser::new(input);
    let expr = parser.parse_expression()?;
    parser.skip_ws();
    if parser.chars.peek().is_some() {
        bail!(
            "trailing characters after filter expression: '{}'",
            parser.chars.collect::<String>()
        );
    }
    Ok(expr)
}

pub(crate) type TableKey = Option<(String, String)>;

#[derive(Debug, Clone)]
pub(crate) struct RowFilter {
    filters: Vec<(Option<(String, String)>, FilterExpr)>,
}

impl RowFilter {
    pub(crate) fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, table_key: Option<(String, String)>, expr: FilterExpr) {
        self.filters.push((table_key, expr));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub(crate) fn from_cli_args(filters: &[String]) -> Result<Self> {
        let mut rf = RowFilter::new();
        for arg in filters {
            let (table_key, expr) = parse_where_arg(arg)?;
            rf.add(table_key, expr);
        }
        Ok(rf)
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
    let colon_pos = arg.find(':');
    if colon_pos == Some(0) {
        bail!(
            "filter expression starts with ':' but no table prefix before it — \
               use 'schema.table:expression' or omit the colon for global filters"
        );
    }
    let table_key = match colon_pos {
        Some(pos) => {
            let prefix = &arg[..pos];
            if let Some(dot) = prefix.find('.') {
                Some((prefix[..dot].to_string(), prefix[dot + 1..].to_string()))
            } else {
                return Err(anyhow::anyhow!(
                    "filter prefix must be schema-qualified (e.g. public.orders:expression), \
                     got '{prefix}' — use 'public.{prefix}:expression' or omit the prefix for global filters"
                ));
            }
        }
        _ => None,
    };
    let expr_str = match colon_pos {
        Some(pos) => arg[pos + 1..].trim(),
        None => arg.trim(),
    };
    if expr_str.is_empty() {
        bail!("empty filter expression");
    }
    let expr = parse_filter_expr(expr_str)?;
    Ok((table_key, expr))
}
