//! Compile a Python expression into DuckDB SQL.
//!
//! The point is speed. `code.python` is easy to write but pulls the whole
//! upstream result into memory as JSON and interprets it row by row, which is
//! the slowest shape a transform can have. `xf.addcol` runs at full vectorized
//! speed but asks the author to write SQL.
//!
//! This module closes that gap: the author writes a Python expression, and it
//! is translated to SQL once, at plan time. Nothing Python-shaped survives into
//! the run - DuckDB executes ordinary vectorized SQL, so a compiled expression
//! costs exactly what the equivalent hand-written SQL costs.
//!
//! Deliberately expression-only. No statements, loops, imports, attribute
//! access beyond a known method table, comprehensions or lambdas. That is not a
//! temporary limitation to be widened later without thought: every construct
//! accepted here must have an exact, vectorizable SQL equivalent, and anything
//! that does not is rejected by name so the author is never silently given
//! something slower or subtly different from what they wrote.
//!
//! Bare names are column references. Literals are inlined and escaped.

use std::fmt::Write as _;

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Name(String),
    Num(String),
    Str(String),
    FString(String),
    Op(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    End,
}

#[derive(Debug, Clone)]
enum Ast {
    Col(String),
    Num(String),
    Str(String),
    /// f-string: literal chunks interleaved with embedded expressions.
    FStr(Vec<FPart>),
    Bool(bool),
    Null,
    Unary(String, Box<Ast>),
    Bin(String, Box<Ast>, Box<Ast>),
    /// `a if cond else b`
    Cond(Box<Ast>, Box<Ast>, Box<Ast>),
    Call(String, Vec<Ast>),
    /// `receiver.method(args)`
    Method(Box<Ast>, String, Vec<Ast>),
    /// `x in (a, b)` / `x not in (...)`
    In(Box<Ast>, Vec<Ast>, bool),
    /// `x is None` / `x is not None`
    IsNull(Box<Ast>, bool),
    #[allow(dead_code)]
    Tuple(Vec<Ast>),
}

#[derive(Debug, Clone)]
enum FPart {
    Lit(String),
    Expr(Ast),
}

pub struct CompileError {
    pub message: String,
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

fn err<T>(msg: impl Into<String>) -> Result<T, CompileError> {
    Err(CompileError { message: msg.into() })
}

// ---------------------------------------------------------------- tokenizer

fn tokenize(src: &str) -> Result<Vec<Tok>, CompileError> {
    let b: Vec<char> = src.chars().collect();
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '#' {
            return err("comments are not supported in a Duckle Python expression");
        }
        // f-string
        if (c == 'f' || c == 'F') && i + 1 < b.len() && (b[i + 1] == '"' || b[i + 1] == '\'') {
            let quote = b[i + 1];
            let mut j = i + 2;
            let mut s = String::new();
            let mut closed = false;
            while j < b.len() {
                if b[j] == '\\' && j + 1 < b.len() {
                    s.push(b[j + 1]);
                    j += 2;
                    continue;
                }
                if b[j] == quote {
                    closed = true;
                    j += 1;
                    break;
                }
                s.push(b[j]);
                j += 1;
            }
            if !closed {
                return err("unterminated f-string");
            }
            out.push(Tok::FString(s));
            i = j;
            continue;
        }
        if c == '"' || c == '\'' {
            let quote = c;
            let mut j = i + 1;
            let mut s = String::new();
            let mut closed = false;
            while j < b.len() {
                if b[j] == '\\' && j + 1 < b.len() {
                    s.push(b[j + 1]);
                    j += 2;
                    continue;
                }
                if b[j] == quote {
                    closed = true;
                    j += 1;
                    break;
                }
                s.push(b[j]);
                j += 1;
            }
            if !closed {
                return err("unterminated string literal");
            }
            out.push(Tok::Str(s));
            i = j;
            continue;
        }
        if c.is_ascii_digit() || (c == '.' && i + 1 < b.len() && b[i + 1].is_ascii_digit()) {
            let mut j = i;
            let mut s = String::new();
            while j < b.len() && (b[j].is_ascii_digit() || b[j] == '.' || b[j] == '_') {
                if b[j] != '_' {
                    s.push(b[j]);
                }
                j += 1;
            }
            out.push(Tok::Num(s));
            i = j;
            continue;
        }
        if c.is_alphabetic() || c == '_' {
            let mut j = i;
            let mut s = String::new();
            while j < b.len() && (b[j].is_alphanumeric() || b[j] == '_') {
                s.push(b[j]);
                j += 1;
            }
            out.push(Tok::Name(s));
            i = j;
            continue;
        }
        // multi-char operators first
        let two: String = b[i..(i + 2).min(b.len())].iter().collect();
        if ["==", "!=", "<=", ">=", "//", "**"].contains(&two.as_str()) {
            out.push(Tok::Op(two));
            i += 2;
            continue;
        }
        match c {
            '(' => { out.push(Tok::LParen); i += 1; }
            ')' => { out.push(Tok::RParen); i += 1; }
            '[' => { out.push(Tok::LBracket); i += 1; }
            ']' => { out.push(Tok::RBracket); i += 1; }
            ',' => { out.push(Tok::Comma); i += 1; }
            '.' => { out.push(Tok::Dot); i += 1; }
            '+' | '-' | '*' | '/' | '%' | '<' | '>' => {
                out.push(Tok::Op(c.to_string()));
                i += 1;
            }
            '=' => return err("'=' is assignment; use '==' to compare"),
            '&' | '|' | '^' | '~' => {
                return err(format!(
                    "bitwise operator '{}' is not supported; use 'and' / 'or' / 'not'",
                    c
                ))
            }
            ':' => {
                return err(
                    "':' is not valid here; lambda, dict literals and slices are not supported",
                )
            }
            ';' => return err("';' is not valid in an expression"),
            other => return err(format!("unexpected character '{}'", other)),
        }
    }
    out.push(Tok::End);
    Ok(out)
}

// ------------------------------------------------------------------- parser

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        self.toks.get(self.pos).unwrap_or(&Tok::End)
    }
    fn next(&mut self) -> Tok {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }
    fn eat_op(&mut self, op: &str) -> bool {
        if matches!(self.peek(), Tok::Op(o) if o == op) {
            self.pos += 1;
            return true;
        }
        false
    }
    fn eat_name(&mut self, kw: &str) -> bool {
        if matches!(self.peek(), Tok::Name(n) if n == kw) {
            self.pos += 1;
            return true;
        }
        false
    }
    fn expect(&mut self, t: Tok, what: &str) -> Result<(), CompileError> {
        if *self.peek() == t {
            self.pos += 1;
            Ok(())
        } else {
            err(format!("expected {}", what))
        }
    }

    /// Lowest precedence: the conditional expression `a if c else b`.
    fn parse_expr(&mut self) -> Result<Ast, CompileError> {
        let body = self.parse_or()?;
        if self.eat_name("if") {
            let cond = self.parse_or()?;
            if !self.eat_name("else") {
                return err("conditional expression needs an 'else' branch");
            }
            let other = self.parse_expr()?;
            return Ok(Ast::Cond(Box::new(cond), Box::new(body), Box::new(other)));
        }
        Ok(body)
    }

    fn parse_or(&mut self) -> Result<Ast, CompileError> {
        let mut lhs = self.parse_and()?;
        while self.eat_name("or") {
            let rhs = self.parse_and()?;
            lhs = Ast::Bin("OR".into(), Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_and(&mut self) -> Result<Ast, CompileError> {
        let mut lhs = self.parse_not()?;
        while self.eat_name("and") {
            let rhs = self.parse_not()?;
            lhs = Ast::Bin("AND".into(), Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn parse_not(&mut self) -> Result<Ast, CompileError> {
        if self.eat_name("not") {
            let inner = self.parse_not()?;
            return Ok(Ast::Unary("NOT".into(), Box::new(inner)));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<Ast, CompileError> {
        let lhs = self.parse_add()?;
        // `is None` / `is not None`
        if self.eat_name("is") {
            let negated = self.eat_name("not");
            if !self.eat_name("None") {
                return err("'is' is only supported as 'is None' or 'is not None'");
            }
            return Ok(Ast::IsNull(Box::new(lhs), negated));
        }
        if self.eat_name("in") {
            let items = self.parse_membership_list()?;
            return Ok(Ast::In(Box::new(lhs), items, false));
        }
        // `not in`
        if matches!(self.peek(), Tok::Name(n) if n == "not") {
            let save = self.pos;
            self.pos += 1;
            if self.eat_name("in") {
                let items = self.parse_membership_list()?;
                return Ok(Ast::In(Box::new(lhs), items, true));
            }
            self.pos = save;
        }
        for (py, sql) in [("==", "="), ("!=", "<>"), ("<=", "<="), (">=", ">="), ("<", "<"), (">", ">")] {
            if self.eat_op(py) {
                let rhs = self.parse_add()?;
                return Ok(Ast::Bin(sql.into(), Box::new(lhs), Box::new(rhs)));
            }
        }
        Ok(lhs)
    }

    fn parse_membership_list(&mut self) -> Result<Vec<Ast>, CompileError> {
        // Accept a parenthesised/bracketed literal collection.
        let close = match self.next() {
            Tok::LParen => Tok::RParen,
            Tok::LBracket => Tok::RBracket,
            _ => return err("'in' needs a list or tuple, e.g. x in ('a', 'b')"),
        };
        let mut items = Vec::new();
        if *self.peek() != close {
            loop {
                items.push(self.parse_expr()?);
                if *self.peek() == Tok::Comma {
                    self.pos += 1;
                    if *self.peek() == close {
                        break;
                    }
                    continue;
                }
                break;
            }
        }
        self.expect(close, "closing bracket for the 'in' list")?;
        if items.is_empty() {
            return err("'in' needs at least one value");
        }
        Ok(items)
    }

    fn parse_add(&mut self) -> Result<Ast, CompileError> {
        let mut lhs = self.parse_mul()?;
        loop {
            if self.eat_op("+") {
                let rhs = self.parse_mul()?;
                lhs = Ast::Bin("+".into(), Box::new(lhs), Box::new(rhs));
            } else if self.eat_op("-") {
                let rhs = self.parse_mul()?;
                lhs = Ast::Bin("-".into(), Box::new(lhs), Box::new(rhs));
            } else {
                return Ok(lhs);
            }
        }
    }

    fn parse_mul(&mut self) -> Result<Ast, CompileError> {
        let mut lhs = self.parse_unary()?;
        loop {
            let op = if self.eat_op("*") {
                "*"
            } else if self.eat_op("/") {
                "/"
            } else if self.eat_op("//") {
                "//"
            } else if self.eat_op("%") {
                "%"
            } else {
                return Ok(lhs);
            };
            let rhs = self.parse_unary()?;
            lhs = Ast::Bin(op.into(), Box::new(lhs), Box::new(rhs));
        }
    }

    fn parse_unary(&mut self) -> Result<Ast, CompileError> {
        if self.eat_op("-") {
            let inner = self.parse_unary()?;
            return Ok(Ast::Unary("-".into(), Box::new(inner)));
        }
        if self.eat_op("+") {
            return self.parse_unary();
        }
        self.parse_power()
    }

    fn parse_power(&mut self) -> Result<Ast, CompileError> {
        let base = self.parse_postfix()?;
        if self.eat_op("**") {
            let exp = self.parse_unary()?;
            return Ok(Ast::Bin("**".into(), Box::new(base), Box::new(exp)));
        }
        Ok(base)
    }

    fn parse_postfix(&mut self) -> Result<Ast, CompileError> {
        let mut node = self.parse_atom()?;
        loop {
            if *self.peek() == Tok::Dot {
                self.pos += 1;
                let name = match self.next() {
                    Tok::Name(n) => n,
                    _ => return err("expected a method name after '.'"),
                };
                if *self.peek() != Tok::LParen {
                    return err(format!(
                        "attribute access '.{}' is not supported; only method calls like .upper() are",
                        name
                    ));
                }
                self.pos += 1;
                let args = self.parse_args()?;
                node = Ast::Method(Box::new(node), name, args);
                continue;
            }
            if *self.peek() == Tok::LBracket {
                return err("indexing and slicing are not supported");
            }
            return Ok(node);
        }
    }

    fn parse_args(&mut self) -> Result<Vec<Ast>, CompileError> {
        let mut args = Vec::new();
        if *self.peek() == Tok::RParen {
            self.pos += 1;
            return Ok(args);
        }
        loop {
            args.push(self.parse_expr()?);
            if *self.peek() == Tok::Comma {
                self.pos += 1;
                continue;
            }
            break;
        }
        self.expect(Tok::RParen, "')'")?;
        Ok(args)
    }

    fn parse_atom(&mut self) -> Result<Ast, CompileError> {
        match self.next() {
            Tok::Num(n) => Ok(Ast::Num(n)),
            Tok::Str(s) => Ok(Ast::Str(s)),
            Tok::FString(s) => self.parse_fstring(&s),
            Tok::Name(n) => match n.as_str() {
                "True" => Ok(Ast::Bool(true)),
                "False" => Ok(Ast::Bool(false)),
                "None" => Ok(Ast::Null),
                "lambda" => err("lambda is not supported"),
                _ => {
                    if *self.peek() == Tok::LParen {
                        self.pos += 1;
                        let args = self.parse_args()?;
                        Ok(Ast::Call(n, args))
                    } else {
                        Ok(Ast::Col(n))
                    }
                }
            },
            Tok::LParen => {
                let first = self.parse_expr()?;
                if *self.peek() == Tok::Comma {
                    let mut items = vec![first];
                    while *self.peek() == Tok::Comma {
                        self.pos += 1;
                        if *self.peek() == Tok::RParen {
                            break;
                        }
                        items.push(self.parse_expr()?);
                    }
                    self.expect(Tok::RParen, "')'")?;
                    return Ok(Ast::Tuple(items));
                }
                self.expect(Tok::RParen, "')'")?;
                Ok(first)
            }
            Tok::LBracket => err("list literals are only supported on the right of 'in'"),
            Tok::End => err("expression ended unexpectedly"),
            other => err(format!("unexpected token {:?}", other)),
        }
    }

    /// Parse an f-string body into literal and expression parts.
    fn parse_fstring(&mut self, body: &str) -> Result<Ast, CompileError> {
        let chars: Vec<char> = body.chars().collect();
        let mut parts = Vec::new();
        let mut lit = String::new();
        let mut i = 0usize;
        while i < chars.len() {
            if chars[i] == '{' {
                if i + 1 < chars.len() && chars[i + 1] == '{' {
                    lit.push('{');
                    i += 2;
                    continue;
                }
                if !lit.is_empty() {
                    parts.push(FPart::Lit(std::mem::take(&mut lit)));
                }
                let mut depth = 1;
                let mut inner = String::new();
                i += 1;
                while i < chars.len() && depth > 0 {
                    if chars[i] == '{' {
                        depth += 1;
                    } else if chars[i] == '}' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    inner.push(chars[i]);
                    i += 1;
                }
                if depth != 0 {
                    return err("unterminated '{' in f-string");
                }
                i += 1; // consume '}'
                if inner.contains(':') || inner.contains('!') {
                    return err("f-string format specifiers are not supported");
                }
                parts.push(FPart::Expr(compile_to_ast(&inner)?));
                continue;
            }
            if chars[i] == '}' {
                if i + 1 < chars.len() && chars[i + 1] == '}' {
                    lit.push('}');
                    i += 2;
                    continue;
                }
                return err("unmatched '}' in f-string");
            }
            lit.push(chars[i]);
            i += 1;
        }
        if !lit.is_empty() {
            parts.push(FPart::Lit(lit));
        }
        Ok(Ast::FStr(parts))
    }
}

fn compile_to_ast(src: &str) -> Result<Ast, CompileError> {
    if src.trim().is_empty() {
        return err("expression is empty");
    }
    let toks = tokenize(src)?;
    let mut p = Parser { toks, pos: 0 };
    let ast = p.parse_expr()?;
    if *p.peek() != Tok::End {
        return err("unexpected trailing input; this must be a single expression");
    }
    Ok(ast)
}

// ------------------------------------------------------------------ emitter

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn quote_col(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn emit(ast: &Ast, out: &mut String) -> Result<(), CompileError> {
    match ast {
        Ast::Col(name) => {
            out.push_str(&quote_col(name));
            Ok(())
        }
        Ast::Num(n) => {
            out.push_str(n);
            Ok(())
        }
        Ast::Str(s) => {
            out.push_str(&sql_str(s));
            Ok(())
        }
        Ast::Bool(b) => {
            out.push_str(if *b { "TRUE" } else { "FALSE" });
            Ok(())
        }
        Ast::Null => {
            out.push_str("NULL");
            Ok(())
        }
        Ast::FStr(parts) => {
            // concat() rather than ||: concat treats NULL as empty, which is
            // closer to what an f-string does than SQL's NULL-propagating ||.
            out.push_str("concat(");
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                match p {
                    FPart::Lit(s) => out.push_str(&sql_str(s)),
                    FPart::Expr(e) => emit(e, out)?,
                }
            }
            out.push(')');
            Ok(())
        }
        Ast::Unary(op, inner) => {
            out.push('(');
            out.push_str(op);
            out.push(' ');
            emit(inner, out)?;
            out.push(')');
            Ok(())
        }
        Ast::Bin(op, a, b) => {
            match op.as_str() {
                "**" => {
                    out.push_str("power(");
                    emit(a, out)?;
                    out.push_str(", ");
                    emit(b, out)?;
                    out.push(')');
                }
                "//" => {
                    // Python floor-divides; SQL '/' on integers already
                    // truncates toward zero, which differs for negatives, so
                    // be explicit.
                    out.push_str("floor(");
                    emit(a, out)?;
                    out.push_str(" / ");
                    emit(b, out)?;
                    out.push(')');
                }
                _ => {
                    out.push('(');
                    emit(a, out)?;
                    let _ = write!(out, " {} ", op);
                    emit(b, out)?;
                    out.push(')');
                }
            }
            Ok(())
        }
        Ast::Cond(cond, then, other) => {
            out.push_str("CASE WHEN ");
            emit(cond, out)?;
            out.push_str(" THEN ");
            emit(then, out)?;
            out.push_str(" ELSE ");
            emit(other, out)?;
            out.push_str(" END");
            Ok(())
        }
        Ast::IsNull(inner, negated) => {
            out.push('(');
            emit(inner, out)?;
            out.push_str(if *negated { " IS NOT NULL" } else { " IS NULL" });
            out.push(')');
            Ok(())
        }
        Ast::In(lhs, items, negated) => {
            out.push('(');
            emit(lhs, out)?;
            out.push_str(if *negated { " NOT IN (" } else { " IN (" });
            for (i, it) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                emit(it, out)?;
            }
            out.push_str("))");
            Ok(())
        }
        Ast::Tuple(_) => err("a bare tuple is only supported on the right of 'in'"),
        Ast::Call(name, args) => emit_call(name, args, out),
        Ast::Method(recv, name, args) => emit_method(recv, name, args, out),
    }
}

/// Builtins with an exact vectorized SQL equivalent.
fn emit_call(name: &str, args: &[Ast], out: &mut String) -> Result<(), CompileError> {
    let n = args.len();
    let simple = |sql_fn: &str, out: &mut String, args: &[Ast]| -> Result<(), CompileError> {
        out.push_str(sql_fn);
        out.push('(');
        for (i, a) in args.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            emit(a, out)?;
        }
        out.push(')');
        Ok(())
    };
    let cast = |ty: &str, out: &mut String, args: &[Ast]| -> Result<(), CompileError> {
        out.push_str("TRY_CAST(");
        emit(&args[0], out)?;
        let _ = write!(out, " AS {})", ty);
        Ok(())
    };
    match (name, n) {
        ("len", 1) => simple("length", out, args),
        ("abs", 1) => simple("abs", out, args),
        ("round", 1) | ("round", 2) => simple("round", out, args),
        ("int", 1) => cast("BIGINT", out, args),
        ("float", 1) => cast("DOUBLE", out, args),
        ("str", 1) => cast("VARCHAR", out, args),
        ("bool", 1) => cast("BOOLEAN", out, args),
        ("min", _) if n >= 2 => simple("least", out, args),
        ("max", _) if n >= 2 => simple("greatest", out, args),
        ("sum", _) | ("any", _) | ("all", _) | ("sorted", _) | ("list", _) | ("set", _) => err(
            format!("'{}' works on a collection, which a per-row expression does not have; use an Aggregate node instead", name),
        ),
        ("open", _) | ("eval", _) | ("exec", _) | ("__import__", _) | ("input", _) => {
            err(format!("'{}' is not allowed", name))
        }
        _ => err(format!(
            "unsupported function '{}' with {} argument(s)",
            name, n
        )),
    }
}

/// String / value methods with an exact SQL equivalent.
fn emit_method(recv: &Ast, name: &str, args: &[Ast], out: &mut String) -> Result<(), CompileError> {
    let n = args.len();
    let call = |sql_fn: &str, out: &mut String| -> Result<(), CompileError> {
        out.push_str(sql_fn);
        out.push('(');
        emit(recv, out)?;
        for a in args {
            out.push_str(", ");
            emit(a, out)?;
        }
        out.push(')');
        Ok(())
    };
    match (name, n) {
        ("upper", 0) => call("upper", out),
        ("lower", 0) => call("lower", out),
        ("strip", 0) => call("trim", out),
        ("lstrip", 0) => call("ltrim", out),
        ("rstrip", 0) => call("rtrim", out),
        ("strip", 1) => call("trim", out),
        ("title", 0) => call("initcap", out),
        ("replace", 2) => call("replace", out),
        ("startswith", 1) => call("starts_with", out),
        ("endswith", 1) => call("ends_with", out),
        ("zfill", 1) => {
            // lpad with '0' to the requested width.
            out.push_str("lpad(");
            emit(recv, out)?;
            out.push_str(", ");
            emit(&args[0], out)?;
            out.push_str(", '0')");
            Ok(())
        }
        ("split", 1) => call("string_split", out),
        ("format", _) => err("str.format is not supported; use an f-string"),
        _ => err(format!(
            "unsupported method '.{}()' with {} argument(s)",
            name, n
        )),
    }
}

/// Compile a Python expression to a DuckDB SQL expression.
///
/// Returns the SQL text on success, or an error naming the construct that
/// could not be translated. It never falls back to something slower: an
/// expression either becomes vectorized SQL or is rejected.
pub fn compile(src: &str) -> Result<String, CompileError> {
    let ast = compile_to_ast(src)?;
    let mut out = String::new();
    emit(&ast, &mut out)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::compile;

    fn ok(src: &str) -> String {
        compile(src).unwrap_or_else(|e| panic!("compile({src:?}) failed: {e}"))
    }
    fn bad(src: &str) -> String {
        match compile(src) {
            Ok(sql) => panic!("compile({src:?}) unexpectedly succeeded: {sql}"),
            Err(e) => e.message,
        }
    }

    #[test]
    fn arithmetic_and_columns() {
        assert_eq!(ok("amount * 1.2"), r#"("amount" * 1.2)"#);
        assert_eq!(ok("a + b - c"), r#"(("a" + "b") - "c")"#);
        assert_eq!(ok("qty // 3"), r#"floor("qty" / 3)"#);
        assert_eq!(ok("x ** 2"), r#"power("x", 2)"#);
        assert_eq!(ok("-x"), r#"(- "x")"#);
    }

    #[test]
    fn conditional_becomes_case_when() {
        assert_eq!(
            ok("amount * 1.2 if region == 'EU' else amount"),
            r#"CASE WHEN ("region" = 'EU') THEN ("amount" * 1.2) ELSE "amount" END"#
        );
    }

    #[test]
    fn null_checks_and_membership() {
        assert_eq!(ok("email is None"), r#"("email" IS NULL)"#);
        assert_eq!(ok("email is not None"), r#"("email" IS NOT NULL)"#);
        assert_eq!(ok("region in ('EU', 'UK')"), r#"("region" IN ('EU', 'UK'))"#);
        assert_eq!(ok("region not in ('EU',)"), r#"("region" NOT IN ('EU'))"#);
    }

    #[test]
    fn boolean_logic() {
        assert_eq!(
            ok("a > 1 and not b"),
            r#"(("a" > 1) AND (NOT "b"))"#
        );
        assert_eq!(ok("a or b"), r#"("a" OR "b")"#);
    }

    #[test]
    fn string_methods_map_to_duckdb() {
        assert_eq!(ok("name.upper()"), r#"upper("name")"#);
        assert_eq!(ok("name.strip().lower()"), r#"lower(trim("name"))"#);
        assert_eq!(ok("sku.replace('-', '')"), r#"replace("sku", '-', '')"#);
        assert_eq!(ok("code.startswith('X')"), r#"starts_with("code", 'X')"#);
        assert_eq!(ok("id.zfill(8)"), r#"lpad("id", 8, '0')"#);
    }

    #[test]
    fn builtins_map_to_duckdb() {
        assert_eq!(ok("len(name)"), r#"length("name")"#);
        assert_eq!(ok("round(amount, 2)"), r#"round("amount", 2)"#);
        assert_eq!(ok("int(qty)"), r#"TRY_CAST("qty" AS BIGINT)"#);
        assert_eq!(ok("max(a, b)"), r#"greatest("a", "b")"#);
    }

    #[test]
    fn fstring_becomes_concat() {
        assert_eq!(
            ok("f'{first} {last}'"),
            r#"concat("first", ' ', "last")"#
        );
        assert_eq!(ok("f'ID-{id}'"), r#"concat('ID-', "id")"#);
    }

    #[test]
    fn string_literals_are_escaped() {
        // A Python double-quoted literal holding an apostrophe must come out
        // as a correctly doubled SQL literal.
        assert_eq!(ok(r#"x == "O'Brien""#), r#"("x" = 'O''Brien')"#);
        // A quote inside a literal cannot break out of the emitted SQL.
        let sql = ok(r#"x == "a' OR 1=1 --""#);
        assert_eq!(sql, r#"("x" = 'a'' OR 1=1 --')"#);
        // Nor via an f-string's literal chunks.
        assert_eq!(ok(r#"f"it's {x}""#), r#"concat('it''s ', "x")"#);
    }

    #[test]
    fn column_names_are_quoted_so_keywords_and_spaces_survive() {
        assert_eq!(ok("select + 1"), r#"("select" + 1)"#);
    }

    #[test]
    fn dangerous_and_unmappable_constructs_are_rejected_by_name() {
        assert!(bad("__import__('os')").contains("not allowed"));
        assert!(bad("eval('1')").contains("not allowed"));
        assert!(bad("open('/etc/passwd')").contains("not allowed"));
        assert!(bad("[x for x in y]").contains("only supported on the right of 'in'"));
        assert!(bad("lambda x: x").contains("lambda"));
        assert!(bad("a.b").contains("attribute access"));
        assert!(bad("x[0]").contains("indexing"));
        assert!(bad("a & b").contains("bitwise"));
        assert!(bad("x = 1").contains("assignment"));
        assert!(bad("sum(x)").contains("Aggregate node"));
        assert!(bad("weird_fn(x)").contains("unsupported function"));
        assert!(bad("name.encode()").contains("unsupported method"));
        assert!(bad("a if b").contains("else"));
        assert!(bad("").contains("empty"));
        assert!(bad("1 2").contains("single expression"));
    }
}
