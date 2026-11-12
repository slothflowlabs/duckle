//! src.generate: rows of believable fake data, compiled to SQL.
//!
//! Most generators are a row loop. This one is a `SELECT` over `range(n)`, for three
//! reasons that were measured rather than assumed: a preview of a trillion-row node stops at
//! its limit in milliseconds, ten million rows take seconds because DuckDB does the work,
//! and nothing has to be written out and read back, so the node runs on either execution
//! path like any other source.
//!
//! What only a faker library knows, the names and addresses of fourteen locales, it supplies
//! at compile time as a POOL of values, and a row picks from the pool with
//! `hash(xor(i, salt))`.
//! That has consequences worth stating:
//!
//! - The same seed gives the same rows, always and on every machine, which is what makes a
//!   test written against generated data repeatable. Neither `random()` nor a row loop
//!   seeded per thread would give that.
//! - The salt comes from the seed and the column's NAME, not its position, so adding a
//!   column leaves the rows a test recorded alone.
//! - A pool is finite. Values that should barely repeat (an email, a phone number, a key)
//!   are composed from smaller parts instead of being drawn from one.
//! - `hash()` is not documented as stable across DuckDB versions, and `md5_number` is, but
//!   md5 costs twenty times as much per value and `range` runs on one thread.
//!   `the_rows_for_a_seed_are_pinned` fails if an upgrade ever moves them.
//!
//! The pools are drawn with a fixed seed of their own, so the seed a person sets changes the
//! picks and never the SQL's size. `fake` is pinned exactly in Cargo.toml for the same reason:
//! a new version can change its lists, which would move the rows.

use std::collections::{HashMap, HashSet};

use chrono::{NaiveDate, NaiveDateTime};
use fake::faker::{
    address::raw as address, barcode::raw as barcode, company::raw as company,
    creditcard::raw as creditcard, currency::raw as currency, filesystem::raw as filesystem,
    finance::raw as finance, internet::raw as internet, job::raw as job, lorem::raw as lorem,
    name::raw as name,
};
use fake::locales::{
    Data, AR_SA, CY_GB, DE_DE, EN, FA_IR, FR_FR, IT_IT, JA_JP, NL_NL, PT_BR, PT_PT, TR_TR, ZH_CN,
    ZH_TW,
};
use fake::faker::impls::address::CityNameGenFn;
use fake::Fake;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde_json::Value as JsonValue;

use super::{quote_ident, sql_escape};

/// How many values a column draws. Long enough that a name column looks like a population,
/// short enough that the SQL stays a size a person can scroll past.
const POOL: usize = 1000;

/// A generator whose values come from a pool, one to one.
const POOLED: &[&str] = &[
    "person.firstName",
    "person.lastName",
    "person.title",
    "address.city",
    "address.state",
    "address.stateAbbr",
    "address.country",
    "address.countryCode",
    "address.timezone",
    "company.name",
    "company.industry",
    "company.profession",
    "company.catchPhrase",
    "job.title",
    "internet.ipv4",
    "internet.ipv6",
    "internet.mac",
    "internet.userAgent",
    "finance.creditCard",
    "finance.currencyCode",
    "finance.currencyName",
    "finance.bic",
    "finance.isin",
    "text.word",
    "text.sentence",
    "text.paragraph",
    "file.name",
    "file.path",
    "file.extension",
    "file.mimeType",
    "book.isbn",
];

/// A generator computed in SQL, or composed from pools.
const COMPUTED: &[&str] = &[
    "sequence",
    "integer",
    "decimal",
    "boolean",
    "date",
    "timestamp",
    "uuid",
    "pick",
    "pattern",
    "constant",
    "person.fullName",
    "internet.email",
    "internet.username",
    "address.street",
    "address.zip",
    "address.latitude",
    "address.longitude",
    "phone.number",
    "phone.cell",
];

/// Every generator a column can name. The form's list is checked against this.
pub fn names() -> Vec<&'static str> {
    COMPUTED.iter().chain(POOLED).copied().collect()
}

/// Every locale a generator can speak, as the form offers them.
pub fn locales() -> Vec<&'static str> {
    LOCALES.iter().map(|(name, _)| *name).collect()
}

/// What a locale contributes beyond its pools.
#[derive(Clone, Copy)]
struct Locale {
    pool: fn(&str) -> Option<Vec<String>>,
    name_tpl: &'static str,
    phone: &'static [&'static str],
    cell: &'static [&'static str],
    zip: &'static [&'static str],
}

macro_rules! locale {
    ($l:ident) => {
        Locale {
            pool: |generator| drawn(generator, $l),
            name_tpl: $l::NAME_TPL,
            phone: $l::PHONE_NUMBER_FORMATS,
            cell: $l::PHONE_CELL_NUMBER_FORMATS,
            zip: $l::ADDRESS_ZIP_FORMATS,
        }
    };
}

const LOCALES: &[(&str, Locale)] = &[
    ("en", locale!(EN)),
    ("ar", locale!(AR_SA)),
    ("cy", locale!(CY_GB)),
    ("de", locale!(DE_DE)),
    ("fa", locale!(FA_IR)),
    ("fr", locale!(FR_FR)),
    ("it", locale!(IT_IT)),
    ("ja", locale!(JA_JP)),
    ("nl", locale!(NL_NL)),
    ("pt-BR", locale!(PT_BR)),
    ("pt-PT", locale!(PT_PT)),
    ("tr", locale!(TR_TR)),
    ("zh-CN", locale!(ZH_CN)),
    ("zh-TW", locale!(ZH_TW)),
];

/// A pool for one generator, duplicates dropped so every value is as likely as any other.
fn drawn<L: Data + Copy + CityNameGenFn>(generator: &str, l: L) -> Option<Vec<String>> {
    let mut rng = StdRng::seed_from_u64(salt("pool", generator));
    macro_rules! draw {
        ($faker:expr) => {
            (0..POOL)
                .map(|_| $faker.fake_with_rng::<String, _>(&mut rng))
                .collect::<Vec<String>>()
        };
    }
    let values = match generator {
        "person.firstName" => draw!(name::FirstName(l)),
        "person.lastName" => draw!(name::LastName(l)),
        "person.title" => draw!(name::Title(l)),
        "address.city" => draw!(address::CityName(l)),
        "address.streetName" => draw!(address::StreetName(l)),
        "address.state" => draw!(address::StateName(l)),
        "address.stateAbbr" => draw!(address::StateAbbr(l)),
        "address.country" => draw!(address::CountryName(l)),
        "address.countryCode" => draw!(address::CountryCode(l)),
        "address.timezone" => draw!(address::TimeZone(l)),
        "company.name" => draw!(company::CompanyName(l)),
        "company.industry" => draw!(company::Industry(l)),
        "company.profession" => draw!(company::Profession(l)),
        "company.catchPhrase" => draw!(company::CatchPhrase(l)),
        "job.title" => draw!(job::Title(l)),
        "internet.ipv4" => draw!(internet::IPv4(l)),
        "internet.ipv6" => draw!(internet::IPv6(l)),
        "internet.mac" => draw!(internet::MACAddress(l)),
        "internet.userAgent" => draw!(internet::UserAgent(l)),
        "finance.creditCard" => draw!(creditcard::CreditCardNumber(l)),
        "finance.currencyCode" => draw!(currency::CurrencyCode(l)),
        "finance.currencyName" => draw!(currency::CurrencyName(l)),
        "finance.bic" => draw!(finance::Bic(l)),
        "finance.isin" => draw!(finance::Isin(l)),
        "text.word" => draw!(lorem::Word(l)),
        "text.sentence" => draw!(lorem::Sentence(l, 6..12)),
        "text.paragraph" => draw!(lorem::Paragraph(l, 3..6)),
        "file.name" => draw!(filesystem::FileName(l)),
        "file.path" => draw!(filesystem::FilePath(l)),
        "file.extension" => draw!(filesystem::FileExtension(l)),
        "file.mimeType" => draw!(filesystem::MimeType(l)),
        "book.isbn" => draw!(barcode::Isbn13(l)),
        _ => return None,
    };
    let mut seen = HashSet::new();
    Some(values.into_iter().filter(|v| seen.insert(v.clone())).collect())
}

/// FNV-1a, written out so the salt for a seed cannot move with a dependency.
fn salt(seed: &str, key: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in seed.bytes().chain([0]).chain(key.bytes()) {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn list_literal(values: &[String]) -> String {
    let items: Vec<String> = values.iter().map(|v| format!("'{}'", sql_escape(v))).collect();
    format!("[{}]", items.join(","))
}

fn pick_from(values: &[String], hash: &str) -> String {
    format!("{}[1 + ({hash} % {}::UBIGINT)::BIGINT]", list_literal(values), values.len())
}

/// A number from either a JSON number or the text a form field stores. Blank is absent.
fn number(obj: &JsonValue, key: &str) -> Result<Option<f64>, String> {
    match obj.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(n)) => Ok(n.as_f64()),
        Some(JsonValue::String(s)) if s.trim().is_empty() => Ok(None),
        Some(JsonValue::String(s)) => s
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| format!("{key} must be a number, not {s:?}")),
        Some(other) => Err(format!("{key} must be a number, not {other}")),
    }
}

fn text(obj: &JsonValue, key: &str) -> Option<String> {
    match obj.get(key) {
        Some(JsonValue::String(s)) => Some(s.clone()),
        Some(JsonValue::Number(n)) => Some(n.to_string()),
        _ => None,
    }
    .filter(|s| !s.trim().is_empty())
}

fn percent(obj: &JsonValue, key: &str, default: f64) -> Result<f64, String> {
    let p = number(obj, key)?.unwrap_or(default);
    if (0.0..=100.0).contains(&p) {
        Ok(p)
    } else {
        Err(format!("{key} must be between 0 and 100, not {p}"))
    }
}

fn parse_date(s: &str) -> Result<NaiveDate, String> {
    NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
        .map_err(|_| format!("{s:?} is not a date in the form 2024-12-31"))
}

fn parse_timestamp(s: &str) -> Result<NaiveDateTime, String> {
    let t = s.trim();
    ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(t, f).ok())
        .or_else(|| NaiveDate::parse_from_str(t, "%Y-%m-%d").ok()?.and_hms_opt(0, 0, 0))
        .ok_or_else(|| format!("{s:?} is not a time in the form 2024-12-31 23:59:59"))
}

/// One column being compiled.
struct Column<'a> {
    seed: &'a str,
    locale: Locale,
    name: &'a str,
    props: &'a JsonValue,
    /// How many columns before this one use the same generator.
    ///
    /// The first first-name column and the first email column share a slot, so one row's
    /// email is that row's person. A second person in the row takes the next slot.
    slot: usize,
}

impl Column<'_> {
    /// `hash(...)` for this row, salted by what it is for. The key is the column's name
    /// unless the value is meant to be shared with another column.
    ///
    /// The salt goes INTO the hashed value. DuckDB's two-argument `hash(i, salt)` keeps the
    /// row's low bits tied across salts, so every column with the same power-of-two number
    /// of values moved in lockstep with every other: a 4-value pick beside a 4-value pick
    /// showed 4 of the 16 pairs.
    fn hash(&self, key: &str) -> String {
        format!("hash(xor(i::UBIGINT, {}::UBIGINT))", salt(self.seed, key))
    }

    fn own_hash(&self, suffix: &str) -> String {
        self.hash(&format!("{}|{suffix}", self.name))
    }

    /// An index into a list of `n`.
    fn within(&self, suffix: &str, n: u64) -> String {
        format!("({} % {n}::UBIGINT)", self.own_hash(suffix))
    }

    fn pool(&self, generator: &str) -> Result<Vec<String>, String> {
        (self.locale.pool)(generator)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| format!("no generator called {generator:?}"))
    }

    /// This row's person, as (first name, last name, the same two as plain lowercase ASCII).
    ///
    /// The ASCII lists are the name lists transliterated, in the same order, which is what
    /// lets an email read like the kanji name beside it.
    fn person(&self) -> Result<[String; 4], String> {
        let plain = |values: &[String]| -> Vec<String> {
            values
                .iter()
                .map(|v| {
                    let s: String = deunicode::deunicode(v)
                        .to_lowercase()
                        .chars()
                        .filter(char::is_ascii_alphanumeric)
                        .collect();
                    if s.is_empty() { "user".to_string() } else { s }
                })
                .collect()
        };
        let first = self.pool("person.firstName")?;
        let last = self.pool("person.lastName")?;
        let first_hash = self.hash(&format!("person|{}|first", self.slot));
        let last_hash = self.hash(&format!("person|{}|last", self.slot));
        Ok([
            pick_from(&first, &first_hash),
            pick_from(&last, &last_hash),
            pick_from(&plain(&first), &first_hash),
            pick_from(&plain(&last), &last_hash),
        ])
    }

    /// A string shaped by `format`: `#` a digit, `^` a digit other than zero, `?` a capital
    /// letter, `*` a letter or digit. Anything else stands for itself.
    fn shaped(&self, format: &str) -> String {
        const ALNUM: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut parts = Vec::new();
        let mut literal = String::new();
        for (pos, c) in format.chars().enumerate() {
            let spot = pos.to_string();
            let picked = match c {
                '#' => format!("chr(48 + {}::INTEGER)", self.within(&spot, 10)),
                '^' => format!("chr(49 + {}::INTEGER)", self.within(&spot, 9)),
                '?' => format!("chr(65 + {}::INTEGER)", self.within(&spot, 26)),
                '*' => format!(
                    "substr('{ALNUM}', 1 + {}::INTEGER, 1)",
                    self.within(&spot, ALNUM.len() as u64)
                ),
                _ => {
                    literal.push(c);
                    continue;
                }
            };
            if !literal.is_empty() {
                parts.push(format!("'{}'", sql_escape(&literal)));
                literal.clear();
            }
            parts.push(picked);
        }
        if !literal.is_empty() {
            parts.push(format!("'{}'", sql_escape(&literal)));
        }
        parts.join(" || ")
    }

    fn integer(&self) -> Result<String, String> {
        let min = number(self.props, "min")?.unwrap_or(0.0) as i64;
        let max = number(self.props, "max")?.unwrap_or(100.0) as i64;
        if max < min {
            return Err(format!("max ({max}) is below min ({min})"));
        }
        let span = (i128::from(max) - i128::from(min) + 1) as u64;
        Ok(format!("({min} + {}::BIGINT)::BIGINT", self.within("v", span)))
    }

    fn decimal(&self) -> Result<String, String> {
        let min = number(self.props, "min")?.unwrap_or(0.0);
        let max = number(self.props, "max")?.unwrap_or(1000.0);
        let scale = number(self.props, "scale")?.unwrap_or(2.0) as i32;
        if !(0..=12).contains(&scale) {
            return Err(format!("scale must be between 0 and 12, not {scale}"));
        }
        if max < min {
            return Err(format!("max ({max}) is below min ({min})"));
        }
        // Whole units of the last decimal place, so the value is exact and the range holds.
        let unit = 10f64.powi(scale);
        let (low, high) = ((min * unit).round() as i64, (max * unit).round() as i64);
        let span = (i128::from(high) - i128::from(low) + 1) as u64;
        let per_unit = if scale == 0 {
            "1".to_string()
        } else {
            format!("0.{}1", "0".repeat(scale as usize - 1))
        };
        Ok(format!(
            "(({low} + {}::BIGINT)::DECIMAL(18,0) * {per_unit})::DECIMAL(18,{scale})",
            self.within("v", span)
        ))
    }

    fn date(&self) -> Result<String, String> {
        let min = parse_date(&text(self.props, "min").unwrap_or_else(|| "2020-01-01".into()))?;
        let max = parse_date(&text(self.props, "max").unwrap_or_else(|| "2025-12-31".into()))?;
        if max < min {
            return Err(format!("max ({max}) is below min ({min})"));
        }
        let days = (max - min).num_days() as u64 + 1;
        Ok(format!("(DATE '{min}' + {}::INTEGER)", self.within("v", days)))
    }

    fn timestamp(&self) -> Result<String, String> {
        let min = parse_timestamp(&text(self.props, "min").unwrap_or_else(|| "2020-01-01".into()))?;
        let max = parse_timestamp(&text(self.props, "max").unwrap_or_else(|| "2025-12-31 23:59:59".into()))?;
        if max < min {
            return Err(format!("max ({max}) is below min ({min})"));
        }
        let seconds = (max - min).num_seconds() as u64 + 1;
        Ok(format!(
            "(TIMESTAMP '{}' + to_seconds({}::BIGINT))",
            min.format("%Y-%m-%d %H:%M:%S"),
            self.within("v", seconds)
        ))
    }

    fn uuid(&self) -> String {
        let (a, b) = (self.own_hash("a"), self.own_hash("b"));
        format!(
            "printf('%08x-%04x-4%03x-%04x-%012x', {a} >> 32, ({a} >> 16) & 65535::UBIGINT, {a} & 4095::UBIGINT, \
             (({b} >> 48) & 16383::UBIGINT) | 32768::UBIGINT, {b} & 281474976710655::UBIGINT)"
        )
    }

    fn pick(&self) -> Result<String, String> {
        let values: Vec<String> = match self.props.get("values") {
            Some(JsonValue::Array(a)) => {
                a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()
            }
            _ => text(self.props, "values")
                .unwrap_or_default()
                .split(['\n', ','])
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .collect(),
        };
        if values.is_empty() {
            return Err("pick needs values to choose from, separated by commas or new lines".into());
        }
        Ok(pick_from(&values, &self.own_hash("v")))
    }

    fn expr(&self, generator: &str) -> Result<String, String> {
        let shortest = |formats: &[&str]| {
            formats.iter().min_by_key(|f| f.chars().count()).map(|f| f.to_string()).unwrap_or_default()
        };
        Ok(match generator {
            "" => return Err("needs a generator".into()),
            "sequence" => {
                let start = number(self.props, "start")?.unwrap_or(1.0) as i64;
                let step = number(self.props, "step")?.unwrap_or(1.0) as i64;
                format!("({start} + i * {step})::BIGINT")
            }
            "integer" => self.integer()?,
            "decimal" => self.decimal()?,
            "boolean" => {
                let p = percent(self.props, "percent", 50.0)?;
                format!("({} < {})", self.within("v", 10_000), (p * 100.0).round())
            }
            "date" => self.date()?,
            "timestamp" => self.timestamp()?,
            "uuid" => self.uuid(),
            "pick" => self.pick()?,
            "pattern" => {
                let pattern = text(self.props, "pattern")
                    .ok_or("pattern needs a pattern, such as AB-##-??")?;
                self.shaped(&pattern)
            }
            "constant" => format!("'{}'", sql_escape(&text(self.props, "value").unwrap_or_default())),
            "person.firstName" => {
                let [first, ..] = self.person()?;
                first
            }
            "person.lastName" => {
                let [_, last, ..] = self.person()?;
                last
            }
            "person.fullName" => {
                let [first, last, ..] = self.person()?;
                let tpl = self.locale.name_tpl;
                let gap = tpl.replace("{FirstName}", "").replace("{LastName}", "");
                let (a, b) = if tpl.starts_with("{LastName}") { (last, first) } else { (first, last) };
                format!("{a} || '{}' || {b}", sql_escape(&gap))
            }
            "internet.email" => {
                let [.., first, last] = self.person()?;
                // example.com and friends are reserved by RFC 2606, so a generated address
                // can never reach a real mailbox, whatever a test does with it. The row
                // number makes it unique, which a column named email is usually asked to be.
                let tld = pick_from(&["com".into(), "net".into(), "org".into()], &self.own_hash("tld"));
                format!("{first} || '.' || {last} || (i + 1) || '@example.' || {tld}")
            }
            "internet.username" => {
                let [.., first, last] = self.person()?;
                format!("{first} || '_' || {last} || (i + 1)")
            }
            "address.street" => {
                let streets = self.pool("address.streetName")?;
                format!(
                    "(1 + {}) || ' ' || {}",
                    self.within("number", 9999),
                    pick_from(&streets, &self.own_hash("street"))
                )
            }
            "address.zip" => self.shaped(self.locale.zip.first().copied().unwrap_or("#####")),
            "address.latitude" => format!("(({}::BIGINT - 9000000) / 100000.0)", self.within("v", 18_000_001)),
            "address.longitude" => format!("(({}::BIGINT - 18000000) / 100000.0)", self.within("v", 36_000_001)),
            "phone.number" => self.shaped(&shortest(self.locale.phone)),
            "phone.cell" => self.shaped(&shortest(self.locale.cell)),
            other => pick_from(&self.pool(other)?, &self.own_hash("v")),
        })
    }
}

fn row_count(props: &JsonValue) -> Result<u64, String> {
    let n = number(props, "rowCount").map_err(|e| format!("src.generate: {e}"))?.unwrap_or(100.0);
    if n < 0.0 || n.fract() != 0.0 {
        return Err(format!("src.generate: rowCount must be a whole number of rows, not {n}"));
    }
    Ok(n as u64)
}

pub(crate) fn build_source(props: &JsonValue) -> Result<String, String> {
    let rows = row_count(props)?;
    let seed = text(props, "seed").unwrap_or_else(|| "duckle".into());
    let locale_name = text(props, "locale").unwrap_or_else(|| "en".into());
    let locale = LOCALES
        .iter()
        .find(|(name, _)| *name == locale_name)
        .map(|(_, l)| *l)
        .ok_or_else(|| format!("src.generate: no locale called {locale_name:?}"))?;
    let columns = props
        .get("columns")
        .and_then(JsonValue::as_array)
        .filter(|c| !c.is_empty())
        .ok_or("src.generate: needs at least one column")?;

    let mut seen = HashSet::new();
    let mut used: HashMap<String, usize> = HashMap::new();
    let mut projection = Vec::with_capacity(columns.len());
    for props in columns {
        let name = text(props, "name").unwrap_or_default().trim().to_string();
        if name.is_empty() {
            return Err("src.generate: every column needs a name".into());
        }
        if !seen.insert(name.to_lowercase()) {
            return Err(format!("src.generate: column {name:?} is listed twice"));
        }
        let generator = text(props, "generator").unwrap_or_default();
        let slot = used.entry(generator.clone()).or_default();
        let column = Column { seed: &seed, locale, name: &name, props, slot: *slot };
        *slot += 1;
        let wrap = |e: String| format!("src.generate: column {name:?}: {e}");
        let mut expr = column.expr(generator.trim()).map_err(wrap)?;
        let nulls = percent(props, "nullPercent", 0.0).map_err(wrap)?;
        if nulls > 0.0 {
            let drop = column.within("null", 10_000);
            expr = format!("CASE WHEN {drop} < {} THEN NULL ELSE {expr} END", (nulls * 100.0).round());
        }
        projection.push(format!("{expr} AS {}", quote_ident(&name)));
    }
    Ok(format!("SELECT {} FROM range({rows}) t(i)", projection.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog_field(key: &str) -> Vec<String> {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../duckle-mcp/catalog.json");
        let catalog: JsonValue = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let component = catalog["components"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == "src.generate")
            .expect("src.generate is in the catalog");
        let fields = component["manifest"]["sections"][0]["fields"].as_array().unwrap();
        let field = fields.iter().find(|f| f["key"] == key).expect("the field exists");
        field["options"].as_array().unwrap().iter().map(|o| o["value"].as_str().unwrap().to_string()).collect()
    }

    #[test]
    fn the_form_offers_every_generator_the_engine_has() {
        let mut form = catalog_field("columns");
        let mut engine: Vec<String> = names().iter().map(|n| n.to_string()).collect();
        form.sort();
        engine.sort();
        assert_eq!(form, engine);
    }

    #[test]
    fn the_form_offers_every_locale_the_engine_has() {
        let mut form = catalog_field("locale");
        let mut engine: Vec<String> = locales().iter().map(|n| n.to_string()).collect();
        form.sort();
        engine.sort();
        assert_eq!(form, engine);
    }

    #[test]
    fn a_value_is_written_as_a_literal_never_as_sql() {
        let sql = build_source(&serde_json::json!({
            "columns": [
                { "name": "a", "generator": "pick", "values": "x'); DROP TABLE t; --" },
                { "name": "b", "generator": "constant", "value": "' OR 1=1" },
                { "name": "c\"d", "generator": "pattern", "pattern": "'#" },
            ],
        }))
        .unwrap();
        assert!(sql.contains("'x''); DROP TABLE t; --'"), "{sql}");
        assert!(sql.contains("''' OR 1=1'"), "{sql}");
        assert!(sql.contains("\"c\"\"d\""), "{sql}");
    }
}
