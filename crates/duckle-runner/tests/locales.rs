//! Every UI language carries every key the English file has, with the same
//! `{{placeholders}}`. A missing key shows English in the middle of a translated
//! screen; a translated placeholder (`{{path}}` written `{{pad}}`) prints literally.
//! Both had shipped in most of the 60 locales (#378).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn locales_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../frontend/src/i18n/locales")
}

fn flatten(prefix: &str, v: &serde_json::Value, out: &mut BTreeMap<String, String>) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, child) in map {
                let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                flatten(&key, child, out);
            }
        }
        other => {
            out.insert(prefix.to_string(), other.as_str().map(String::from).unwrap_or_else(|| other.to_string()));
        }
    }
}

fn strings(file: &Path) -> BTreeMap<String, String> {
    let text = std::fs::read_to_string(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
    let json: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{} is not JSON: {e}", file.display()));
    let mut out = BTreeMap::new();
    flatten("", &json, &mut out);
    out
}

/// The names between `{{` and `}}`, in any script.
fn placeholders(s: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut rest = s;
    while let Some(open) = rest.find("{{") {
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else { break };
        out.insert(after[..close].trim().to_string());
        rest = &after[close + 2..];
    }
    out
}

#[test]
fn every_locale_has_every_english_key_with_its_placeholders() {
    let dir = locales_dir();
    let en = strings(&dir.join("en.json"));
    let mut problems = Vec::new();
    let mut seen = 0;
    for entry in std::fs::read_dir(&dir).expect("the locales directory") {
        let path = entry.expect("a directory entry").path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !name.ends_with(".json") || name == "en.json" {
            continue;
        }
        seen += 1;
        let tr = strings(&path);
        for (key, english) in &en {
            match tr.get(key) {
                None => problems.push(format!("{name}: missing {key}")),
                Some(t) if t.trim().is_empty() => problems.push(format!("{name}: {key} is empty")),
                Some(t) if placeholders(t) != placeholders(english) => problems.push(format!(
                    "{name}: {key} has {:?}, English has {:?}",
                    placeholders(t),
                    placeholders(english)
                )),
                Some(_) => {}
            }
        }
    }
    assert!(seen >= 50, "expected the full set of locales beside en.json, found {seen}");
    assert!(problems.is_empty(), "{} problem(s):\n{}", problems.len(), problems.join("\n"));
}
