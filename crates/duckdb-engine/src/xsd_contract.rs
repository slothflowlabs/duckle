//! The accepted XSD parser-contract store (#315).
//!
//! The connector and the headless CLI must use the same file format. Keeping
//! the small read/replace operation here prevents an operator accepting a
//! contract through one surface from being invisible to another.

use std::path::{Path, PathBuf};

/// Where accepted parser contracts live for a workspace.
pub fn path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("xsd_contracts")
}

/// Return every well-formed accepted contract, in file order.
pub fn list(path: &Path) -> Result<Vec<(String, String)>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (uri, fingerprint) = line.split_once(char::is_whitespace)?;
            Some((uri.to_string(), fingerprint.trim().to_string()))
        })
        .collect())
}

/// Return the accepted fingerprint for one schema root.
pub fn accepted(path: &Path, uri: &str) -> Option<String> {
    list(path)
        .ok()?
        .into_iter()
        .find_map(|(known_uri, fingerprint)| (known_uri == uri).then_some(fingerprint))
}

/// Replace one URI's accepted fingerprint, preserving comments and other URIs.
///
/// The old value is returned for the audit record. A missing value means this
/// is the first explicit acceptance for the URI.
pub fn accept(path: &Path, uri: &str, fingerprint: &str) -> Result<Option<String>, String> {
    let existing = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let previous = existing.lines().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }
        let (known_uri, known_fingerprint) = trimmed.split_once(char::is_whitespace)?;
        (known_uri == uri).then_some(known_fingerprint.trim().to_string())
    });
    let mut lines: Vec<String> = existing
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return true;
            }
            trimmed
                .split_once(char::is_whitespace)
                .map(|(known_uri, _)| known_uri != uri)
                .unwrap_or(true)
        })
        .map(str::to_string)
        .collect();
    lines.push(format!("{uri} {fingerprint}"));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    std::fs::write(path, lines.join("\n") + "\n")
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(previous)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_one_uri_without_disturbing_comments_or_other_contracts() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join(".duckle/xsd_contracts");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "# keep\na.xsd old\nb.xsd other\n").unwrap();

        assert_eq!(accept(&file, "a.xsd", "new").unwrap(), Some("old".into()));
        assert_eq!(
            list(&file).unwrap(),
            vec![
                ("b.xsd".into(), "other".into()),
                ("a.xsd".into(), "new".into())
            ]
        );
        assert!(std::fs::read_to_string(file)
            .unwrap()
            .starts_with("# keep\n"));
    }

    #[test]
    fn a_missing_store_is_an_empty_store() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("missing");
        assert!(list(&file).unwrap().is_empty());
        assert_eq!(accepted(&file, "a.xsd"), None);
    }
}
