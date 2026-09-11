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

/// Split one `<uri> <fingerprint>` line, from the RIGHT.
///
/// A schema URI is an operator-supplied path and may contain spaces;
/// `C:/my schemas/order.xsd` is ordinary. Splitting at the first whitespace
/// returned `C:/my` as the URI, so a contract written for such a schema could
/// never be read back: every run took the first-sight branch and
/// `xsdChangePolicy: fail` silently stopped refusing anything.
///
/// The fingerprint is a SHA-256 hex digest and never contains whitespace, so
/// the last whitespace is the only unambiguous boundary. Lines written by the
/// old code parse identically, because they had no space to be confused by.
fn split_line(line: &str) -> Option<(&str, &str)> {
    let (uri, fingerprint) = line.rsplit_once(char::is_whitespace)?;
    let (uri, fingerprint) = (uri.trim_end(), fingerprint.trim());
    (!uri.is_empty() && !fingerprint.is_empty()).then_some((uri, fingerprint))
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
            let (uri, fingerprint) = split_line(line)?;
            Some((uri.to_string(), fingerprint.to_string()))
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
        let (known_uri, known_fingerprint) = split_line(trimmed)?;
        (known_uri == uri).then_some(known_fingerprint.to_string())
    });
    let mut lines: Vec<String> = existing
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return true;
            }
            split_line(trimmed).map(|(known_uri, _)| known_uri != uri).unwrap_or(true)
        })
        .map(str::to_string)
        .collect();
    lines.push(format!("{uri} {fingerprint}"));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    // Temp then rename, so a reader sees the whole old store or the whole new
    // one. A bare write truncates first, and a truncated store parses cleanly
    // as "nothing is accepted" - which is the same fail-open as an unreadable
    // URI, reached by a different route. The engine records a contract at run
    // time while an operator can be accepting one from the CLI, so the two
    // really can meet.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, lines.join("\n") + "\n")
        .map_err(|e| format!("{}: {e}", tmp.display()))?;
    // Windows rename REPLACES, which is what this needs; it is not the
    // remove-then-rename that would leave a window with no file at all.
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("{}: {e}", path.display())
    })?;
    Ok(previous)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A schema path containing a space could be WRITTEN but never READ BACK,
    /// because the line was split at the first whitespace and the URI came back
    /// truncated. Nothing reported it: every run then took the first-sight
    /// branch, so under `xsdChangePolicy: fail` a schema that moved was never
    /// refused. Fail-open, silent, and a path with a space is ordinary.
    ///
    /// The fingerprint is a SHA-256 and never contains whitespace, so the split
    /// belongs at the LAST one.
    #[test]
    fn a_uri_containing_a_space_is_found_again() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("xsd_contracts");
        let uri = "C:/my schemas/order v2.xsd";

        accept(&store, uri, "abc123").expect("accepted");
        assert_eq!(
            accepted(&store, uri).as_deref(),
            Some("abc123"),
            "a contract that cannot be read back is a fail-open"
        );

        // And it is one entry, not two: the replace has to match it as well.
        accept(&store, uri, "def456").expect("re-accepted");
        let all = list(&store).expect("listed");
        assert_eq!(all.len(), 1, "the replace did not match its own line: {all:?}");
        assert_eq!(all[0], (uri.to_string(), "def456".to_string()));
    }

    /// The previous fingerprint is what the audit record reports, and it comes
    /// from the same parse, so it has to survive a spaced URI too.
    #[test]
    fn the_previous_fingerprint_survives_a_spaced_uri() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("xsd_contracts");
        let uri = "/srv/xsd files/a.xsd";
        assert_eq!(accept(&store, uri, "one").expect("first"), None);
        assert_eq!(
            accept(&store, uri, "two").expect("second").as_deref(),
            Some("one"),
            "an audit record that cannot name what it replaced is not a record"
        );
    }

    /// A torn write leaves a truncated store, and a truncated store reads as
    /// "nothing is accepted" - the same fail-open by another route. Replacing
    /// through a temp file and a rename means a reader sees the whole old file
    /// or the whole new one.
    ///
    /// This asserts the housekeeping half only: that the rename happened and
    /// left no temp behind. It does NOT prove atomicity, which needs a reader
    /// racing a writer; the guarantee there comes from rename being atomic on
    /// both platforms rather than from this test.
    #[test]
    fn replacing_the_store_leaves_no_temp_file_behind() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("xsd_contracts");
        accept(&store, "a.xsd", "one").expect("a");
        accept(&store, "b.xsd", "two").expect("b");

        let stray: Vec<String> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n != "xsd_contracts")
            .collect();
        assert!(stray.is_empty(), "left a temp file behind: {stray:?}");
        assert_eq!(list(&store).expect("listed").len(), 2);
    }

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
