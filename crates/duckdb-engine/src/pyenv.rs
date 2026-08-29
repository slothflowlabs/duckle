//! #246: what a workspace's Python environment actually contains, and whether
//! it matches the lock file committed beside it.
//!
//! Finding the interpreter was never the hard part. The failure this exists for
//! is two machines that both look correct:
//!
//! ```text
//! machine A   .venv with splink==4.0.0
//! machine B   .venv with splink==3.9.1
//! ```
//!
//! Same pipeline, same `uv.lock`, different answers, and nothing anywhere says
//! so. Duckle therefore reads the environment as it IS - the distributions
//! installed in it - rather than trusting a marker written by whoever created
//! it. A stamp file records an intention; `*.dist-info` records the fact.
//!
//! Nothing here installs anything. Resolving dependencies during a pipeline run
//! would turn a missing package into a mid-run download, which is exactly what
//! an air-gapped or scheduled run cannot have. `duckle-runner python prepare`
//! is a separate, explicit step.

use std::path::{Path, PathBuf};

/// A workspace's Python environment, as found.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Env {
    /// The interpreter `code.python` would use, if the workspace has its own.
    pub interpreter: Option<String>,
    /// From `pyvenv.cfg`, so no interpreter has to be started to learn it.
    pub python_version: Option<String>,
    /// OS and architecture, because a lock resolves per platform.
    pub platform: String,
    /// SHA-256 of `uv.lock` as committed. `None` when there is no lock, which
    /// is what turns every check here off.
    pub lock_sha256: Option<String>,
    /// name -> version, as actually installed.
    pub installed: Vec<(String, String)>,
    /// name -> version, as the lock says.
    pub locked: Vec<(String, String)>,
    /// SHA-256 over the installed set plus the Python version. The identity of
    /// the environment itself, independent of who built it.
    pub environment_hash: Option<String>,
}

/// One way the environment and the lock disagree.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum Drift {
    /// Installed at a version the lock does not name. The machine A / machine B
    /// case, and the reason this module exists.
    Version { name: String, installed: String, locked: String },
    /// Installed and not in the lock at all: something was added on top.
    Unlocked { name: String, installed: String },
    /// In the lock and not installed.
    ///
    /// Reported, but not on its own a failure. A lock file lists the whole
    /// resolution across platforms and extras, so a package absent here may
    /// simply not apply here. Getting that wrong would fail correct runs, and
    /// a package that really is needed and really is missing raises ImportError
    /// on the first row, which is already unambiguous.
    Missing { name: String, locked: String },
}

impl Drift {
    /// Does this difference mean the environment is not the locked one?
    pub fn is_failure(&self) -> bool {
        !matches!(self, Drift::Missing { .. })
    }
}

/// The distributions that BUILD an environment rather than live in it.
///
/// `python -m venv` seeds pip, and setuptools and wheel before 3.12. This
/// module supports a stdlib venv on purpose, so treating the tooling as a lock
/// violation would refuse every correct workspace built that way - and a check
/// that fires on a correct setup is a check somebody turns off.
///
/// They are still REPORTED by `installed_packages` and still counted in the
/// environment hash: two machines on different pip versions are genuinely
/// different environments. They are only exempt from being called drift.
const BOOTSTRAP: &[&str] = &["pip", "setuptools", "wheel", "pkg-resources", "distribute"];

/// PEP 503 normalisation, so `Foo.Bar_baz` and `foo-bar-baz` are one package.
fn normalize(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut last_dash = false;
    for c in name.chars() {
        if c == '-' || c == '_' || c == '.' {
            if !last_dash {
                out.push('-');
                last_dash = true;
            }
        } else {
            out.extend(c.to_lowercase());
            last_dash = false;
        }
    }
    out.trim_matches('-').to_string()
}

fn sha256_bytes(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Where a venv keeps its installed distributions, on either platform and for
/// any Python minor version.
fn site_packages(venv: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    // Windows layout.
    let win = venv.join("Lib").join("site-packages");
    if win.is_dir() {
        dirs.push(win);
    }
    // POSIX layout: lib/pythonX.Y/site-packages, and lib64 on some distros.
    for lib in ["lib", "lib64"] {
        if let Ok(rd) = std::fs::read_dir(venv.join(lib)) {
            for e in rd.flatten() {
                let sp = e.path().join("site-packages");
                if sp.is_dir() {
                    dirs.push(sp);
                }
            }
        }
    }
    dirs
}

/// What is installed, read from `*.dist-info` directory names.
///
/// The directory name is `name-version.dist-info` by packaging spec, so the
/// answer needs no file opened and no interpreter started - which matters
/// because this runs before every pipeline that has a Python stage.
pub fn installed_packages(venv: &Path) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for sp in site_packages(venv) {
        let Ok(rd) = std::fs::read_dir(&sp) else {
            continue;
        };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            let Some(stem) = name.strip_suffix(".dist-info") else {
                continue;
            };
            // Split on the LAST '-': a version never contains one, a name may.
            let Some((n, v)) = stem.rsplit_once('-') else {
                continue;
            };
            out.push((normalize(n), v.to_string()));
        }
    }
    out.sort();
    out.dedup();
    out
}

/// What `uv.lock` says should be there.
///
/// Only the name and version of each `[[package]]`. The lock also carries
/// resolution markers, and honouring those properly means implementing PEP 508
/// marker evaluation - which is why a locked package that is not installed is
/// reported rather than treated as an error.
pub fn locked_packages(lock: &Path) -> Vec<(String, String)> {
    let Ok(text) = std::fs::read_to_string(lock) else {
        return Vec::new();
    };
    let Ok(doc) = text.parse::<toml::Table>() else {
        return Vec::new();
    };
    let mut out: Vec<(String, String)> = doc
        .get("package")
        .and_then(|p| p.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|p| {
                    let t = p.as_table()?;
                    let n = t.get("name")?.as_str()?;
                    let v = t.get("version")?.as_str()?;
                    Some((normalize(n), v.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out.dedup();
    out
}

/// The Python version a venv was built with, from `pyvenv.cfg`.
fn venv_python_version(venv: &Path) -> Option<String> {
    let text = std::fs::read_to_string(venv.join("pyvenv.cfg")).ok()?;
    for line in text.lines() {
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        // uv writes `version_info`, the stdlib venv module writes `version`.
        if matches!(k.trim(), "version" | "version_info") {
            return Some(v.trim().to_string());
        }
    }
    None
}

/// Read a workspace's Python environment.
pub fn inspect(workspace: &Path) -> Env {
    let venv = workspace.join(".venv");
    let lock = workspace.join("uv.lock");
    let installed = installed_packages(&venv);
    let python_version = venv_python_version(&venv);
    // The identity of the environment: what is in it, and what runs it. Sorted
    // and normalised, so two machines that installed the same things in a
    // different order agree.
    let environment_hash = if installed.is_empty() {
        None
    } else {
        let mut material = String::new();
        material.push_str(python_version.as_deref().unwrap_or("unknown"));
        material.push('\n');
        for (n, v) in &installed {
            material.push_str(n);
            material.push_str("==");
            material.push_str(v);
            material.push('\n');
        }
        Some(sha256_bytes(material.as_bytes()))
    };
    Env {
        interpreter: crate::connectors::python_in_workspace(workspace),
        python_version,
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        lock_sha256: std::fs::read(&lock).ok().map(|b| sha256_bytes(&b)),
        installed,
        locked: locked_packages(&lock),
        environment_hash,
    }
}

/// How the environment and the lock disagree.
///
/// Empty when there is no lock: a workspace that never committed one has not
/// asked for this, and inventing a requirement it did not declare would break
/// every existing pipeline.
pub fn drift(env: &Env) -> Vec<Drift> {
    if env.lock_sha256.is_none() {
        return Vec::new();
    }
    let locked: std::collections::BTreeMap<&str, &str> =
        env.locked.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
    let installed: std::collections::BTreeMap<&str, &str> =
        env.installed.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
    let mut out = Vec::new();
    for (n, iv) in &installed {
        match locked.get(n) {
            Some(lv) if lv == iv => {}
            Some(lv) => out.push(Drift::Version {
                name: n.to_string(),
                installed: iv.to_string(),
                locked: lv.to_string(),
            }),
            // Scaffolding being present is never evidence that the
            // environment is wrong.
            None if BOOTSTRAP.contains(n) => {}
            None => out.push(Drift::Unlocked {
                name: n.to_string(),
                installed: iv.to_string(),
            }),
        }
    }
    for (n, lv) in &locked {
        if !installed.contains_key(n) {
            out.push(Drift::Missing {
                name: n.to_string(),
                locked: lv.to_string(),
            });
        }
    }
    out
}

/// One line per difference, for a person.
pub fn describe(drifts: &[Drift]) -> String {
    drifts
        .iter()
        .map(|d| match d {
            Drift::Version { name, installed, locked } => {
                format!("  {name}: installed {installed}, locked {locked}")
            }
            Drift::Unlocked { name, installed } => {
                format!("  {name}: installed {installed}, not in the lock")
            }
            Drift::Missing { name, locked } => {
                format!("  {name}: locked {locked}, not installed")
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Refuse to run a Python stage against an environment that is not the locked
/// one, unless the operator says otherwise.
///
/// Only fires when a lock exists. `DUCKLE_PYTHON_ALLOW_DRIFT=1` downgrades it
/// to a warning, because there is always a machine where the rule is wrong and
/// a check with no way past it gets deleted rather than fixed.
pub fn guard(workspace: &Path) -> Result<Option<String>, crate::EngineError> {
    let env = inspect(workspace);
    let drifts = drift(&env);
    if drifts.is_empty() {
        return Ok(None);
    }
    let failures: Vec<Drift> = drifts.iter().filter(|d| d.is_failure()).cloned().collect();
    let report = describe(&drifts);
    if failures.is_empty() || std::env::var("DUCKLE_PYTHON_ALLOW_DRIFT").is_ok_and(|v| v != "0") {
        return Ok(Some(format!(
            "python: the workspace environment differs from uv.lock:\n{report}"
        )));
    }
    Err(crate::EngineError::Config(format!(
        "python: the workspace .venv is not the environment uv.lock describes, so this run \
         would not be the run the lock says it is:\n{}\n\nRun `duckle-runner python prepare` to \
         rebuild it from the lock, or set DUCKLE_PYTHON_ALLOW_DRIFT=1 to run anyway.",
        describe(&failures)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn venv_with(dir: &Path, pkgs: &[(&str, &str)], py: &str) {
        let sp = dir.join(".venv").join("Lib").join("site-packages");
        std::fs::create_dir_all(&sp).unwrap();
        for (n, v) in pkgs {
            std::fs::create_dir_all(sp.join(format!("{n}-{v}.dist-info"))).unwrap();
        }
        std::fs::write(
            dir.join(".venv").join("pyvenv.cfg"),
            format!("home = /usr\nversion = {py}\n"),
        )
        .unwrap();
    }

    fn lock_with(dir: &Path, pkgs: &[(&str, &str)]) {
        let body: String = pkgs
            .iter()
            .map(|(n, v)| format!("[[package]]\nname = \"{n}\"\nversion = \"{v}\"\n\n"))
            .collect();
        std::fs::write(dir.join("uv.lock"), format!("version = 1\n\n{body}")).unwrap();
    }

    #[test]
    fn a_name_is_the_same_package_however_it_is_written() {
        assert_eq!(normalize("Foo.Bar_baz"), "foo-bar-baz");
        assert_eq!(normalize("scikit__learn"), "scikit-learn");
        assert_eq!(normalize("pyarrow"), "pyarrow");
    }

    /// The dist-info name is `name-version`, and a package name may contain a
    /// hyphen while a version may not - so the split is on the LAST one.
    #[test]
    fn a_hyphenated_package_name_keeps_its_version() {
        let tmp = tempfile::tempdir().unwrap();
        venv_with(tmp.path(), &[("beautifulsoup4", "4.12.3"), ("ruamel-yaml", "0.18.6")], "3.12.4");
        let got = installed_packages(&tmp.path().join(".venv"));
        assert_eq!(
            got,
            vec![
                ("beautifulsoup4".to_string(), "4.12.3".to_string()),
                ("ruamel-yaml".to_string(), "0.18.6".to_string()),
            ]
        );
    }

    /// The failure this module exists for: both machines look fine.
    #[test]
    fn the_same_package_at_a_different_version_is_drift() {
        let tmp = tempfile::tempdir().unwrap();
        venv_with(tmp.path(), &[("splink", "3.9.1")], "3.12.4");
        lock_with(tmp.path(), &[("splink", "4.0.0")]);
        let d = drift(&inspect(tmp.path()));
        assert_eq!(
            d,
            vec![Drift::Version {
                name: "splink".into(),
                installed: "3.9.1".into(),
                locked: "4.0.0".into()
            }]
        );
        assert!(d[0].is_failure());
        assert!(guard(tmp.path()).is_err(), "a wrong version must stop the run");
    }

    /// A lock resolves for every platform, so something it names being absent
    /// here is information, not proof of a broken environment.
    #[test]
    fn a_locked_package_that_is_not_installed_reports_but_does_not_fail() {
        let tmp = tempfile::tempdir().unwrap();
        venv_with(tmp.path(), &[("pyarrow", "17.0.0")], "3.12.4");
        lock_with(tmp.path(), &[("pyarrow", "17.0.0"), ("pywin32", "306")]);
        let d = drift(&inspect(tmp.path()));
        assert_eq!(d.len(), 1);
        assert!(!d[0].is_failure());
        let warned = guard(tmp.path()).expect("must not fail");
        assert!(warned.unwrap_or_default().contains("pywin32"), "but must be said out loud");
    }

    /// `python -m venv` seeds pip (and setuptools/wheel before 3.12), and this
    /// module explicitly supports a stdlib venv - so a correct workspace would
    /// have been refused for carrying the tools that built it.
    ///
    /// Scaffolding being present is never evidence that the environment is
    /// wrong, and a check that fires on every correct setup gets turned off.
    #[test]
    fn the_tools_that_build_a_venv_are_not_drift() {
        let tmp = tempfile::tempdir().unwrap();
        venv_with(
            tmp.path(),
            &[("pyarrow", "17.0.0"), ("pip", "24.2"), ("setuptools", "75.1.0"), ("wheel", "0.44.0")],
            "3.11.9",
        );
        lock_with(tmp.path(), &[("pyarrow", "17.0.0")]);
        assert!(
            drift(&inspect(tmp.path())).is_empty(),
            "a stdlib venv's own tooling is not a lock violation: {:?}",
            drift(&inspect(tmp.path()))
        );
        assert!(guard(tmp.path()).unwrap().is_none(), "and must not stop the run");
    }

    /// Something pip-installed on top is exactly what the lock is supposed to
    /// rule out, so it counts.
    #[test]
    fn a_package_the_lock_never_mentions_is_drift() {
        let tmp = tempfile::tempdir().unwrap();
        venv_with(tmp.path(), &[("pyarrow", "17.0.0"), ("lightgbm", "4.5.0")], "3.12.4");
        lock_with(tmp.path(), &[("pyarrow", "17.0.0")]);
        let d = drift(&inspect(tmp.path()));
        assert_eq!(
            d,
            vec![Drift::Unlocked { name: "lightgbm".into(), installed: "4.5.0".into() }]
        );
        assert!(guard(tmp.path()).is_err());
    }

    /// A workspace with no lock has not asked for any of this.
    #[test]
    fn no_lock_means_no_check() {
        let tmp = tempfile::tempdir().unwrap();
        venv_with(tmp.path(), &[("splink", "3.9.1")], "3.12.4");
        assert!(drift(&inspect(tmp.path())).is_empty());
        assert!(guard(tmp.path()).unwrap().is_none());
    }

    /// Two machines that installed the same things agree; one extra package
    /// does not.
    #[test]
    fn the_environment_hash_is_the_contents_not_the_order() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        venv_with(a.path(), &[("pyarrow", "17.0.0"), ("polars", "1.6.0")], "3.12.4");
        venv_with(b.path(), &[("polars", "1.6.0"), ("pyarrow", "17.0.0")], "3.12.4");
        assert_eq!(inspect(a.path()).environment_hash, inspect(b.path()).environment_hash);

        let c = tempfile::tempdir().unwrap();
        venv_with(c.path(), &[("pyarrow", "17.0.0"), ("polars", "1.6.1")], "3.12.4");
        assert_ne!(inspect(a.path()).environment_hash, inspect(c.path()).environment_hash);
    }

    /// The interpreter that runs the packages is part of the environment: the
    /// same wheels under a different Python are not the same environment.
    #[test]
    fn the_python_version_is_part_of_the_environment_hash() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        venv_with(a.path(), &[("pyarrow", "17.0.0")], "3.12.4");
        venv_with(b.path(), &[("pyarrow", "17.0.0")], "3.13.0");
        assert_ne!(inspect(a.path()).environment_hash, inspect(b.path()).environment_hash);
    }
}
