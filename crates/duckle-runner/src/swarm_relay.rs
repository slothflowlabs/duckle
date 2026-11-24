//! The web editor's side of the agent swarm: start the `duckle-mcp swarm`
//! process installed beside this binary and relay its events to the browser as
//! Server-Sent Events. The desktop app does the same over a Tauri channel.
//!
//! If the browser goes away, the swarm is told to stop, and ended if it has not
//! within a few seconds.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

/// duckle-mcp beside this binary, where the pip package, the Docker image and
/// a cargo build all put it.
pub fn server() -> Result<PathBuf, String> {
    let name = if cfg!(windows) {
        "duckle-mcp.exe"
    } else {
        "duckle-mcp"
    };
    let exe = std::env::current_exe().map_err(|e| format!("locating this executable: {e}"))?;
    exe.parent()
        .map(|d| d.join(name))
        .filter(|p| p.is_file())
        .ok_or_else(|| format!("The swarm needs {name} next to {}.", exe.display()))
}

fn command(server: &Path) -> Command {
    let mut cmd = Command::new(server);
    cmd.arg("swarm");
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    cmd
}

/// Which ways of reaching a model this server offers.
pub fn status() -> Result<Value, String> {
    let out = command(&server()?)
        .arg("--status")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("starting the swarm server: {e}"))?;
    serde_json::from_slice(&out.stdout)
        .map_err(|e| format!("reading the swarm server's answer: {e}"))
}

fn frame(out: &mut dyn Write, event: Option<&str>, data: &str) -> std::io::Result<()> {
    if let Some(e) = event {
        write!(out, "event: {e}\n")?;
    }
    write!(out, "data: {data}\n\n")?;
    out.flush()
}

/// Run one swarm, writing each event to `out` as a `data:` frame and the
/// `finished` event again as `event: result`, as /api/run_stream ends.
pub fn stream(out: &mut dyn Write, request: &Value) -> Result<(), String> {
    stream_with(
        &mut command(&server()?),
        out,
        request,
        Duration::from_secs(5),
    )
}

fn stream_with(
    cmd: &mut Command,
    out: &mut dyn Write,
    request: &Value,
    tick: Duration,
) -> Result<(), String> {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("starting the swarm server: {e}"))?;
    let mut stdin = child.stdin.take().ok_or("the swarm server has no input")?;
    writeln!(stdin, "{request}")
        .and_then(|_| stdin.flush())
        .map_err(|e| format!("starting the swarm: {e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or("the swarm server has no output")?;
    let (tx, rx) = mpsc::channel::<String>();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut finished: Option<String> = None;
    let mut gone: Option<Instant> = None;
    loop {
        let wrote = match rx.recv_timeout(tick) {
            Ok(line) => {
                let is_final = serde_json::from_str::<Value>(&line)
                    .is_ok_and(|v| v.get("type").and_then(Value::as_str) == Some("finished"));
                if is_final {
                    finished = Some(line.clone());
                }
                gone.is_some() || frame(out, None, &line).is_ok()
            }
            // A comment keeps a quiet stream open through proxies, and shows
            // whether the browser is still there.
            Err(RecvTimeoutError::Timeout) => {
                gone.is_some()
                    || out
                        .write_all(b": working\n\n")
                        .and_then(|_| out.flush())
                        .is_ok()
            }
            Err(RecvTimeoutError::Disconnected) => break,
        };
        if !wrote && gone.is_none() {
            gone = Some(Instant::now());
            let _ = writeln!(stdin, "stop");
            let _ = stdin.flush();
        }
        if gone.is_some_and(|t| t.elapsed() > Duration::from_secs(10)) {
            let _ = child.kill();
        }
    }
    let _ = reader.join();
    let _ = child.wait();
    if gone.is_none() {
        // With the page still here, only a crash ends without a finished event (a stop
        // is the page going away): a failure with its reason, sent as an event the
        // page shows, like the server's own.
        let last = finished.unwrap_or_else(|| {
            let ended = json!({ "type": "finished", "ok": false, "stopped": false, "summary": "The swarm ended without finishing." })
                .to_string();
            let _ = frame(out, None, &ended);
            ended
        });
        let _ = frame(out, Some("result"), &last);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in for the swarm server: prints the events it is given, one per
    /// line, then waits for the stop it should be sent.
    fn script(lines: &[&str], wait_for_stop: bool) -> Command {
        #[cfg(windows)]
        {
            let mut body: Vec<String> = lines
                .iter()
                .map(|l| format!("Write-Output '{l}'"))
                .collect();
            if wait_for_stop {
                body.push("[Console]::In.ReadLine() | Out-Null; $l = [Console]::In.ReadLine(); Write-Output ('{\"type\":\"finished\",\"saw\":\"' + $l + '\"}')".into());
            } else {
                body.insert(0, "[Console]::In.ReadLine() | Out-Null".into());
            }
            let mut c = Command::new("powershell");
            c.args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &body.join("; "),
            ]);
            c
        }
        #[cfg(not(windows))]
        {
            let mut body: Vec<String> = lines.iter().map(|l| format!("echo '{l}'")).collect();
            if wait_for_stop {
                body.push("read first; read l; echo \"{\\\"type\\\":\\\"finished\\\",\\\"saw\\\":\\\"$l\\\"}\"".into());
            } else {
                body.insert(0, "read first".into());
            }
            let mut c = Command::new("sh");
            c.args(["-c", &body.join("; ")]);
            c
        }
    }

    #[test]
    fn events_are_relayed_as_frames_and_the_finished_one_ends_as_the_result() {
        let mut cmd = script(
            &[r#"{"type":"started"}"#, r#"{"type":"finished","ok":true}"#],
            false,
        );
        let mut out = Vec::new();
        stream_with(
            &mut cmd,
            &mut out,
            &json!({ "goal": "g" }),
            Duration::from_secs(5),
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert_eq!(
            text,
            "data: {\"type\":\"started\"}\n\n\
             data: {\"type\":\"finished\",\"ok\":true}\n\n\
             event: result\ndata: {\"type\":\"finished\",\"ok\":true}\n\n"
        );
    }

    #[test]
    fn a_swarm_that_dies_without_finishing_ends_as_a_failure_the_page_shows() {
        // Only a crash gets here (a stop is the browser going away), so it is not a stop,
        // and the page only shows what arrives as a data frame.
        let mut cmd = script(&[r#"{"type":"started"}"#], false);
        let mut out = Vec::new();
        stream_with(
            &mut cmd,
            &mut out,
            &json!({ "goal": "g" }),
            Duration::from_secs(5),
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        let frames: Vec<(Option<&str>, Value)> = text
            .split("\n\n")
            .filter(|f| !f.is_empty() && !f.starts_with(':'))
            .map(|f| {
                let event = f.lines().find_map(|l| l.strip_prefix("event: "));
                let data = f.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
                (event, serde_json::from_str(data).unwrap())
            })
            .collect();
        let ended = json!({ "type": "finished", "ok": false, "stopped": false, "summary": "The swarm ended without finishing." });
        assert_eq!(
            frames,
            vec![
                (None, json!({ "type": "started" })),
                (None, ended.clone()),
                (Some("result"), ended),
            ]
        );
    }

    /// A browser that has gone away: every write fails.
    struct Gone;
    impl Write for Gone {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "gone"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_browser_that_goes_away_stops_the_swarm() {
        // The stand-in prints one event, then waits for a second input line:
        // the stop. Without it, it would wait until killed 10 s later.
        let mut cmd = script(&[r#"{"type":"started"}"#], true);
        let started = Instant::now();
        stream_with(
            &mut cmd,
            &mut Gone,
            &json!({ "goal": "g" }),
            Duration::from_millis(200),
        )
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "it waited to be killed: {:?}",
            started.elapsed()
        );
    }
}
