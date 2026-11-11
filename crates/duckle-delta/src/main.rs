//! duckle-delta: reads one JSON request on stdin, writes one JSON answer on stdout.
//! `{"ok":true,...outcome}` on success; `{"ok":false,"error":"..."}` and exit code 1
//! otherwise. Credentials travel on stdin or in the environment, never in argv.

use std::io::Read;

fn main() {
    let mut input = String::new();
    let answer = match std::io::stdin().read_to_string(&mut input) {
        Err(e) => Err(format!("cannot read the request: {e}")),
        Ok(_) => match serde_json::from_str::<duckle_delta::Request>(&input) {
            Err(e) => Err(format!("not a valid request: {e}")),
            Ok(req) => match tokio::runtime::Runtime::new() {
                Err(e) => Err(format!("cannot start the runtime: {e}")),
                Ok(rt) => rt.block_on(duckle_delta::run(req)),
            },
        },
    };
    match answer {
        Ok(outcome) => {
            let mut v = serde_json::to_value(outcome).unwrap_or_default();
            v["ok"] = true.into();
            println!("{v}");
        }
        Err(error) => {
            println!("{}", serde_json::json!({ "ok": false, "error": error }));
            std::process::exit(1);
        }
    }
}
