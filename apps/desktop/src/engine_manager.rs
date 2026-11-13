//! Engine installation manager.
//!
//! Duckle ships a tiny shell and downloads its execution engines on
//! first launch into the app-data directory, rather than statically
//! bundling them. DuckDB and SlothDB install through one shared path:
//! fetch the platform's release archive from GitHub, extract the binary,
//! mark it executable, and verify it runs.

use serde::Serialize;
use std::io::Read;
use std::path::{Path, PathBuf};

pub const DUCKDB_VERSION: &str = "2.0.0";
pub const SLOTHDB_VERSION: &str = "0.2.7";
/// Pinned llama.cpp build. Bump periodically; the GGUF wire format
/// is stable so newer server binaries keep working with older models.
/// Note: assets at older builds use a different naming (avx/avx2/cuda
/// flavors) - keep this on a recent build that ships the `*-cpu-*`
/// universal variant.
pub const LLAMACPP_BUILD: &str = "b9305";
/// A GGUF chat model the assistant can be installed with.
///
/// The catalogue is curated rather than a live Hugging Face search: every
/// entry is a repo + filename that has been checked to resolve, so the picker
/// cannot offer something that 404s halfway through a multi-gigabyte download.
/// All are Q4_K_M quantisations, which is the size/quality knee for local use.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct LlamaModel {
    /// Stable id stored in settings. Never reuse one for a different file.
    pub id: &'static str,
    pub label: &'static str,
    pub repo: &'static str,
    pub file: &'static str,
    /// The Hugging Face commit the file is fetched at: a fixed revision rather
    /// than `main`, which moves, so the bytes behind the URL match its pin.
    pub revision: &'static str,
    /// Real download size, from the Hugging Face file listing.
    pub size_mb: u32,
    /// What this choice costs and buys, in the user's terms.
    pub note: &'static str,
}

/// Models offered at install time, smallest first.
///
/// Bigger is not automatically better here: the assistant runs on the user's
/// own machine, so a 14B model on a laptop with no GPU offload is slower than
/// it is useful. The notes say so rather than leaving it to be discovered
/// after an 8.5 GB download.
pub const LLAMA_MODELS: &[LlamaModel] = &[
    LlamaModel {
        id: "qwen2.5-coder-0.5b",
        label: "Qwen2.5 Coder 0.5B",
        repo: "Qwen/Qwen2.5-Coder-0.5B-Instruct-GGUF",
        file: "qwen2.5-coder-0.5b-instruct-q4_k_m.gguf",
        revision: "ebb2015119c907b064c512bf053e945850b5875f",
        size_mb: 469,
        note: "Smallest option. For low-RAM or older machines; expect rough answers.",
    },
    LlamaModel {
        id: "llama-3.2-1b",
        label: "Llama 3.2 1B Instruct",
        repo: "bartowski/Llama-3.2-1B-Instruct-GGUF",
        file: "Llama-3.2-1B-Instruct-Q4_K_M.gguf",
        revision: "067b946cf014b7c697f3654f621d577a3e3afd1c",
        size_mb: 770,
        note: "Very fast and light. Better at plain English than at pipeline JSON.",
    },
    LlamaModel {
        id: "qwen2.5-coder-1.5b",
        label: "Qwen2.5 Coder 1.5B",
        repo: "Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF",
        file: "qwen2.5-coder-1.5b-instruct-q4_k_m.gguf",
        revision: "f86cb2c1fa58255f8052cc32aeede1b7482d4361",
        size_mb: 1066,
        note: "Default. Runs on any modern laptop CPU. Best choice if you are unsure.",
    },
    LlamaModel {
        id: "llama-3.2-3b",
        label: "Llama 3.2 3B Instruct",
        repo: "bartowski/Llama-3.2-3B-Instruct-GGUF",
        file: "Llama-3.2-3B-Instruct-Q4_K_M.gguf",
        revision: "5ab33fa94d1d04e903623ae72c95d1696f09f9e8",
        size_mb: 1926,
        note: "Stronger at plain English, weaker at code than the Qwen models.",
    },
    LlamaModel {
        id: "qwen2.5-coder-3b",
        label: "Qwen2.5 Coder 3B",
        repo: "Qwen/Qwen2.5-Coder-3B-Instruct-GGUF",
        file: "qwen2.5-coder-3b-instruct-q4_k_m.gguf",
        revision: "f74adce6aa16316c625447af059dbebe4983757c",
        size_mb: 2007,
        note: "Noticeably better pipeline generation. Comfortable on 16 GB of RAM.",
    },
    LlamaModel {
        id: "phi-3.5-mini",
        label: "Phi-3.5 Mini",
        repo: "bartowski/Phi-3.5-mini-instruct-GGUF",
        file: "Phi-3.5-mini-instruct-Q4_K_M.gguf",
        revision: "6d70da17e749a471ccb62ade694486011a75cda3",
        size_mb: 2282,
        note: "Strong reasoning for its size. Good all-rounder on a mid-range laptop.",
    },
    LlamaModel {
        id: "qwen3-4b",
        label: "Qwen3 4B Instruct",
        repo: "unsloth/Qwen3-4B-Instruct-2507-GGUF",
        file: "Qwen3-4B-Instruct-2507-Q4_K_M.gguf",
        revision: "a06e946bb6b655725eafa393f4a9745d460374c9",
        size_mb: 2382,
        note: "Newer generation. Best quality per gigabyte in the mid sizes.",
    },
    LlamaModel {
        id: "mistral-7b",
        label: "Mistral 7B Instruct v0.3",
        repo: "bartowski/Mistral-7B-Instruct-v0.3-GGUF",
        file: "Mistral-7B-Instruct-v0.3-Q4_K_M.gguf",
        revision: "61fd4167fff3ab01ee1cfe0da183fa27a944db48",
        size_mb: 4170,
        note: "Well-rounded general model. Wants 16 GB of RAM or GPU offload.",
    },
    LlamaModel {
        id: "qwen2.5-coder-7b",
        label: "Qwen2.5 Coder 7B",
        repo: "Qwen/Qwen2.5-Coder-7B-Instruct-GGUF",
        file: "qwen2.5-coder-7b-instruct-q4_k_m.gguf",
        revision: "13fb94bfda8c8cf22497dc57b78f391a9acb426a",
        size_mb: 4466,
        note: "The best code model here that still fits a 8 GB GPU. Slow on CPU alone.",
    },
    LlamaModel {
        id: "qwen2.5-7b",
        label: "Qwen2.5 7B Instruct",
        repo: "bartowski/Qwen2.5-7B-Instruct-GGUF",
        file: "Qwen2.5-7B-Instruct-Q4_K_M.gguf",
        revision: "8911e8a47f92bac19d6f5c64a2e2095bd2f7d031",
        size_mb: 4466,
        note: "General-purpose sibling of the 7B coder. Better prose, weaker code.",
    },
    LlamaModel {
        id: "qwen3-8b",
        label: "Qwen3 8B",
        repo: "unsloth/Qwen3-8B-GGUF",
        file: "Qwen3-8B-Q4_K_M.gguf",
        revision: "a6adef130ffb23ddaf1a62fec9dced968c9bc482",
        size_mb: 4795,
        note: "Newer generation at 8B. Wants a GPU with 8 GB or more.",
    },
    LlamaModel {
        id: "gemma-2-9b",
        label: "Gemma 2 9B",
        repo: "bartowski/gemma-2-9b-it-GGUF",
        file: "gemma-2-9b-it-Q4_K_M.gguf",
        revision: "d731033f3dc4018261fd39896e50984d398b4ac5",
        size_mb: 5494,
        note: "Google's 9B. Strong general answers; needs real GPU memory.",
    },
    LlamaModel {
        id: "qwen2.5-coder-14b",
        label: "Qwen2.5 Coder 14B",
        repo: "Qwen/Qwen2.5-Coder-14B-Instruct-GGUF",
        file: "qwen2.5-coder-14b-instruct-q4_k_m.gguf",
        revision: "d0a692ef765eefbf2fabb130b3cb2e8917e3d225",
        size_mb: 8572,
        note: "Only worth it with a GPU that has 10 GB or more of memory.",
    },
    LlamaModel {
        id: "deepseek-coder-v2-lite",
        label: "DeepSeek Coder V2 Lite",
        repo: "bartowski/DeepSeek-Coder-V2-Lite-Instruct-GGUF",
        file: "DeepSeek-Coder-V2-Lite-Instruct-Q4_K_M.gguf",
        revision: "8f248fa2072348f77a8bc37754e470de1f61866e",
        size_mb: 9884,
        note: "Largest option. A mixture-of-experts coder; needs 12 GB of GPU memory.",
    },
];

/// The model used when the user has not chosen one.
pub const DEFAULT_LLAMA_MODEL_ID: &str = "qwen2.5-coder-1.5b";

/// Look a model up by id, falling back to the default.
///
/// An unknown id resolves to the default rather than failing: a settings file
/// written by a newer build, or a catalogue entry we later retire, should not
/// leave the assistant uninstallable.
pub fn llama_model(id: Option<&str>) -> &'static LlamaModel {
    let want = id.unwrap_or(DEFAULT_LLAMA_MODEL_ID);
    LLAMA_MODELS
        .iter()
        .find(|m| m.id == want)
        .or_else(|| LLAMA_MODELS.iter().find(|m| m.id == DEFAULT_LLAMA_MODEL_ID))
        .unwrap_or(&LLAMA_MODELS[0])
}

/// Static description of an installable engine.
struct EngineSpec {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    required: bool,
    repo: &'static str,
    version: &'static str,
    /// Binary base name (without the .exe suffix).
    binary: &'static str,
}

const DUCKDB: EngineSpec = EngineSpec {
    id: "duckdb",
    name: "DuckDB",
    description: "Default engine - local analytics, file formats, SQL.",
    required: true,
    repo: "duckdb/duckdb",
    version: DUCKDB_VERSION,
    binary: "duckdb",
};

const SLOTHDB: EngineSpec = EngineSpec {
    id: "slothdb",
    name: "SlothDB",
    description: "Optional embedded engine. Downloads from the SlothDB releases.",
    required: false,
    repo: "SouravRoy-ETL/slothdb",
    version: SLOTHDB_VERSION,
    binary: "slothdb",
};

/// llama.cpp HTTP server + a small Qwen GGUF model. Treated as an
/// optional "engine" for UX consistency with the setup screen but
/// powers the Duckie AI Assistant chat panel rather than the SQL
/// execution path.
const LLAMACPP: EngineSpec = EngineSpec {
    id: "llamacpp",
    name: "Duckie AI Assistant",
    description: "Local chat assistant via llama.cpp + Qwen 1.5B. Downloads ~1.1 GB; runs entirely offline once installed.",
    required: false,
    // Repo moved from ggerganov to ggml-org in mid-2025; use the new
    // org path directly to skip the 301 redirect.
    repo: "ggml-org/llama.cpp",
    version: LLAMACPP_BUILD,
    binary: "llama-server",
};

const ENGINES: [&EngineSpec; 3] = [&DUCKDB, &SLOTHDB, &LLAMACPP];

fn spec(id: &str) -> Option<&'static EngineSpec> {
    ENGINES.iter().copied().find(|e| e.id == id)
}

fn binary_file_name(s: &EngineSpec) -> String {
    if cfg!(windows) {
        format!("{}.exe", s.binary)
    } else {
        s.binary.to_string()
    }
}

/// Marks an artifact with no recorded SHA-256. Nothing unpinned is installed.
pub(crate) const UNPINNED: &str = "UNPINNED";

/// SHA-256 of every artifact Duckle downloads, by the exact URL it requests
/// (#288).
///
/// The URL names the tool, its version, the platform and the asset, so a
/// version bump asks for a URL with no pin here, and the install refuses until
/// one is recorded. `every_artifact_a_supported_platform_can_download_is_pinned`
/// walks every URL each supported platform can request, so that refusal happens
/// in CI rather than on a user's machine. Every URL names a fixed version or
/// revision: a pin against a moving one (`latest`, `main`) would only turn a
/// silent substitution into a failed install.
///
/// Recorded from what the publishers publish, never by downloading: GitHub's
/// release-asset digest, the `.sha256` dbt puts beside each archive, and the
/// sha256 Hugging Face reports for each file.
const PINS: &[(&str, &str)] = &[
    ("https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-windows-amd64.tar.gz", "91663c2f36c2a334b0e5e639006d8c29c29e013af0c477d80bb931df68870312"),
    ("https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-windows-arm64.tar.gz", "4b5d1e4611debf676a693a8a6896dc29b352a9c3b11b706e7421d1d6082e05fd"),
    ("https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-linux-amd64.tar.gz", "c6507206baf7793c6013dbd82368469a45f20dc0e17a6f92f204602f652ed2bc"),
    ("https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-linux-arm64.tar.gz", "859e1875d2a9515fbb4bf8871061437a14af82320da68c753f094c4b776348fe"),
    ("https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-osx-universal.tar.gz", "8a8f67e7c7f7d1392fa63ba3971948391392fe2a25fca63e7008e33c50beb4c2"),
    ("https://github.com/SouravRoy-ETL/slothdb/releases/download/v0.2.7/slothdb.exe", "4e82163a38a281abc5a4c8acedcacf5d8e27b473eb37b3f2c4f5f7de9fc8cfad"),
    ("https://github.com/SouravRoy-ETL/slothdb/releases/download/v0.2.7/slothdb-linux-x64", "6976509996e486aa3b2044dde222e759e02b7a2f13b1a068b6eefdf59bfbd3f0"),
    ("https://github.com/SouravRoy-ETL/slothdb/releases/download/v0.2.7/slothdb-macos", "85738d640d10ce4a2f373a721481424c5651ed44db97620ab407a9abde112940"),
    ("https://github.com/ggml-org/llama.cpp/releases/download/b9305/llama-b9305-bin-win-cpu-x64.zip", "bc247f4779981db6abef123448b104b1e7a9fe386899a5b47ac36bbf4d6d9c0d"),
    ("https://github.com/ggml-org/llama.cpp/releases/download/b9305/llama-b9305-bin-win-cpu-arm64.zip", "5feca290e2ac798e02b60533f93c154583e25ef0519ac792d07e9bb26d08fd3a"),
    ("https://github.com/ggml-org/llama.cpp/releases/download/b9305/llama-b9305-bin-ubuntu-x64.tar.gz", "0a32013a7cca1c51652bba69799d793886372ec922dbc3aa0788a1f45a7059ba"),
    ("https://github.com/ggml-org/llama.cpp/releases/download/b9305/llama-b9305-bin-ubuntu-arm64.tar.gz", "06b1bb12fc77a5c6842c61f1720c672ec755387492a775d5d2aeb07b5a54bd22"),
    ("https://github.com/ggml-org/llama.cpp/releases/download/b9305/llama-b9305-bin-macos-arm64.tar.gz", "3c8bae495555472bbbcc2051fd8f8af2fbe060a537494cc2f33051e2f226e69a"),
    ("https://github.com/ggml-org/llama.cpp/releases/download/b9305/llama-b9305-bin-macos-x64.tar.gz", "74ddbcae1012759fa9e8ce81a28135df575db7141ea1f2881d06eadf912f72ca"),
    ("https://github.com/astral-sh/uv/releases/download/0.13.8/uv-x86_64-pc-windows-msvc.zip", "529e78a9cec6cd7e3e014f7ecd8bda0c113969d4ed0f2efeedb85c30943325b8"),
    ("https://github.com/astral-sh/uv/releases/download/0.13.8/uv-aarch64-pc-windows-msvc.zip", "59e6d919bed1f72035e622a7c930058f08a8931c4c78ceaf9f56e7fb74dd2c48"),
    ("https://github.com/astral-sh/uv/releases/download/0.13.8/uv-x86_64-unknown-linux-gnu.tar.gz", "5acfd9177cfecf0bf87578f059728215ccc884388988f55f7a65041b57387d91"),
    ("https://github.com/astral-sh/uv/releases/download/0.13.8/uv-aarch64-unknown-linux-gnu.tar.gz", "74f2f5c6024d397481db228db8fc25eed420db192243f88438c118e7e013e585"),
    ("https://github.com/astral-sh/uv/releases/download/0.13.8/uv-x86_64-apple-darwin.tar.gz", "a04c2ffeac28b2003a2c13caf538ab425fcd7def3e4a41571f4a8fb370a66af3"),
    ("https://github.com/astral-sh/uv/releases/download/0.13.8/uv-aarch64-apple-darwin.tar.gz", "6b71028a6b904f2ae3340ef52e595512de807d06c0a20eaef2f692581b246558"),
    ("https://public.cdn.getdbt.com/fs/cli/fs-v2.0.11-x86_64-pc-windows-msvc.zip", "48a4bdba30b84716313e1cc8298643484a3daa3d5f16bf7e941fb0b9e1fb0572"),
    ("https://public.cdn.getdbt.com/fs/cli/fs-v2.0.11-x86_64-unknown-linux-gnu.tar.gz", "773c6c195e0928c42cdd18be125aa42a837ed4f14c69dff70cd25e65c4018556"),
    ("https://public.cdn.getdbt.com/fs/cli/fs-v2.0.11-aarch64-unknown-linux-gnu.tar.gz", "5fea14ea629f3232a793847d20f3ba76b15ff05b26c3bad75e2ea01108d626ec"),
    ("https://public.cdn.getdbt.com/fs/cli/fs-v2.0.11-x86_64-apple-darwin.tar.gz", "f0b49a56b54686d56f2160ef76ec78bd60a21d9f60fe7fa2e43a298d4f4ba7f7"),
    ("https://public.cdn.getdbt.com/fs/cli/fs-v2.0.11-aarch64-apple-darwin.tar.gz", "99838a3a20b4ad688c8fce5fb557ca1c0126749e4c50ab294cfc2988b6e52f1b"),
    ("https://huggingface.co/Qwen/Qwen2.5-Coder-0.5B-Instruct-GGUF/resolve/ebb2015119c907b064c512bf053e945850b5875f/qwen2.5-coder-0.5b-instruct-q4_k_m.gguf", "1d9614638d18024d0fbb36575a15f1302a3adf044df10345688ec4f6e1c4ff32"),
    ("https://huggingface.co/bartowski/Llama-3.2-1B-Instruct-GGUF/resolve/067b946cf014b7c697f3654f621d577a3e3afd1c/Llama-3.2-1B-Instruct-Q4_K_M.gguf", "6f85a640a97cf2bf5b8e764087b1e83da0fdb51d7c9fab7d0fece9385611df83"),
    ("https://huggingface.co/Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF/resolve/f86cb2c1fa58255f8052cc32aeede1b7482d4361/qwen2.5-coder-1.5b-instruct-q4_k_m.gguf", "cc324af070c2ecbfd324a30884d2f951a7ff756aba85cb811a6ec436933bb046"),
    ("https://huggingface.co/bartowski/Llama-3.2-3B-Instruct-GGUF/resolve/5ab33fa94d1d04e903623ae72c95d1696f09f9e8/Llama-3.2-3B-Instruct-Q4_K_M.gguf", "6c1a2b41161032677be168d354123594c0e6e67d2b9227c84f296ad037c728ff"),
    ("https://huggingface.co/Qwen/Qwen2.5-Coder-3B-Instruct-GGUF/resolve/f74adce6aa16316c625447af059dbebe4983757c/qwen2.5-coder-3b-instruct-q4_k_m.gguf", "724fb256bec1ff062b2f65e4569e871ad2e95ab2a3989723d1769c54294730b7"),
    ("https://huggingface.co/bartowski/Phi-3.5-mini-instruct-GGUF/resolve/6d70da17e749a471ccb62ade694486011a75cda3/Phi-3.5-mini-instruct-Q4_K_M.gguf", "e4165e3a71af97f1b4820da61079826d8752a2088e313af0c7d346796c38eff5"),
    ("https://huggingface.co/unsloth/Qwen3-4B-Instruct-2507-GGUF/resolve/a06e946bb6b655725eafa393f4a9745d460374c9/Qwen3-4B-Instruct-2507-Q4_K_M.gguf", "3605803b982cb64aead44f6c1b2ae36e3acdb41d8e46c8a94c6533bc4c67e597"),
    ("https://huggingface.co/bartowski/Mistral-7B-Instruct-v0.3-GGUF/resolve/61fd4167fff3ab01ee1cfe0da183fa27a944db48/Mistral-7B-Instruct-v0.3-Q4_K_M.gguf", "1270d22c0fbb3d092fb725d4d96c457b7b687a5f5a715abe1e818da303e562b6"),
    ("https://huggingface.co/Qwen/Qwen2.5-Coder-7B-Instruct-GGUF/resolve/13fb94bfda8c8cf22497dc57b78f391a9acb426a/qwen2.5-coder-7b-instruct-q4_k_m.gguf", "509287f78cb4d4cf6b3843734733b914b2c158e43e22a7f4bf5e963800894d3c"),
    ("https://huggingface.co/bartowski/Qwen2.5-7B-Instruct-GGUF/resolve/8911e8a47f92bac19d6f5c64a2e2095bd2f7d031/Qwen2.5-7B-Instruct-Q4_K_M.gguf", "65b8fcd92af6b4fefa935c625d1ac27ea29dcb6ee14589c55a8f115ceaaa1423"),
    ("https://huggingface.co/unsloth/Qwen3-8B-GGUF/resolve/a6adef130ffb23ddaf1a62fec9dced968c9bc482/Qwen3-8B-Q4_K_M.gguf", "120307ba529eb2439d6c430d94104dabd578497bc7bfe7e322b5d9933b449bd4"),
    ("https://huggingface.co/bartowski/gemma-2-9b-it-GGUF/resolve/d731033f3dc4018261fd39896e50984d398b4ac5/gemma-2-9b-it-Q4_K_M.gguf", "13b2a7b4115bbd0900162edcebe476da1ba1fc24e718e8b40d32f6e300f56dfe"),
    ("https://huggingface.co/Qwen/Qwen2.5-Coder-14B-Instruct-GGUF/resolve/d0a692ef765eefbf2fabb130b3cb2e8917e3d225/qwen2.5-coder-14b-instruct-q4_k_m.gguf", "c1e659736d89ac1065fb495330fb824d94001974a4bfa78e7270e43476a8d940"),
    ("https://huggingface.co/bartowski/DeepSeek-Coder-V2-Lite-Instruct-GGUF/resolve/8f248fa2072348f77a8bc37754e470de1f61866e/DeepSeek-Coder-V2-Lite-Instruct-Q4_K_M.gguf", "603bd3f8a0281d16571da7c08bd661ee17ff0d1be6fcbd1b42242da257ef0bb8"),
];

/// The pinned SHA-256 for `url`, or [`UNPINNED`].
pub(crate) fn pinned_sha256(url: &str) -> &'static str {
    PINS.iter().find(|(u, _)| *u == url).map(|(_, d)| *d).unwrap_or(UNPINNED)
}

/// Where an engine's release asset is published.
fn engine_url(s: &EngineSpec, asset: &str) -> String {
    // Tag naming convention varies per upstream: DuckDB + SlothDB
    // both use v-prefixed semver tags (v1.5.3); llama.cpp uses raw
    // build tags (b9305). Pre-prepending `v` to every version
    // produces a 404 against ggml-org/llama.cpp.
    let tag = if s.id == "llamacpp" { s.version.to_string() } else { format!("v{}", s.version) };
    format!("https://github.com/{}/releases/download/{}/{}", s.repo, tag, asset)
}

/// Where a model file is published, at its pinned revision.
pub(crate) fn model_url(m: &LlamaModel) -> String {
    format!("https://huggingface.co/{}/resolve/{}/{}", m.repo, m.revision, m.file)
}

/// An artifact's bytes, from a local folder or from the network.
pub(crate) struct Artifact {
    pub(crate) reader: Box<dyn Read + Send>,
    /// The size, when it is known before reading.
    pub(crate) len: Option<u64>,
}

/// Downloads are forbidden (#288): a production machine provisioned in
/// advance, which must never fetch a runtime for itself.
pub(crate) fn offline() -> bool {
    std::env::var("DUCKLE_OFFLINE")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

/// Open an artifact by the URL it is published at (#288).
///
/// A file named like the URL's last segment in `DUCKLE_ARTIFACT_DIR` is used
/// with no network at all, which is how an air-gapped machine installs.
/// Otherwise it is downloaded, unless `DUCKLE_OFFLINE` forbids the network, and
/// then the error names the file to put in that folder. The caller checks a
/// local file against the same pin as a download.
pub(crate) fn open_artifact(client: &reqwest::blocking::Client, url: &str) -> Result<Artifact, String> {
    let name = url.rsplit('/').next().unwrap_or(url);
    let dir = std::env::var_os("DUCKLE_ARTIFACT_DIR").filter(|d| !d.is_empty()).map(PathBuf::from);
    if let Some(path) = dir.as_ref().map(|d| d.join(name)).filter(|p| p.is_file()) {
        let file = std::fs::File::open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let len = file.metadata().ok().map(|m| m.len());
        return Ok(Artifact { reader: Box::new(file), len });
    }
    if offline() {
        let folder = match &dir {
            Some(d) => format!("the folder DUCKLE_ARTIFACT_DIR names ({})", d.display()),
            None => "a folder and point DUCKLE_ARTIFACT_DIR at it".to_string(),
        };
        return Err(format!(
            "offline (DUCKLE_OFFLINE), so {name} is not downloaded. Put the file published at {url} in {folder}."
        ));
    }
    let resp = client.get(url).send().map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {} from {url}", resp.status().as_u16()));
    }
    let len = resp.content_length();
    Ok(Artifact { reader: Box::new(resp), len })
}

pub(crate) fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Compare a computed digest against the pinned one.
///
/// Fails closed: a mismatch is an error, never a warning, because the artifact is
/// about to be executed - and so is an artifact with no pin at all. The caller must delete any partially written file before
/// propagating, or the next run finds it on disk and skips the download entirely.
pub(crate) fn verify_download(label: &str, actual_hex: &str, expected: &str) -> Result<(), String> {
    if expected == UNPINNED {
        return Err(format!(
            "{label} has no pinned checksum, so it was not installed: nothing but TLS would vouch for it. Record its SHA-256 in PINS in engine_manager.rs."
        ));
    }
    if !actual_hex.eq_ignore_ascii_case(expected) {
        return Err(format!(
            "{label} failed checksum verification and was discarded. Expected {expected}, got {actual_hex}. Either the download was tampered with, or the pinned digest is stale after a version bump."
        ));
    }
    Ok(())
}

fn engine_dir(app_data: &Path, s: &EngineSpec) -> PathBuf {
    app_data.join("engines").join(s.id)
}

fn binary_path(app_data: &Path, s: &EngineSpec) -> PathBuf {
    engine_dir(app_data, s).join(binary_file_name(s))
}

/// A small file recording which version of an engine's binary is installed,
/// written on install. Without it, status() can only check that *a* binary
/// exists, so a version bump (e.g. DuckDB 1.5.3 -> 1.5.4) would keep the stale
/// binary and never re-download. Reading the stamp lets status() detect the
/// mismatch and re-run the one-click install over the old binary.
fn version_stamp_path(app_data: &Path, s: &EngineSpec) -> PathBuf {
    engine_dir(app_data, s).join(".installed-version")
}

/// The version recorded on disk for an engine, if any (None for a stamp-less
/// pre-existing install or a missing engine).
fn installed_version(app_data: &Path, s: &EngineSpec) -> Option<String> {
    std::fs::read_to_string(version_stamp_path(app_data, s))
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Public helper kept for the engine() resolver in lib.rs.
pub fn duckdb_path(app_data: &Path) -> PathBuf {
    binary_path(app_data, &DUCKDB)
}

/// Path the AI assistant server binary lands at.
pub fn llamacpp_path(app_data: &Path) -> PathBuf {
    binary_path(app_data, &LLAMACPP)
}

/// Path the GGUF model file lands at (sibling of the binary).
///
/// Resolved by looking for whichever `.gguf` is actually in the engine
/// directory rather than by name, so the chat server starts against whatever
/// the user chose at install time. Falls back to the default model's filename
/// when nothing is installed yet, which is the path the installer writes to.
pub fn llama_model_path(app_data: &Path) -> PathBuf {
    let dir = engine_dir(app_data, &LLAMACPP);
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let mut found: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("gguf"))
            .collect();
        // Deterministic when a previous model was left behind: the newest wins,
        // and ties break by name so the choice never flips between launches.
        found.sort();
        if let Some(newest) = found
            .iter()
            .max_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok())
        {
            return newest.clone();
        }
    }
    dir.join(llama_model(None).file)
}

/// Release asset name for this OS/arch, or None if unsupported.
fn asset_for(s: &EngineSpec) -> Option<String> {
    asset_for_platform(s, std::env::consts::OS, std::env::consts::ARCH)
}

/// An engine's release asset for an OS/arch, so the pin check can walk every
/// platform rather than only the one it runs on.
fn asset_for_platform(s: &EngineSpec, os: &str, arch: &str) -> Option<String> {
    match s.id {
        "duckdb" => Some(
            match (os, arch) {
                ("windows", "x86_64") => "duckdb-cli-windows-amd64.tar.gz",
                ("windows", "aarch64") => "duckdb-cli-windows-arm64.tar.gz",
                ("linux", "x86_64") => "duckdb-cli-linux-amd64.tar.gz",
                ("linux", "aarch64") => "duckdb-cli-linux-arm64.tar.gz",
                ("macos", _) => "duckdb-cli-osx-universal.tar.gz",
                _ => return None,
            }
            .to_string(),
        ),
        // SlothDB ships raw, single-file binaries per its releases -
        // not zips. Names per https://github.com/SouravRoy-ETL/slothdb.
        "slothdb" => Some(
            match (os, arch) {
                ("windows", _) => "slothdb.exe",
                ("linux", "x86_64") => "slothdb-linux-x64",
                ("macos", _) => "slothdb-macos",
                _ => return None,
            }
            .to_string(),
        ),
        // llama.cpp ships pre-built binaries per OS/arch. We pick the
        // most-compatible variant (no GPU acceleration) so the model
        // runs on any CPU - the chat assistant only needs ~5 tok/s.
        // Windows ships as zip; Linux + macOS as tar.gz.
        "llamacpp" => Some(
            match (os, arch) {
                ("windows", "x86_64") => format!("llama-{}-bin-win-cpu-x64.zip", LLAMACPP_BUILD),
                ("windows", "aarch64") => format!("llama-{}-bin-win-cpu-arm64.zip", LLAMACPP_BUILD),
                ("linux", "x86_64") => format!("llama-{}-bin-ubuntu-x64.tar.gz", LLAMACPP_BUILD),
                ("linux", "aarch64") => format!("llama-{}-bin-ubuntu-arm64.tar.gz", LLAMACPP_BUILD),
                ("macos", "aarch64") => format!("llama-{}-bin-macos-arm64.tar.gz", LLAMACPP_BUILD),
                ("macos", _) => format!("llama-{}-bin-macos-x64.tar.gz", LLAMACPP_BUILD),
                _ => return None,
            },
        ),
        _ => None,
    }
}

/// DuckDB CLI release asset name for an arbitrary OS/arch (not necessarily the
/// host). Used to fetch a cross-target DuckDB when "Build Pipeline" targets a
/// different OS than the one Duckle runs on.
fn duckdb_asset(os: &str, arch: &str) -> Option<&'static str> {
    Some(match (os, arch) {
        ("windows", "x86_64") => "duckdb-cli-windows-amd64.tar.gz",
        ("windows", "aarch64") => "duckdb-cli-windows-arm64.tar.gz",
        ("linux", "x86_64") => "duckdb-cli-linux-amd64.tar.gz",
        ("linux", "aarch64") => "duckdb-cli-linux-arm64.tar.gz",
        ("macos", _) => "duckdb-cli-osx-universal.tar.gz",
        _ => return None,
    })
}

/// Resolve a DuckDB CLI binary for a DIFFERENT target than the host, used to
/// assemble a cross-OS "Build Pipeline" artifact. Downloads the official
/// DuckDB release archive (same pinned DUCKDB_VERSION as the host engine) for the
/// requested os/arch and caches the extracted binary under
/// `engines/duckdb-cross/<os>-<arch>/duckdb(.exe)`. Returns the cached path.
///
/// The downloaded binary is for the TARGET OS, so the host cannot execute it;
/// it is only ever copied into the artifact payload. Its exec bit is set at
/// run time when the artifact self-extracts on the target (see selfextract).
pub fn ensure_cross_duckdb(app_data: &Path, os: &str, arch: &str) -> Result<PathBuf, String> {
    let asset = duckdb_asset(os, arch)
        .ok_or_else(|| format!("No DuckDB build for {}-{}", os, arch))?;
    let bin_name = if os == "windows" { "duckdb.exe" } else { "duckdb" };
    let dir = app_data
        .join("engines")
        .join("duckdb-cross")
        .join(format!("{}-{}", os, arch));
    let target = dir.join(bin_name);
    if target.exists() {
        return Ok(target);
    }
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    let url = engine_url(&DUCKDB, asset);
    let client = reqwest::blocking::Client::builder()
        .user_agent("duckle")
        .use_preconfigured_tls(duckle_duckdb_engine::tls::build_client_config())
        .build()
        .map_err(|e| e.to_string())?;
    let mut fetched = open_artifact(&client, &url)
        .map_err(|e| format!("Couldn't download DuckDB {} for {}-{}: {}", DUCKDB_VERSION, os, arch, e))?;
    let expected = fetched.len;
    let mut bytes = Vec::new();
    fetched.reader.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
    // Reject a truncated transfer before baking the binary into a shipped
    // artifact: a short read here would otherwise produce a corrupt bundled
    // duckdb that only fails on the target. A DuckDB CLI archive is multi-MB, so a
    // tiny body also signals an error/redirect page slipped through.
    if let Some(expected) = expected {
        if (bytes.len() as u64) != expected {
            return Err(format!(
                "DuckDB download for {}-{} was truncated ({} of {} bytes)",
                os,
                arch,
                bytes.len(),
                expected
            ));
        }
    }
    if bytes.len() < 1_000_000 {
        return Err(format!(
            "DuckDB download for {}-{} is implausibly small ({} bytes); aborting",
            os,
            arch,
            bytes.len()
        ));
    }
    // Before the archive is opened, let alone the binary extracted and run.
    verify_download("the DuckDB CLI download", &hex_sha256(&bytes), pinned_sha256(&url))?;

    // DuckDB CLI archives ship a single self-contained binary named duckdb(.exe).
    if !extract_tar_gz(&bytes, &dir, &target, bin_name, bin_name)? {
        return Err("DuckDB binary not found inside the downloaded archive".to_string());
    }
    Ok(target)
}

#[derive(Debug, Serialize)]
pub struct EngineStatus {
    pub id: String,
    pub name: String,
    pub description: String,
    pub required: bool,
    pub installed: bool,
    /// The version currently on disk (None when no binary is present).
    pub version: Option<String>,
    /// The version this build of Duckle pins / ships. The UI compares it to
    /// `version` to offer an upgrade rather than a fresh install.
    pub target_version: String,
    /// A binary is present but its version differs from `target_version`, i.e.
    /// an upgrade is available (distinct from a missing engine).
    pub outdated: bool,
    pub path: Option<String>,
    pub available: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum InstallProgress {
    Downloading { received: u64, total: Option<u64> },
    Extracting,
    Verifying,
    /// Per-extension progress for the DuckDB extension pre-install step
    /// that runs after the engine binary lands. Fetching them up front
    /// means the first time a fresh user touches a Postgres source or an
    /// S3 file there is no network hop.
    InstallingExtension { name: String, index: u32, total: u32 },
    /// Model-file download phase, used only by the llamacpp engine.
    /// The model is much larger than the binary (~1.1 GB vs ~50 MB)
    /// so we report its progress separately for clearer UX.
    DownloadingModel { received: u64, total: Option<u64> },
    Done { path: String },
}

/// DuckDB extensions Duckle uses or is wired to use. Pre-installed once
/// at first launch so future ATTACH / read_xlsx / httpfs calls do not
/// stop to download an extension mid-run.
const DUCKDB_EXTENSIONS: &[&str] = &[
    "httpfs",   // S3 / GCS / HTTP(S) URLs
    "azure",    // Azure Blob native
    "sqlite",   // SQLite ATTACH
    "postgres", // PostgreSQL ATTACH
    "mysql",    // MySQL / MariaDB ATTACH
    "excel",    // .xlsx reader
    "iceberg",  // Apache Iceberg table scan + write (v1.5+)
    "delta",    // Delta Lake table scan
    "ducklake", // DuckLake: DuckDB-native lakehouse catalog
    "vss",      // Vector similarity search (array_* distance funcs)
    "fts",      // Full-text search (BM25 keyword scoring)
    // avro is deliberately absent, and not for the reason this comment used to
    // give: it does publish for the pinned version, and src.avro is `available`
    // rather than preview. src.avro reads through the pure-Rust apache-avro
    // crate (connectors.rs run_avro_source), so it needs no extension at all -
    // verified by running an avro pipeline against an empty extension cache.
    // The only path that reaches DuckDB's avro is the format dispatch used by
    // inspect, and `read_avro` autoloads on demand, which a bare `LOAD` does
    // not.
];

fn duckdb_command(bin: &Path) -> std::process::Command {
    let mut cmd = std::process::Command::new(bin);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NO_WINDOW: suppress the console flash on Windows.
        cmd.creation_flags(0x0800_0000);
    }
    // -no-init: never process a user's ~/.duckdbrc. An init file that prints
    // output would pollute version/platform probes and extension installs.
    cmd.arg("-no-init");
    cmd
}

/// #91: ask the DuckDB binary its actual version (it prints e.g.
/// "v1.5.4 19864453f7" on the first line). Only duckdb is assumed to support
/// `--version` reliably. Used as a fallback when the install stamp is
/// missing/stale so a genuine pinned-version binary - placed by an older build,
/// an in-app self-update, or a manual drop - is not falsely flagged outdated.
fn probed_version(bin: &Path, s: &EngineSpec) -> Option<String> {
    if s.id != "duckdb" {
        return None;
    }
    let out = duckdb_command(bin).arg("--version").output().ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .map(|t| t.trim_start_matches('v').to_string())
        .filter(|v| !v.is_empty())
}

/// Walk through every DuckDB extension Duckle needs, INSTALL+LOADing each
/// so the file lands in the user's local DuckDB extension cache. Failures
/// are logged via the progress callback but never abort the engine
/// install: a user offline for one extension still gets a working engine
/// and the rest of the extensions; the missing one will autoload (or
/// fail loudly) the first time it's actually used.
fn install_duckdb_extensions<F: FnMut(InstallProgress)>(bin: &Path, on_progress: &mut F) {
    // Offline, the extensions come with the machine's provisioning: DuckDB
    // fetching them itself is a download like any other (#288).
    if offline() {
        return;
    }
    let total = DUCKDB_EXTENSIONS.len() as u32;
    for (i, ext) in DUCKDB_EXTENSIONS.iter().enumerate() {
        on_progress(InstallProgress::InstallingExtension {
            name: (*ext).to_string(),
            index: (i as u32) + 1,
            total,
        });
        let sql = format!("INSTALL {ext}; LOAD {ext};");
        // Best-effort: ignore the result; the next step (or a later run)
        // will retry. Don't let one slow / unreachable extension block
        // the whole engine install.
        let _ = duckdb_command(bin)
            .arg(":memory:")
            .arg("-c")
            .arg(&sql)
            .output();
    }
}

/// DuckDB's status when `DUCKLE_DUCKDB_BIN` points at a usable binary.
///
/// #373: the desktop runs whatever `resolve_duckdb_bin` returns, and an
/// externally-set `DUCKLE_DUCKDB_BIN` wins there (#179). First-run setup asked
/// the engine store instead, which knows nothing about the override, so it
/// blocked on a download for a DuckDB the app would actually run. This mirrors
/// `pick_duckdb_bin`'s precedence so the gate and the runtime agree.
///
/// The override is reported as installed and NEVER outdated: the user pointed
/// Duckle at this binary, and the runtime uses it whatever its version, so
/// offering to "upgrade" it by downloading over the bundled path - which the
/// runtime would then ignore - would be a lie. A blank value, or a path to
/// nothing, returns None so the normal engines-dir check still runs.
fn duckdb_override_status(env_override: Option<&std::ffi::OsStr>) -> Option<EngineStatus> {
    let path = env_override.filter(|v| !v.is_empty()).map(PathBuf::from)?;
    if !path.exists() {
        return None;
    }
    Some(EngineStatus {
        id: DUCKDB.id.to_string(),
        name: DUCKDB.name.to_string(),
        description: DUCKDB.description.to_string(),
        required: DUCKDB.required,
        installed: true,
        outdated: false,
        version: probed_version(&path, &DUCKDB),
        target_version: DUCKDB.version.to_string(),
        path: Some(path.to_string_lossy().to_string()),
        available: asset_for(&DUCKDB).is_some(),
    })
}

pub fn status(app_data: &Path, duckdb_override: Option<std::ffi::OsString>) -> Vec<EngineStatus> {
    ENGINES
        .iter()
        .map(|s| {
            // #373: an externally-provided DuckDB satisfies the required
            // engine, since that is the one a run would use.
            if s.id == DUCKDB.id {
                if let Some(st) = duckdb_override_status(duckdb_override.as_deref()) {
                    return st;
                }
            }
            let path = binary_path(app_data, s);
            let exists = path.exists();
            let on_disk = installed_version(app_data, s);
            // #91: trust the install stamp as the fast path, but when it is
            // absent/stale fall back to the binary's own reported version, so a
            // genuine pinned-version binary without a stamp is not falsely
            // flagged outdated (the spurious "upgrade DuckDB 1.5.4" banner).
            let effective = if on_disk.is_some() {
                on_disk.clone()
            } else if exists {
                probed_version(&path, s)
            } else {
                None
            };
            // Backfill the stamp when the probe confirms the pinned version, so
            // subsequent calls hit the fast path and skip re-spawning the binary.
            if exists
                && on_disk.as_deref() != Some(s.version)
                && effective.as_deref() == Some(s.version)
            {
                let _ = std::fs::write(version_stamp_path(app_data, s), s.version);
            }
            // "installed" requires the binary to exist AND match the pinned
            // version, so bumping a version re-triggers the install flow.
            let installed = exists && effective.as_deref() == Some(s.version);
            // A binary is present but a different version: an upgrade is due.
            let outdated = exists && effective.as_deref() != Some(s.version);
            EngineStatus {
                id: s.id.to_string(),
                name: s.name.to_string(),
                description: s.description.to_string(),
                required: s.required,
                installed,
                // Report the real on-disk version when a binary is present
                // (so the UI shows the outdated version, not the pinned one).
                version: if exists { effective } else { None },
                target_version: s.version.to_string(),
                outdated,
                path: exists.then(|| path.to_string_lossy().to_string()),
                available: asset_for(s).is_some(),
            }
        })
        .collect()
}

/// Download + install any engine by id. Streams progress.
/// `model_id` picks the GGUF for the chat assistant; it is ignored by every
/// other engine, and None means the default model.
pub fn install<F: FnMut(InstallProgress)>(
    app_data: &Path,
    engine_id: &str,
    model_id: Option<&str>,
    on_progress: F,
) -> Result<String, String> {
    let s = spec(engine_id).ok_or_else(|| format!("Unknown engine '{}'", engine_id))?;
    install_spec(app_data, s, model_id, on_progress)
}

/// macOS (#89): make a freshly downloaded engine dir launchable. Downloaded
/// llama.cpp release binaries are signed by ggml's identity but not notarized;
/// on macOS 15+ the leftover `com.apple.quarantine` / `com.apple.provenance`
/// xattrs plus the non-notarized signature get the process SIGKILL'd on exec,
/// which surfaces as the llama-server "didn't become ready" timeout. Clearing
/// ALL xattrs (not just quarantine - `com.apple.provenance` also gates
/// Gatekeeper re-evaluation) and ad-hoc re-signing the Mach-O files (dylibs
/// first, then the executable, with no hardened-runtime flag so library
/// validation won't reject the ad-hoc dylibs) makes them launchable. Writes a
/// marker so `ensure_macos_launchable` can skip the work on later launches. Only
/// the main binary failing to sign is fatal - it would not run at all. No-op off
/// macOS (guarded at run time so the code still type-checks on every platform).
pub(crate) fn macos_prepare_engine(dir: &Path, main_binary: &Path) -> Result<(), String> {
    if !cfg!(target_os = "macos") {
        return Ok(());
    }
    let _ = std::process::Command::new("xattr")
        .arg("-cr")
        .arg(dir)
        .output();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "dylib").unwrap_or(false) {
                let _ = adhoc_sign(&p);
            }
        }
    }
    adhoc_sign(main_binary)?;
    let _ = std::fs::write(dir.join(MACOS_PREPARED_MARKER), "1");
    Ok(())
}

/// macOS (#89): ad-hoc code-sign one Mach-O file. `--sign -` produces a local
/// signature with no hardened-runtime flag, so the file runs on this machine
/// without notarization and won't enforce library validation against a team id.
fn adhoc_sign(path: &Path) -> Result<(), String> {
    let out = std::process::Command::new("codesign")
        .args(["--force", "--sign", "-", "--timestamp=none"])
        .arg(path)
        .output()
        .map_err(|e| format!("run codesign on {}: {}", path.display(), e))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "codesign {}: {}",
            path.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

const MACOS_PREPARED_MARKER: &str = ".duckle-macos-prepared";

/// macOS (#89): ensure an already-installed engine dir was made launchable,
/// repairing installs done by a Duckle build that predated the signing fix (or
/// one whose install-time signing was too weak). Marker-gated so it does the
/// clear + re-sign at most once per install. No-op off macOS. Call this right
/// before launching a downloaded binary.
pub(crate) fn ensure_macos_launchable(dir: &Path, main_binary: &Path) -> Result<(), String> {
    if !cfg!(target_os = "macos") || dir.join(MACOS_PREPARED_MARKER).exists() {
        return Ok(());
    }
    macos_prepare_engine(dir, main_binary)
}

/// Extract a .tar.gz engine archive into `dir`, flattening every entry to its
/// leaf name so the binary keeps its sibling shared libraries. Returns whether
/// the wanted binary was found (it is written atomically to `target`, since
/// status() keys off that path existing).
///
/// Link entries need real handling. A tar stores a symlink's target in the
/// HEADER with an empty body, so copying the entry writes a 0-byte regular
/// file. That was issue #89: llama.cpp ships `libllama.0.0.9305.dylib` plus the
/// `libllama.0.dylib` / `libllama.dylib` symlinks its binaries actually link
/// against, and those landed as empty files. dyld then reported
/// `Library not loaded: @rpath/libllama-common.0.dylib ... tried: '<path>' ()`,
/// where the empty parentheses mean the file was found but was not a valid
/// Mach-O. No amount of re-signing could fix that, because there was nothing to
/// sign; the bytes were never written.
fn extract_tar_gz(
    buf: &[u8],
    dir: &Path,
    target: &Path,
    want: &str,
    binary: &str,
) -> Result<bool, String> {
    let gz = flate2::read::GzDecoder::new(std::io::Cursor::new(buf));
    let mut archive = tar::Archive::new(gz);
    let mut extracted = false;
    // Links are replayed after the regular files, because a tar may list a
    // link before the entry it points at.
    let mut links: Vec<(String, String)> = Vec::new();
    for entry in archive.entries().map_err(|e| e.to_string())? {
        let mut entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path().map_err(|e| e.to_string())?.to_path_buf();
        let leaf = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        if entry.header().entry_type().is_dir() || leaf.is_empty() {
            continue;
        }
        let et = entry.header().entry_type();
        if et.is_symlink() || et.is_hard_link() {
            let target_leaf = entry
                .link_name()
                .ok()
                .flatten()
                .and_then(|p| p.file_name().map(|f| f.to_string_lossy().to_string()))
                .unwrap_or_default();
            if !target_leaf.is_empty() && target_leaf != leaf {
                links.push((leaf, target_leaf));
            }
            continue;
        }
        let is_target_binary =
            leaf.eq_ignore_ascii_case(want) || leaf.eq_ignore_ascii_case(binary);
        if is_target_binary {
            // Atomic for the binary status() keys off of.
            copy_atomic(&mut entry, target)?;
            extracted = true;
        } else {
            let out_path = dir.join(&leaf);
            let mut out = std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
            std::io::copy(&mut entry, &mut out).map_err(|e| e.to_string())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ =
                    std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(0o755));
            }
        }
    }
    // Recreate the links now every real file is on disk. All of llama.cpp's
    // links are same-directory, so the flattened leaf is a valid relative
    // target.
    for (link_leaf, target_leaf) in links {
        let link_path = dir.join(&link_leaf);
        if !dir.join(&target_leaf).exists() {
            // Nothing to point at: leave what is there rather than replacing a
            // real file with a dangling link.
            continue;
        }
        let _ = std::fs::remove_file(&link_path);
        #[cfg(unix)]
        {
            if let Err(e) = std::os::unix::fs::symlink(&target_leaf, &link_path) {
                return Err(format!(
                    "could not link {} -> {}: {}",
                    link_leaf, target_leaf, e
                ));
            }
        }
        #[cfg(not(unix))]
        {
            // Windows needs a privilege for symlinks, so copy instead. The
            // duplicated bytes are harmless and the loader is satisfied.
            std::fs::copy(dir.join(&target_leaf), &link_path).map_err(|e| {
                format!("could not copy {} -> {}: {}", target_leaf, link_leaf, e)
            })?;
        }
    }
    Ok(extracted)
}

fn install_spec<F: FnMut(InstallProgress)>(
    app_data: &Path,
    s: &EngineSpec,
    model_id: Option<&str>,
    mut on_progress: F,
) -> Result<String, String> {
    let asset = asset_for(s).ok_or_else(|| {
        format!(
            "No {} build for {}-{}",
            s.name,
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;
    let url = engine_url(s, &asset);

    let dir = engine_dir(app_data, s);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    let client = reqwest::blocking::Client::builder()
        .user_agent("duckle")
        // Trust the OS store (+ optional DUCKLE_CA_CERT) on top of the bundled
        // roots so the engine download works behind a TLS-inspecting proxy.
        .use_preconfigured_tls(duckle_duckdb_engine::tls::build_client_config())
        .build()
        .map_err(|e| e.to_string())?;
    let mut fetched = open_artifact(&client, &url)
        .map_err(|e| format!("Couldn't download {} {}: {}", s.name, s.version, e))?;
    let total = fetched.len;
    let mut buf: Vec<u8> = Vec::with_capacity(total.unwrap_or(0) as usize);
    let mut chunk = [0u8; 64 * 1024];
    let mut received: u64 = 0;
    on_progress(InstallProgress::Downloading { received: 0, total });
    loop {
        let n = fetched.reader.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        received += n as u64;
        on_progress(InstallProgress::Downloading { received, total });
    }

    // Before extraction. llama.cpp's archive yields a server binary plus the shared
    // libraries it dlopens, so a substituted archive is arbitrary code either way.
    verify_download(
        &format!("the {} download", s.name),
        &hex_sha256(&buf),
        pinned_sha256(&url),
    )?;

    let target = binary_path(app_data, s);

    let lower = asset.to_ascii_lowercase();
    if lower.ends_with(".zip") {
        on_progress(InstallProgress::Extracting);
        let want = binary_file_name(s);
        let reader = std::io::Cursor::new(buf);
        let mut archive = zip::ZipArchive::new(reader).map_err(|e| e.to_string())?;
        let mut extracted = false;
        // llama.cpp's zip ships the server binary alongside several
        // shared libraries (llama.dll, ggml.dll, ...) that the binary
        // dlopens at runtime - we have to extract them too. DuckDB
        // ships a single self-contained binary; the targeted extract
        // path stays for it.
        let extract_all = s.id == "llamacpp";
        for i in 0..archive.len() {
            let mut file = archive.by_index(i).map_err(|e| e.to_string())?;
            let name = file.name().map_err(|e| e.to_string())?.into_owned();
            let leaf = name.rsplit('/').next().unwrap_or(&name).to_string();
            if file.is_dir() || leaf.is_empty() {
                continue;
            }
            let is_target_binary =
                leaf.eq_ignore_ascii_case(&want) || leaf.eq_ignore_ascii_case(s.binary);
            if extract_all {
                if is_target_binary {
                    // Write the binary status() keys off of atomically so a
                    // crash mid-extract can't leave a partial "installed" file.
                    copy_atomic(&mut file, &target)?;
                    extracted = true;
                } else {
                    let out_path = dir.join(&leaf);
                    let mut out =
                        std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
                    std::io::copy(&mut file, &mut out).map_err(|e| e.to_string())?;
                }
            } else if is_target_binary {
                copy_atomic(&mut file, &target)?;
                extracted = true;
                break;
            }
        }
        if !extracted {
            return Err(format!(
                "{} binary not found inside the downloaded archive",
                s.name
            ));
        }
    } else if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        // DuckDB (from 2.0, every platform) and llama.cpp's Linux + macOS
        // releases ship as tar.gz. Same
        // semantics as the llamacpp zip branch: extract every file
        // to the engine dir so the binary keeps its sibling .so / .dylib.
        on_progress(InstallProgress::Extracting);
        let want = binary_file_name(s);
        let extracted = extract_tar_gz(&buf, &dir, &target, &want, s.binary)?;
        if !extracted {
            return Err(format!(
                "{} binary not found inside the downloaded tarball",
                s.name
            ));
        }
    } else {
        // Raw single-file binary (SlothDB) - the download IS the binary.
        if buf.is_empty() {
            return Err(format!("{} download was empty", s.name));
        }
        // Reject a truncated transfer, then install atomically so a partial
        // binary never lands at the final path (status() would call it
        // installed).
        if let Some(t) = total {
            if (buf.len() as u64) < t {
                return Err(format!(
                    "{} download truncated ({} of {} bytes); try again",
                    s.name,
                    buf.len(),
                    t
                ));
            }
        }
        write_atomic(&target, &buf)?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755));
    }

    // macOS (#89): clear Gatekeeper xattrs + ad-hoc re-sign the freshly
    // downloaded binaries so the kernel allows execution. A broken signature on
    // llama-server only surfaces later as an opaque "didn't become ready"
    // timeout, so it is fatal here; for the self-contained DB engines it stays
    // best-effort (they run regardless).
    if let Err(e) = macos_prepare_engine(&dir, &target) {
        if s.id == "llamacpp" {
            return Err(format!(
                "Installed {} but could not sign it for macOS: {}",
                s.name, e
            ));
        }
    }

    // Verify the binary landed and is non-empty. Probing --version is
    // best-effort: DuckDB supports it; we don't assume every engine does,
    // so a non-zero --version isn't fatal as long as the file is there.
    on_progress(InstallProgress::Verifying);
    let bytes = std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0);
    if bytes == 0 {
        return Err(format!("Installed {} binary is empty", s.name));
    }
    let _ = duckdb_command(&target).arg("--version").output();

    // Stamp the installed version so status() detects a future version bump
    // and re-installs instead of keeping a stale binary.
    let _ = std::fs::write(version_stamp_path(app_data, s), s.version);

    // The host binary above was overwritten in place, but a version bump also
    // leaves the previous version's cached cross-OS DuckDB binaries stale
    // (engines/duckdb-cross/, used by Build Pipeline). They are NOT version-
    // keyed and short-circuit on existence, so without this they would never be
    // re-fetched. Drop the whole cache so the next Build Pipeline downloads the
    // matching version - and so an old (e.g. 1.5.3) binary is not left behind
    // in the app storage directory. Best-effort.
    if s.id == "duckdb" {
        let cross = app_data.join("engines").join("duckdb-cross");
        let _ = std::fs::remove_dir_all(&cross);
    }

    // Pre-fetch the extensions Duckle uses so the first connector hit
    // doesn't pause to download an extension. Only meaningful for the
    // DuckDB engine; SlothDB has its own model.
    if s.id == "duckdb" {
        install_duckdb_extensions(&target, &mut on_progress);
    }

    // llama.cpp's binary alone is useless without a model. Fetch the chosen
    // GGUF from HuggingFace right after the binary lands.
    if s.id == "llamacpp" {
        install_llama_model(app_data, model_id, &mut on_progress)?;
    }

    let path = target.to_string_lossy().to_string();
    on_progress(InstallProgress::Done { path: path.clone() });
    Ok(path)
}

/// A unique temp sibling of `target` for atomic download/extract: write here,
/// then rename into place so a truncated / crash-interrupted file never appears
/// at the final path (where status()/idempotency checks would treat it as
/// installed).
fn part_path(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(format!(".part{}", std::process::id()));
    target.with_file_name(name)
}

/// Rename a fully-written temp file into `target` (exec perms on unix first);
/// removes the temp on failure so a partial never lingers.
fn finalize_download(tmp: &Path, target: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(tmp, std::fs::Permissions::from_mode(0o755));
    }
    std::fs::rename(tmp, target).map_err(|e| {
        let _ = std::fs::remove_file(tmp);
        format!("finalize {}: {}", target.display(), e)
    })
}

/// Write `bytes` to a temp sibling, then rename into `target` atomically.
fn write_atomic(target: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = part_path(target);
    if let Err(e) = std::fs::write(&tmp, bytes) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.to_string());
    }
    finalize_download(&tmp, target)
}

/// Copy a reader to a temp sibling, then rename into `target` atomically.
fn copy_atomic(reader: &mut impl std::io::Read, target: &Path) -> Result<(), String> {
    let tmp = part_path(target);
    let res = (|| -> Result<(), String> {
        let mut out = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
        std::io::copy(reader, &mut out).map_err(|e| e.to_string())?;
        Ok(())
    })();
    if let Err(e) = res {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    finalize_download(&tmp, target)
}

/// Download the chosen GGUF model file into the llamacpp engine dir.
/// Separate phase from the binary download so the UI can show "stage
/// 2 of 2" instead of one big progress bar for both. HuggingFace
/// supports range requests; we just stream sequentially for simplicity.
///
/// `model_id` is the id the user picked at install time; an unknown or absent
/// one falls back to the default rather than failing.
fn install_llama_model<F: FnMut(InstallProgress)>(
    app_data: &Path,
    model_id: Option<&str>,
    on_progress: &mut F,
) -> Result<(), String> {
    let model = llama_model(model_id);
    // Target by exact filename, not by whatever .gguf happens to be present:
    // picking a different model must download it, not silently keep the old one.
    let target = engine_dir(app_data, &LLAMACPP).join(model.file);
    // Idempotent: if this model is already there and non-empty, skip.
    if let Ok(meta) = std::fs::metadata(&target) {
        if meta.len() > 1_000_000 {
            return Ok(());
        }
    }
    let url = model_url(model);
    let client = reqwest::blocking::Client::builder()
        .user_agent("duckle")
        // No global timeout - the model is over a GB on home internet.
        .timeout(None)
        // Same merged trust store as the engine download (OS + bundled roots).
        .use_preconfigured_tls(duckle_duckdb_engine::tls::build_client_config())
        .build()
        .map_err(|e| e.to_string())?;
    let mut fetched = open_artifact(&client, &url).map_err(|e| {
        format!("Couldn't download the {} model: {e}. Hugging Face may be rate-limiting; try again in a minute.", model.label)
    })?;
    let total = fetched.len;
    on_progress(InstallProgress::DownloadingModel { received: 0, total });
    // Stream to a temp sibling, validate, then rename into place - so a
    // truncated or interrupted download never lands at the model path where the
    // idempotency check above would treat it as fully installed.
    let tmp = part_path(&target);
    let mut out = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
    let mut chunk = [0u8; 256 * 1024];
    let mut received: u64 = 0;
    // Hashed as it streams: the file is over a gigabyte, so it is never held in
    // memory and cannot be hashed afterwards without reading it back.
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    let validated = (|| -> Result<(), String> {
        loop {
            let n = fetched.reader.read(&mut chunk).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            sha2::Digest::update(&mut hasher, &chunk[..n]);
            std::io::Write::write_all(&mut out, &chunk[..n]).map_err(|e| e.to_string())?;
            received += n as u64;
            on_progress(InstallProgress::DownloadingModel { received, total });
        }
        std::io::Write::flush(&mut out).map_err(|e| e.to_string())?;
        // Truncated transfer: the server declared more bytes than arrived.
        if let Some(t) = total {
            if received < t {
                return Err(format!(
                    "model download truncated ({} of {} bytes); try again",
                    received, t
                ));
            }
        }
        // GGUF files start with the magic bytes "GGUF".
        if received < 4 {
            return Err("model download too small to be a GGUF file".into());
        }
        let mut header = [0u8; 4];
        let mut f = std::fs::File::open(&tmp).map_err(|e| e.to_string())?;
        std::io::Read::read_exact(&mut f, &mut header)
            .map_err(|e| format!("read model header: {}", e))?;
        if &header != b"GGUF" {
            return Err("Downloaded model is not a valid GGUF file (header mismatch)".into());
        }
        // Inside `validated`, so a mismatch takes the existing path that deletes the
        // partial file. Leaving it on disk would be worse than not checking at all:
        // the next run finds the target present and skips the download entirely.
        let digest: String = sha2::Digest::finalize(hasher)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        verify_download(
            &format!("the {} model download", model.file),
            &digest,
            pinned_sha256(&url),
        )?;
        Ok(())
    })();
    drop(out); // close the handle before rename (Windows)
    if let Err(e) = validated {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    finalize_download(&tmp, &target)
}

/// DUCKLE_ARTIFACT_DIR and DUCKLE_OFFLINE are process-wide, so the tests that set
/// them, here and in dbt_engine, take turns.
#[cfg(test)]
pub(crate) static DOWNLOAD_ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A wrong file in a fresh artifact folder, offline, so an install can only find
/// the planted bytes, and nothing reaches the network.
#[cfg(test)]
pub(crate) fn plant_wrong_artifact(name: &str, len: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(name), vec![7u8; len]).unwrap();
    std::env::set_var("DUCKLE_ARTIFACT_DIR", dir.path());
    std::env::set_var("DUCKLE_OFFLINE", "1");
    dir
}

#[cfg(test)]
pub(crate) fn clear_artifact_env() {
    std::env::remove_var("DUCKLE_ARTIFACT_DIR");
    std::env::remove_var("DUCKLE_OFFLINE");
}

#[cfg(test)]
mod download_verification_tests {
    use super::*;

    /// A known vector, so a change to the hex formatting cannot pass unnoticed.
    #[test]
    fn hex_sha256_matches_a_known_vector() {
        assert_eq!(
            hex_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// The whole point: an artifact that does not match its pin must be refused,
    /// because the next thing that happens to it is execution.
    #[test]
    fn a_mismatched_digest_is_refused() {
        let real = hex_sha256(b"the real installer");
        let swapped = hex_sha256(b"the attacker's installer");
        let err = verify_download("test artifact", &swapped, &real)
            .expect_err("a substituted artifact must be refused");
        assert!(err.contains("failed checksum verification"), "unhelpful: {err}");

        verify_download("test artifact", &real, &real).expect("the real artifact must pass");
    }

    /// Digests get pasted from tools that emit upper case.
    #[test]
    fn digest_comparison_ignores_case() {
        let d = hex_sha256(b"x");
        verify_download("test artifact", &d.to_uppercase(), &d).unwrap();
    }

    /// #288: nothing unpinned is installed. Only TLS would vouch for it.
    #[test]
    fn an_unpinned_artifact_is_refused() {
        let err = verify_download("test artifact", &hex_sha256(b"anything"), UNPINNED)
            .expect_err("an unpinned artifact must not be installed");
        assert!(err.contains("no pinned checksum"), "unhelpful: {err}");
    }

    /// Every URL any supported platform can download, through the same
    /// functions the installs build them with.
    fn every_download_url() -> Vec<String> {
        const PLATFORMS: [(&str, &str); 6] = [
            ("windows", "x86_64"),
            ("windows", "aarch64"),
            ("linux", "x86_64"),
            ("linux", "aarch64"),
            ("macos", "x86_64"),
            ("macos", "aarch64"),
        ];
        let mut urls: Vec<String> = Vec::new();
        for (os, arch) in PLATFORMS {
            for s in ENGINES {
                if let Some(asset) = asset_for_platform(s, os, arch) {
                    urls.push(engine_url(s, &asset));
                }
            }
            if let Some(asset) = duckdb_asset(os, arch) {
                urls.push(engine_url(&DUCKDB, asset));
            }
            urls.extend(crate::dbt_engine::uv_url_for(os, arch));
            urls.extend(crate::dbt_engine::fusion_url_for(os, arch));
        }
        urls.extend(LLAMA_MODELS.iter().map(model_url));
        urls.sort();
        urls.dedup();
        urls
    }

    /// #288: a version bump without its digests fails here, in CI, rather than
    /// as a refused install on somebody's machine.
    #[test]
    fn every_artifact_a_supported_platform_can_download_is_pinned() {
        let urls = every_download_url();
        let unpinned: Vec<&String> = urls.iter().filter(|u| pinned_sha256(u) == UNPINNED).collect();
        assert!(unpinned.is_empty(), "no pin for {unpinned:#?}");
        assert!(urls.len() >= 39, "only {} download URLs found - the walk is broken", urls.len());
    }

    /// And the other way: a pin no download asks for is a stale version's,
    /// left behind by a bump.
    #[test]
    fn every_pin_is_for_a_download_something_makes() {
        let urls = every_download_url();
        let stale: Vec<&str> = PINS.iter().map(|(u, _)| *u).filter(|u| !urls.iter().any(|x| x == u)).collect();
        assert!(stale.is_empty(), "pins nothing downloads: {stale:#?}");
    }

    use super::DOWNLOAD_ENV as ENV;

    /// #288: an air-gapped machine installs from a folder. Port 9 has nothing
    /// listening, so getting the bytes at all proves no request was made.
    #[test]
    fn a_file_in_the_artifact_folder_is_used_without_the_network() {
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("tool.tar.gz"), b"local bytes").unwrap();
        std::env::set_var("DUCKLE_ARTIFACT_DIR", dir.path());
        std::env::set_var("DUCKLE_OFFLINE", "1");
        let got = open_artifact(&reqwest::blocking::Client::new(), "http://127.0.0.1:9/releases/tool.tar.gz");
        std::env::remove_var("DUCKLE_ARTIFACT_DIR");
        std::env::remove_var("DUCKLE_OFFLINE");
        let mut fetched = got.expect("served from the folder");
        let mut bytes = Vec::new();
        fetched.reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"local bytes");
        assert_eq!(fetched.len, Some(11));
    }

    /// #288: offline, nothing is fetched, and the error says what to provide.
    #[test]
    fn offline_refuses_to_download_and_names_the_file_to_provide() {
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("DUCKLE_ARTIFACT_DIR");
        std::env::set_var("DUCKLE_OFFLINE", "1");
        let got = open_artifact(&reqwest::blocking::Client::new(), "http://127.0.0.1:9/releases/tool.tar.gz");
        std::env::remove_var("DUCKLE_OFFLINE");
        let err = got.err().expect("an offline machine downloads nothing");
        assert!(err.contains("tool.tar.gz") && err.contains("DUCKLE_ARTIFACT_DIR"), "{err}");
    }

    /// #288: DuckDB fetches its own extensions, so offline it is not asked to.
    #[test]
    fn offline_does_not_fetch_duckdb_extensions() {
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var("DUCKLE_OFFLINE", "1");
        let mut events = 0;
        install_duckdb_extensions(Path::new("no-such-duckdb"), &mut |_| events += 1);
        std::env::remove_var("DUCKLE_OFFLINE");
        assert_eq!(events, 0, "an offline install asked DuckDB for its extensions");
    }

    /// #288: a file from the folder is held to the same pin a download is. A
    /// cross-target DuckDB is the build-a-bundle path; over 1 MB so it passes
    /// the size check and reaches the pin.
    #[test]
    fn the_cross_target_duckdb_refuses_a_wrong_file_from_the_folder() {
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let app = tempfile::tempdir().unwrap();
        let _dir = plant_wrong_artifact("duckdb-cli-linux-amd64.tar.gz", 1_100_000);
        let got = ensure_cross_duckdb(app.path(), "linux", "x86_64");
        clear_artifact_env();
        let err = got.expect_err("a file that does not match its pin is not installed");
        assert!(err.contains("failed checksum verification"), "{err}");
    }

    /// The same for an engine install, with this machine's asset.
    #[test]
    fn an_engine_install_refuses_a_wrong_file_from_the_folder() {
        let Some(asset) = asset_for(&SLOTHDB) else { return };
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let app = tempfile::tempdir().unwrap();
        let _dir = plant_wrong_artifact(&asset, 4096);
        let got = install(app.path(), "slothdb", None, |_| {});
        clear_artifact_env();
        let err = got.expect_err("a file that does not match its pin is not installed");
        assert!(err.contains("failed checksum verification"), "{err}");
    }

    /// The same for a Duckie model. GGUF magic first, so the header check passes
    /// and the pin is what refuses it; and nothing is left at the model path.
    #[test]
    fn a_model_install_refuses_a_wrong_file_from_the_folder() {
        let _env = ENV.lock().unwrap_or_else(|e| e.into_inner());
        let app = tempfile::tempdir().unwrap();
        let model = llama_model(None);
        // The engine's own install makes this folder before the model's.
        std::fs::create_dir_all(engine_dir(app.path(), &LLAMACPP)).unwrap();
        let dir = plant_wrong_artifact(model.file, 0);
        std::fs::write(dir.path().join(model.file), b"GGUF and then not the model").unwrap();
        let got = install_llama_model(app.path(), None, &mut |_| {});
        clear_artifact_env();
        let err = got.expect_err("a file that does not match its pin is not installed");
        assert!(err.contains("failed checksum verification"), "{err}");
        assert!(!engine_dir(app.path(), &LLAMACPP).join(model.file).exists());
    }
}

#[cfg(test)]
mod tar_extract_tests {
    use super::extract_tar_gz;
    use std::io::Write;

    /// Build a .tar.gz shaped like llama.cpp's macOS release: a real versioned
    /// dylib, the two symlinks that point at it, and the server binary. The
    /// symlink entries are written explicitly so this holds on any host
    /// filesystem, including one that cannot create symlinks.
    fn llamacpp_shaped_tarball() -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());

        let real = b"FAKE-MACHO-DYLIB-BYTES";
        let mut h = tar::Header::new_gnu();
        h.set_path("build/bin/libllama-common.0.0.9305.dylib").unwrap();
        h.set_size(real.len() as u64);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        builder.append(&h, &real[..]).unwrap();

        for link in ["libllama-common.0.dylib", "libllama-common.dylib"] {
            let mut h = tar::Header::new_gnu();
            h.set_size(0); // a tar symlink carries no body
            h.set_mode(0o755);
            h.set_entry_type(tar::EntryType::Symlink);
            h.set_cksum();
            builder
                .append_link(&mut h, format!("build/bin/{link}"), "libllama-common.0.0.9305.dylib")
                .unwrap();
        }

        let server = b"FAKE-SERVER-BINARY";
        let mut h = tar::Header::new_gnu();
        h.set_path("build/bin/llama-server").unwrap();
        h.set_size(server.len() as u64);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        builder.append(&h, &server[..]).unwrap();

        let tar_bytes = builder.into_inner().unwrap();
        let mut gz =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&tar_bytes).unwrap();
        gz.finish().unwrap()
    }

    /// Issue #89. The dylib symlinks used to land as 0-byte regular files,
    /// because a tar symlink has an empty body and the extractor copied the
    /// body. dyld then refused to load them and llama-server aborted, which
    /// looked like a code-signing failure and was not.
    #[test]
    fn dylib_symlinks_are_not_extracted_as_empty_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let target = dir.join("llama-server");
        let found =
            extract_tar_gz(&llamacpp_shaped_tarball(), dir, &target, "llama-server", "llama-server")
                .expect("extraction succeeds");
        assert!(found, "the wanted binary must be found");

        let real = dir.join("libllama-common.0.0.9305.dylib");
        assert_eq!(std::fs::read(&real).unwrap(), b"FAKE-MACHO-DYLIB-BYTES");

        for link in ["libllama-common.0.dylib", "libllama-common.dylib"] {
            let p = dir.join(link);
            assert!(p.exists(), "{link} must exist");
            let meta = std::fs::metadata(&p).unwrap(); // follows the link
            assert_ne!(
                meta.len(),
                0,
                "{link} resolved to 0 bytes, which is the #89 bug"
            );
            // Reading through the link must yield the real library's bytes,
            // whether it is a symlink (unix) or a copy (windows fallback).
            assert_eq!(
                std::fs::read(&p).unwrap(),
                b"FAKE-MACHO-DYLIB-BYTES",
                "{link} must resolve to the real dylib"
            );
        }
    }

    /// A link whose target is missing must not clobber anything or error the
    /// whole install.
    #[test]
    fn dangling_link_is_skipped_not_fatal() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_gnu();
        h.set_size(0);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Symlink);
        h.set_cksum();
        builder
            .append_link(&mut h, "bin/orphan.dylib", "nothing-here.dylib")
            .unwrap();
        let bin = b"BINARY";
        let mut h = tar::Header::new_gnu();
        h.set_path("bin/llama-server").unwrap();
        h.set_size(bin.len() as u64);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        builder.append(&h, &bin[..]).unwrap();
        let tar_bytes = builder.into_inner().unwrap();
        let mut gz =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&tar_bytes).unwrap();
        let buf = gz.finish().unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("llama-server");
        let found = extract_tar_gz(&buf, tmp.path(), &target, "llama-server", "llama-server")
            .expect("a dangling link must not fail the install");
        assert!(found);
        assert!(!tmp.path().join("orphan.dylib").exists());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_lists_all_engines_missing_in_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let st = status(tmp.path(), None);
        assert_eq!(st.len(), 3);
        let duck = st.iter().find(|e| e.id == "duckdb").unwrap();
        assert!(!duck.installed && duck.required && duck.available);
        let sloth = st.iter().find(|e| e.id == "slothdb").unwrap();
        assert!(!sloth.installed && !sloth.required);
        let llama = st.iter().find(|e| e.id == "llamacpp").unwrap();
        assert!(!llama.installed && !llama.required);
    }

    #[test]
    fn status_flags_outdated_when_stamp_differs() {
        // An existing user on an older DuckDB: the binary is present but the
        // version stamp differs from the pinned one. It must read as outdated
        // (upgrade available) and NOT installed, with both versions exposed so
        // the UI can prompt an upgrade rather than a fresh install.
        let tmp = tempfile::tempdir().unwrap();
        let dir = engine_dir(tmp.path(), &DUCKDB);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(binary_file_name(&DUCKDB)), b"old-binary").unwrap();
        std::fs::write(dir.join(".installed-version"), "0.0.1-old").unwrap();

        let st = status(tmp.path(), None);
        let duck = st.iter().find(|e| e.id == "duckdb").unwrap();
        assert!(!duck.installed, "an old version must not read as installed");
        assert!(duck.outdated, "an old version must read as outdated");
        assert_eq!(duck.version.as_deref(), Some("0.0.1-old"));
        assert_eq!(duck.target_version, DUCKDB.version);
    }

    /// #373: when DUCKLE_DUCKDB_BIN points at a real binary the desktop runs
    /// THAT (resolve_duckdb_bin / #179), so first-run setup must report DuckDB
    /// satisfied rather than block on a download. The engines dir is empty
    /// here; only the override makes it installed, and it is never outdated
    /// because the user chose it and the runtime uses it whatever its version.
    #[test]
    fn an_override_binary_satisfies_duckdb_without_a_download() {
        let tmp = tempfile::tempdir().unwrap(); // empty engines dir
        let ext = tempfile::tempdir().unwrap();
        let bin = ext.path().join(binary_file_name(&DUCKDB));
        std::fs::write(&bin, b"stand-in duckdb").unwrap();

        let st = status(tmp.path(), Some(bin.clone().into_os_string()));
        let duck = st.iter().find(|e| e.id == "duckdb").unwrap();
        assert!(duck.installed, "an override binary must satisfy the gate");
        assert!(!duck.outdated, "the user pointed us here; do not offer to replace it");
        assert_eq!(duck.path.as_deref(), Some(bin.to_string_lossy().as_ref()));
    }

    /// A blank value or a path to nothing is not an override: it falls through
    /// to the normal engines-dir check rather than passing the gate falsely.
    #[test]
    fn a_blank_or_missing_override_does_not_satisfy_duckdb() {
        let tmp = tempfile::tempdir().unwrap(); // empty engines dir
        for ov in [
            Some(std::ffi::OsString::new()),
            Some(std::ffi::OsString::from("/no/such/duckdb-xyz")),
            None,
        ] {
            let st = status(tmp.path(), ov);
            let duck = st.iter().find(|e| e.id == "duckdb").unwrap();
            assert!(!duck.installed, "a blank or missing override must not pass the gate");
        }
    }

    #[test]
    #[ignore = "downloads the DuckDB CLI from GitHub releases (network)"]
    fn installs_duckdb() {
        let tmp = tempfile::tempdir().unwrap();
        let path = install(tmp.path(), "duckdb", None, |_| {}).expect("install");
        assert!(std::path::Path::new(&path).exists());
        assert!(status(tmp.path(), None)
            .iter()
            .any(|e| e.id == "duckdb" && e.installed));
    }

    /// A Build Pipeline for another OS bundles that OS's DuckDB. From 2.0 every
    /// platform ships as a .tar.gz, the Windows build included.
    #[test]
    #[ignore = "downloads DuckDB CLIs from GitHub releases (network)"]
    fn fetches_a_cross_target_duckdb() {
        let tmp = tempfile::tempdir().unwrap();
        for (os, arch, magic) in [("linux", "x86_64", &b"\x7fELF"[..]), ("windows", "x86_64", &b"MZ"[..])] {
            let path = ensure_cross_duckdb(tmp.path(), os, arch).expect("the cross download");
            let head = std::fs::read(&path).unwrap();
            assert!(head.starts_with(magic), "{os}-{arch}: not an executable for that OS: {}", path.display());
        }
    }

    #[test]
    #[ignore = "downloads the SlothDB raw binary from GitHub releases (network)"]
    fn installs_slothdb() {
        let tmp = tempfile::tempdir().unwrap();
        let path = install(tmp.path(), "slothdb", None, |_| {}).expect("install");
        let p = std::path::Path::new(&path);
        assert!(p.exists(), "binary should exist");
        assert!(
            std::fs::metadata(p).unwrap().len() > 0,
            "binary should be non-empty"
        );
        assert!(status(tmp.path(), None)
            .iter()
            .any(|e| e.id == "slothdb" && e.installed));
    }
}
