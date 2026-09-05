#!/usr/bin/env python3
"""Generate duckle/_components.py from the component catalog.

Run from the repo (it needs crates/duckle-mcp/catalog.json and the Rust
sources). The generated module is committed, so building a wheel never needs
the repo checked out.

    python packaging/pypi/generate_components.py

Two things happen here.

1. The catalog is trimmed to what a Python caller needs: id, kind, summary and
   field keys. The full catalog is 1.39 MiB, most of it labels, placeholders
   and descriptions for rendering a GUI form. Parsing that on every
   `import duckle` would be a needless cost.

2. Every field key is checked against the Rust sources, and only keys that
   actually appear are advertised as named parameters. This matters more than
   it sounds. The catalog describes what the GUI *renders*, which is not always
   what the engine *reads*: all eight streaming sources advertise a full SASL
   block (saslUsername, saslPassword, security, ...) that no builder consumes.
   A generated `read_kafka(sasl_username=...)` would look like it authenticates
   and would not. Dead keys are still accepted and passed through, so nothing
   is blocked, but they are never presented as if they work.
"""

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
CATALOG = os.path.join(REPO, "crates", "duckle-mcp", "catalog.json")
OUT = os.path.join(HERE, "duckle", "_components.py")

# Keys that are structural rather than component configuration.
SKIP_KEYS = {"notes"}


def rust_string_literals():
    """Every double-quoted string literal in the Rust workspace.

    Deliberately permissive: a key counts as live if it appears anywhere in any
    .rs file. That over-accepts (a key used by a different component still
    counts) but never wrongly hides a working parameter, which is the error
    that would actually hurt.
    """
    lits = set()
    pattern = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"')
    for root, dirs, files in os.walk(os.path.join(REPO, "crates")):
        dirs[:] = [d for d in dirs if d not in ("target", "node_modules", ".git")]
        for fn in files:
            if not fn.endswith(".rs"):
                continue
            try:
                with open(os.path.join(root, fn), encoding="utf-8", errors="ignore") as fh:
                    for m in pattern.finditer(fh.read()):
                        lits.add(m.group(1))
            except OSError:
                pass
    return lits


def field_keys(component):
    manifest = component.get("manifest") or {}
    out = []
    for section in manifest.get("sections") or []:
        for field in section.get("fields") or []:
            # A note is guidance rendered on the form, not a setting, so it is
            # not a property the engine could read and must not be reported as
            # one the engine ignores.
            if field.get("kind") == "note":
                continue
            key = field.get("key")
            if key and key not in SKIP_KEYS and key not in out:
                out.append(key)
    return out


def main():
    if not os.path.exists(CATALOG):
        sys.exit("generate_components: {} not found; run this from the repo".format(CATALOG))
    catalog = json.load(open(CATALOG, encoding="utf-8"))
    live = rust_string_literals()
    print("scanned Rust sources: {:,} distinct string literals".format(len(live)))

    entries = {}
    total_fields = dead_total = 0
    for c in catalog["components"]:
        if c.get("availability") not in ("available", "preview"):
            continue  # do not offer components the engine cannot run
        keys = field_keys(c)
        known = [k for k in keys if k in live]
        dead = [k for k in keys if k not in live]
        total_fields += len(keys)
        dead_total += len(dead)
        entries[c["id"]] = {
            "kind": c.get("kind", ""),
            "summary": (c.get("summary") or "").strip(),
            "params": known,
            "unverified": dead,
        }

    lines = [
        '"""Generated from crates/duckle-mcp/catalog.json. Do not edit by hand.',
        "",
        "Regenerate with: python packaging/pypi/generate_components.py",
        '"""',
        "",
        "# id -> {kind, summary, params, unverified}",
        "#   params      keys confirmed present in the engine sources",
        "#   unverified  keys the catalog advertises that no Rust source mentions;",
        "#               still accepted and passed through, never suggested",
        "COMPONENTS = {",
    ]
    for cid in sorted(entries):
        e = entries[cid]
        lines.append("    {!r}: {{".format(cid))
        lines.append("        'kind': {!r},".format(e["kind"]))
        summary = e["summary"]
        if len(summary) > 300:
            summary = summary[:297] + "..."
        lines.append("        'summary': {!r},".format(summary))
        lines.append("        'params': {!r},".format(e["params"]))
        if e["unverified"]:
            lines.append("        'unverified': {!r},".format(e["unverified"]))
        lines.append("    },")
    lines.append("}")
    lines.append("")

    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines))

    size = os.path.getsize(OUT)
    print(
        "wrote {} components to {} ({:.0f} KB)".format(len(entries), os.path.relpath(OUT, REPO), size / 1024)
    )
    print(
        "fields: {:,} total, {:,} confirmed, {:,} unverified ({:.1f}%)".format(
            total_fields, total_fields - dead_total, dead_total,
            100.0 * dead_total / max(total_fields, 1),
        )
    )


if __name__ == "__main__":
    main()
