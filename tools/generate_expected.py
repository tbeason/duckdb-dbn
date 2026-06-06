"""Generate golden CSV files for each fixture in test/data/.

For every test_data.<schema>.dbn (or .dbn.zst), runs Rust dbn-cli to produce CSV
ground truth, then post-processes:
  - drops the rtype column (extension doesn't emit it)
  - reorders columns to match the extension's per-schema BindData declaration
  - renames dbn-cli's bid_px_00/etc. to match extension's bid_price/etc. for
    schemas where the extension uses BBO-style naming (bbo, cbbo, cmbp)
  - scales raw int64 price columns by 1e-9 to match the extension's DOUBLE prices
  - prepends a comment header recording the dbn-cli version

Run from the duckdb-dbn repo root:
    python tools/generate_expected.py

Outputs land in test/data/expected/. Re-run after changing fixtures.

Ground-truth source: Rust dbn-cli (cargo install dbn-cli).
"""
from __future__ import annotations
import csv
import io
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "test" / "data"
EXPECTED = DATA / "expected"
DBN_CLI = "dbn"  # resolved from PATH; falls back to ~/.cargo/bin/dbn.exe on Windows

# Per-schema column transforms. Each entry's keys:
#   "ext_columns" — column order the extension emits (from XxxBind in
#     src/dbn_extension.cpp). Names AFTER applying "column_renames".
#   "column_renames" — dbn-cli CSV column → extension column. Applied BEFORE
#     selecting "ext_columns". Optional.
#   "price_columns" — column names (after rename) whose value is a raw int64
#     in dbn-cli output and a DOUBLE (1e-9 scaled) in the extension.
#
# Schemas whose ext_columns is None: skipped pending verification of column
# layout — they pass through with `rtype` dropped.

# Helper: BBO/CBBO/CMBP rename single-level fields from MBP-style `_00` suffix
# to BBO-style suffixless.
BBO_RENAMES = {
    "bid_px_00": "bid_price", "ask_px_00": "ask_price",
    "bid_sz_00": "bid_size",  "ask_sz_00": "ask_size",
    "bid_ct_00": "bid_ct",    "ask_ct_00": "ask_ct",
    "bid_pb_00": "bid_pb",    "ask_pb_00": "ask_pb",
}

SCHEMAS: dict[str, dict] = {
    "trades": {
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "action", "side",
            "flags", "depth", "ts_in_delta", "sequence",
        ],
        "price_columns": ["price"],
    },
    "mbo": {
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "order_id", "price", "size", "channel_id",
            "action", "side", "flags", "ts_in_delta", "sequence",
        ],
        "price_columns": ["price"],
    },
    "mbp-1": {
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "action", "side",
            "flags", "depth", "ts_in_delta", "sequence",
            "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00",
            "bid_ct_00", "ask_ct_00",
        ],
        "price_columns": ["price", "bid_px_00", "ask_px_00"],
    },
    "tbbo": {
        # Wire-identical to mbp-1.
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "action", "side",
            "flags", "depth", "ts_in_delta", "sequence",
            "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00",
            "bid_ct_00", "ask_ct_00",
        ],
        "price_columns": ["price", "bid_px_00", "ask_px_00"],
    },
    "mbp-10": {
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "action", "side",
            "flags", "depth", "ts_in_delta", "sequence",
        ] + [
            col_pfx + f"_{i:02d}"
            for i in range(10)
            for col_pfx in ("bid_px", "ask_px", "bid_sz", "ask_sz", "bid_ct", "ask_ct")
        ],
        "price_columns": ["price"] + [
            f"{side}_px_{i:02d}" for i in range(10) for side in ("bid", "ask")
        ],
    },
    "bbo-1s": {
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "side", "flags",
            "bid_price", "ask_price", "bid_size", "ask_size",
            "bid_ct", "ask_ct", "sequence",
        ],
        "column_renames": BBO_RENAMES,
        "price_columns": ["price", "bid_price", "ask_price"],
    },
    "bbo-1m": {
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "side", "flags",
            "bid_price", "ask_price", "bid_size", "ask_size",
            "bid_ct", "ask_ct", "sequence",
        ],
        "column_renames": BBO_RENAMES,
        "price_columns": ["price", "bid_price", "ask_price"],
    },
    "cbbo-1s": {
        # 14 cols; no sequence.
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "side", "flags",
            "bid_price", "ask_price", "bid_size", "ask_size",
            "bid_pb", "ask_pb",
        ],
        "column_renames": BBO_RENAMES,
        "price_columns": ["price", "bid_price", "ask_price"],
    },
    "cbbo-1m": {
        # Same shape as cbbo-1s (CbboBind).
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "side", "flags",
            "bid_price", "ask_price", "bid_size", "ask_size",
            "bid_pb", "ask_pb",
        ],
        "column_renames": BBO_RENAMES,
        "price_columns": ["price", "bid_price", "ask_price"],
    },
    "tcbbo": {
        # Cmbp1Bind: 16 cols with bid_price/ask_price naming, no sequence.
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "action", "side",
            "flags", "ts_in_delta",
            "bid_price", "ask_price", "bid_size", "ask_size",
            "bid_pb", "ask_pb",
        ],
        "column_renames": BBO_RENAMES,
        "price_columns": ["price", "bid_price", "ask_price"],
    },
    "cmbp-1": {
        # 16 cols; no sequence.
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "action", "side",
            "flags", "ts_in_delta",
            "bid_price", "ask_price", "bid_size", "ask_size",
            "bid_pb", "ask_pb",
        ],
        "column_renames": BBO_RENAMES,
        "price_columns": ["price", "bid_price", "ask_price"],
    },
    "ohlcv-1s": {
        "ext_columns": ["ts_event", "instrument_id", "publisher_id",
                        "open", "high", "low", "close", "volume"],
        "price_columns": ["open", "high", "low", "close"],
    },
    "ohlcv-1m": {
        "ext_columns": ["ts_event", "instrument_id", "publisher_id",
                        "open", "high", "low", "close", "volume"],
        "price_columns": ["open", "high", "low", "close"],
    },
    "ohlcv-1h": {
        "ext_columns": ["ts_event", "instrument_id", "publisher_id",
                        "open", "high", "low", "close", "volume"],
        "price_columns": ["open", "high", "low", "close"],
    },
    "ohlcv-1d": {
        "ext_columns": ["ts_event", "instrument_id", "publisher_id",
                        "open", "high", "low", "close", "volume"],
        "price_columns": ["open", "high", "low", "close"],
    },
    "ohlcv-eod": {
        # Same shape as the other OHLCV variants (OhlcvBind).
        "ext_columns": ["ts_event", "instrument_id", "publisher_id",
                        "open", "high", "low", "close", "volume"],
        "price_columns": ["open", "high", "low", "close"],
    },
    "trades.empty": {
        # Wire-identical to trades; zero rows.
        "ext_columns": [
            "ts_event", "ts_recv", "instrument_id", "publisher_id",
            "price", "size", "action", "side",
            "flags", "depth", "ts_in_delta", "sequence",
        ],
        "price_columns": ["price"],
    },
    "status": {
        "ext_columns": None,  # discovery pass
        "price_columns": [],
    },
    "imbalance": {
        "ext_columns": None,
        "price_columns": [],
    },
    "statistics": {
        "ext_columns": None,
        "price_columns": ["price"],
    },
    "definition": {
        "ext_columns": None,
        "price_columns": [],
    },
}


def run_dbn_cli(fixture: Path) -> str:
    """Invoke dbn-cli on `fixture`, return raw CSV stdout."""
    result = subprocess.run(
        [DBN_CLI, str(fixture), "--csv"],
        check=True, capture_output=True, text=True,
    )
    return result.stdout


def dbn_cli_version() -> str:
    try:
        return subprocess.run(
            [DBN_CLI, "--version"], check=True, capture_output=True, text=True,
        ).stdout.strip()
    except FileNotFoundError:
        return "unknown"


def transform(raw_csv: str, schema: str, version: str) -> str:
    """Reorder + rename + scale; return final CSV text."""
    spec = SCHEMAS.get(schema)
    if spec is None:
        raise ValueError(f"schema not registered: {schema}")
    reader = csv.DictReader(io.StringIO(raw_csv))
    rows = list(reader)
    src_cols = list(reader.fieldnames or [])

    renames: dict[str, str] = spec.get("column_renames") or {}
    # Apply renames: produce post-rename src columns + rebuild row dicts.
    rn_src_cols = [renames.get(c, c) for c in src_cols]
    rn_rows = [{renames.get(k, k): v for k, v in r.items()} for r in rows]

    ext_cols = spec.get("ext_columns")
    if ext_cols is None:
        # Discovery pass: drop rtype only, preserve dbn-cli column order.
        ext_cols = [c for c in rn_src_cols if c != "rtype"]

    missing = [c for c in ext_cols if c not in rn_src_cols]
    if missing:
        raise ValueError(
            f"{schema}: dbn-cli CSV is missing columns expected by extension: {missing}\n"
            f"  dbn-cli columns (after rename): {rn_src_cols}"
        )

    price_cols = set(spec.get("price_columns") or [])
    out = io.StringIO()
    # Note: provenance (dbn-cli version) is intentionally NOT embedded as a
    # comment line — DuckDB's read_csv sniffer interacts poorly with leading
    # comments (the ';' in "dbn 0.59.0; schema: trades" was being read as a
    # delimiter candidate before comment-filtering applied).
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(ext_cols)
    for r in rn_rows:
        outrow = []
        for c in ext_cols:
            v = r[c]
            if c in price_cols and v not in ("", None):
                # Raw int64 nanoseconds (1e-9 fixed-point) → DOUBLE.
                outrow.append(f"{int(v) / 1e9:.9f}")
            else:
                outrow.append(v)
        writer.writerow(outrow)
    return out.getvalue()


def parse_schema_from_name(name: str) -> str | None:
    """`test_data.trades.dbn` → `trades`; `test_data.mbp-1.v2.dbn.zst` → `mbp-1`;
    `test_data.trades.empty.dbn` → `trades.empty`.

    Returns None for files that don't follow the naming convention.
    """
    # Strip prefix + .dbn/.dbn.zst suffix; what's left is the schema slug
    # (optionally with a `.vN` version qualifier appended).
    m = re.match(r"test_data\.(.+?)(?:\.v\d+)?\.dbn(?:\.zst)?$", name)
    if not m:
        return None
    return m.group(1)


def main() -> int:
    if not DATA.is_dir():
        print(f"FATAL: {DATA} does not exist", file=sys.stderr)
        return 1
    EXPECTED.mkdir(exist_ok=True)
    version = dbn_cli_version()
    print(f"using {version}", file=sys.stderr)
    errors: list[str] = []
    for fixture in sorted(DATA.glob("test_data.*.dbn*")):
        schema = parse_schema_from_name(fixture.name)
        if schema is None:
            print(f"  SKIP {fixture.name} (no schema)", file=sys.stderr)
            continue
        if schema not in SCHEMAS:
            print(f"  SKIP {fixture.name} (no transform for {schema!r})", file=sys.stderr)
            continue
        try:
            raw = run_dbn_cli(fixture)
            csv_out = transform(raw, schema, version)
        except (subprocess.CalledProcessError, ValueError) as e:
            errors.append(f"{fixture.name}: {e}")
            print(f"  FAIL {fixture.name}: {e}", file=sys.stderr)
            continue
        out_path = EXPECTED / (fixture.stem.removesuffix(".dbn") + ".csv")
        out_path.write_text(csv_out, encoding="utf-8")
        n_rows = csv_out.count("\n") - 1  # minus header row
        print(f"  ok   {fixture.name} -> {out_path.name} ({n_rows} rows)", file=sys.stderr)
    if errors:
        print(f"\n{len(errors)} error(s):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
