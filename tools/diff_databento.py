#!/usr/bin/env python
"""Differential correctness check: our DBN reader vs the authoritative `databento`
Python decoder. For each fixture we export our reader's full output to Parquet
(via the built duckdb.exe, avoiding python/duckdb ABI coupling), decode the same
file with databento.DBNStore, align rows by file order, map column names, and
compare every common column value-for-value. Any mismatch is a candidate bug.

Usage: python tools/diff_databento.py [file_or_fixture ...]
  with no args, runs the full fixture matrix.
"""
import os, sys, subprocess, tempfile, math
import databento as db
import pandas as pd
import numpy as np

EXE = os.path.abspath("build/release/duckdb.exe")
EXT = os.path.abspath("build/release/extension/dbn/dbn.duckdb_extension").replace("\\", "/")

# read_fn -> fixture basename (file is test/data/test_data.<basename>.dbn)
MATRIX = [
    ("read_dbn_trades", "trades"), ("read_dbn_mbo", "mbo"),
    ("read_dbn_mbp1", "mbp-1"), ("read_dbn_mbp10", "mbp-10"),
    ("read_dbn_tbbo", "tbbo"), ("read_dbn_bbo_1s", "bbo-1s"), ("read_dbn_bbo_1m", "bbo-1m"),
    ("read_dbn_cbbo_1s", "cbbo-1s"), ("read_dbn_cbbo_1m", "cbbo-1m"),
    ("read_dbn_cmbp1", "cmbp-1"), ("read_dbn_tcbbo", "tcbbo"),
    ("read_dbn_ohlcv_1s", "ohlcv-1s"), ("read_dbn_ohlcv_1m", "ohlcv-1m"),
    ("read_dbn_ohlcv_1h", "ohlcv-1h"), ("read_dbn_ohlcv_1d", "ohlcv-1d"),
    ("read_dbn_ohlcv_eod", "ohlcv-eod"),
    ("read_dbn_statistics", "statistics"), ("read_dbn_status", "status"),
    ("read_dbn_imbalance", "imbalance"), ("read_dbn_definition", "definition"),
]

# our name -> databento name (single-level book schemas)
RENAME = {
    "bid_price": "bid_px_00", "ask_price": "ask_px_00",
    "bid_size": "bid_sz_00", "ask_size": "ask_sz_00",
    "bid_ct": "bid_ct_00", "ask_ct": "ask_ct_00",
    "bid_pb": "bid_pb_00", "ask_pb": "ask_pb_00",
}

def our_df(read_fn, path):
    with tempfile.TemporaryDirectory() as td:
        pq = os.path.join(td, "o.parquet").replace("\\", "/")
        sql = (f"LOAD '{EXT}'; COPY (SELECT * FROM {read_fn}('{path}')) "
               f"TO '{pq}' (FORMAT parquet);")
        r = subprocess.run([EXE, "-unsigned", "-c", sql], capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"our reader failed: {r.stderr.strip()[:300]}")
        # numpy_nullable keeps NULL-bearing integer columns as nullable Int (not
        # float64), so the comparison can stay EXACT for them.
        return pd.read_parquet(pq, dtype_backend="numpy_nullable")

def ref_df(path):
    # pretty_ts=True -> undefined timestamps become NaT (NULL), matching our
    # reader; price_type=float -> undefined prices become NaN (NULL).
    df = db.DBNStore.from_file(path).to_df(price_type="float", pretty_ts=True,
                                           map_symbols=False).reset_index()
    for c in df.columns:
        if isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].dt.tz_localize(None)
    return df

# Columns whose value INTENTIONALLY differs from databento (not bugs):
#   statistics.quantity -> we surface the undefined sentinel as SQL NULL, while
#   databento keeps the raw INT32_MAX (an int column can't hold NaN).
EXPECTED_DIFF = {"statistics": {"quantity"}}

def col_diffs(a, b, n):
    """Element-wise compare two aligned Series. EXACT for ints/timestamps/strings;
    a small tolerance ONLY for genuine float (1e-9 fixed-point price) columns.
    Returns (num_diffs, first_(row, ours, ref) or None)."""
    is_float = (a.dtype.kind == "f") or (b.dtype.kind == "f")
    av, bv = a.tolist(), b.tolist()
    diffs, first = 0, None
    for k in range(n):
        x, y = av[k], bv[k]
        mx, my = pd.isna(x), pd.isna(y)
        if mx and my:
            continue
        if mx != my:
            ok = False
        elif is_float:
            ok = abs(float(x) - float(y)) <= 1e-6 + 1e-9 * abs(float(y))
        else:
            ok = (x == y)  # ints (exact), pd.Timestamp (exact to ns), strings
        if not ok:
            diffs += 1
            if first is None:
                first = (k, x, y)
    return diffs, first

def compare(read_fn, fixture, path=None):
    if path is None:
        path = f"test/data/test_data.{fixture}.dbn"
    if not os.path.exists(path):
        return (fixture, "SKIP (no fixture)", 0, [])
    path = os.path.abspath(path).replace("\\", "/")
    try:
        ours = our_df(read_fn, path).rename(columns=RENAME)
        ref = ref_df(path)
    except Exception as e:
        return (fixture, f"ERROR {e}", 0, [])
    issues = []
    if len(ours) != len(ref):
        issues.append(f"ROW COUNT ours={len(ours)} ref={len(ref)}")
    n = min(len(ours), len(ref))
    skip = EXPECTED_DIFF.get(fixture, set())
    common = [c for c in ours.columns if c in ref.columns and c not in skip]
    unmatched_ours = [c for c in ours.columns if c not in ref.columns]
    for c in common:
        a = ours[c].iloc[:n].reset_index(drop=True)
        b = ref[c].iloc[:n].reset_index(drop=True)
        ndiff, first = col_diffs(a, b, n)
        if ndiff:
            i, x, y = first
            issues.append(f"col {c}: {ndiff} diffs (row {i}: ours={x!r} ref={y!r})")
    status = "OK" if not issues else "FAIL"
    return (fixture, status, len(common), issues, unmatched_ours)

# version variants: (read_fn, label, explicit path)
VERSIONS = [
    ("read_dbn_mbp1", "mbp-1.v2", "test/data/test_data.mbp-1.v2.dbn.zst"),
    ("read_dbn_ohlcv_1s", "ohlcv-1s.v2", "test/data/test_data.ohlcv-1s.v2.dbn.zst"),
    ("read_dbn_mbo", "mbo.v3", "test/data/test_data.mbo.v3.dbn.zst"),
    ("read_dbn_definition", "definition.v3", "test/data/test_data.definition.v3.dbn.zst"),
    ("read_dbn_trades", "trades.v2", "test/data/test_data.trades.v2.dbn.zst"),
]

def main():
    entries = [(*m, None) for m in MATRIX] + [(f, l, p) for f, l, p in VERSIONS]
    targets = sys.argv[1:]
    if targets:
        # restrict to entries matching a requested fixture label or path substring
        entries = [e for e in entries
                   if any(t == e[1] or t in e[1] or (e[2] and t in e[2]) for t in targets)]
        if not entries:
            print(f"no fixtures match {targets}", file=sys.stderr)
            return 2
    print(f"{'fixture':<14} {'status':<6} {'cmp_cols':>8}  detail")
    nbad = 0
    for read_fn, fixture, path in entries:
        res = compare(read_fn, fixture, path)
        fixture, status = res[0], res[1]
        ncols = res[2] if len(res) > 2 else 0
        issues = res[3] if len(res) > 3 else []
        unmatched = res[4] if len(res) > 4 else []
        print(f"{fixture:<14} {status:<6} {ncols:>8}  " +
              ("; ".join(issues[:4]) if issues else ("unmatched_ours=" + ",".join(unmatched) if unmatched else "")))
        if status == "FAIL" or status.startswith("ERROR"):
            nbad += 1
            for x in issues:
                print(f"    - {x}")
            if unmatched:
                print(f"    (our cols not in ref: {unmatched})")
    print(f"\n{nbad} schema(s) with value mismatches or errors.")
    return 1 if nbad else 0

if __name__ == "__main__":
    sys.exit(main())
