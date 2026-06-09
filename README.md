# duckdb-dbn

A [DuckDB](https://duckdb.org) extension for reading [Databento](https://databento.com)
**DBN** (Databento Binary Encoding) files directly as SQL tables — including the
`.dbn.zst` Zstandard-compressed captures Databento ships.

```sql
LOAD dbn;

SELECT ts_event, instrument_id, price, size, side
FROM read_dbn_trades('XNAS-20240101.trades.dbn.zst')
WHERE side = 'B'
ORDER BY ts_event
LIMIT 10;
```

No conversion step, no Python — point a reader at a `.dbn` / `.dbn.zst` file (or a
glob of them) and query it. Decoding is verified **bit-identical to the official
`databento` Python decoder** across every schema and DBN version (see
[Correctness](#correctness)).

## Features

- **All market-data schemas** — trades, mbo, mbp-1/mbp-10, tbbo, bbo, cbbo, cmbp-1,
  tcbbo, ohlcv (1s/1m/1h/1d/eod), status, imbalance, statistics, definition.
- **Compressed and uncompressed** — reads `.dbn` and `.dbn.zst` transparently.
- **DBN v1, v2, and v3** — version-aware decoding; fields absent in older versions
  are surfaced as SQL `NULL`.
- **Globs / multi-file** — `read_dbn_trades('data/*.trades.dbn.zst')` reads many
  files in order as one table (they must share schema, version, and `ts_out`).
- **Filter & projection pushdown** — `WHERE`/column pruning are pushed into the
  scan (header fields filtered during decode; body fields right after).
- **Correct `NULL` semantics** — DBN "undefined" sentinels (e.g. one-sided quotes,
  optional definition fields) are surfaced as SQL `NULL`, matching `databento` —
  not as the raw sentinel, which would silently corrupt aggregates.
- **Nanosecond timestamps** — all `ts_*` columns are `TIMESTAMP_NS`.
- **Symbol resolution** — `symbols := true` resolves `instrument_id` to its raw
  symbol inline for live captures (see [Symbol resolution](#symbol-resolution)).
- **Replacement scan** — `SELECT * FROM 'file.dbn'` works without naming a function
  (for schema-bearing files; dispatches to `read_dbn`).

## Function reference

Every reader takes a file path (or glob) as its first argument and returns the
schema's records as rows.

| Function | DBN schema |
|---|---|
| `read_dbn_trades(path)` | trades |
| `read_dbn_mbo(path)` | mbo (market-by-order) |
| `read_dbn_mbp1(path)` / `read_dbn_mbp10(path)` | mbp-1 / mbp-10 |
| `read_dbn_tbbo(path)` | tbbo |
| `read_dbn_bbo_1s(path)` / `read_dbn_bbo_1m(path)` | bbo-1s / bbo-1m |
| `read_dbn_cmbp1(path)` | cmbp-1 (consolidated) |
| `read_dbn_cbbo_1s(path)` / `read_dbn_cbbo_1m(path)` | cbbo-1s / cbbo-1m |
| `read_dbn_tcbbo(path)` | tcbbo |
| `read_dbn_ohlcv_1s/1m/1h/1d(path)`, `read_dbn_ohlcv_eod(path)` | ohlcv-* |
| `read_dbn_status(path)` | status |
| `read_dbn_imbalance(path)` | imbalance |
| `read_dbn_statistics(path)` | statistics |
| `read_dbn_definition(path)` | definition |
| `read_dbn_symbol_mapping(path)` | SymbolMappingMsg (live gateway) |
| `read_dbn_system(path)` | SystemMsg (live gateway) |
| `read_dbn(path)` | **polymorphic** — dispatches on `metadata.schema` |
| `dbn_metadata(path)` | one row of file metadata (version, dataset, schema, …) |
| `dbn_records(path)` | raw record headers + body bytes (rtype-agnostic) |

Named parameter (market-data readers): `symbols := true` (default `false`).

`read_dbn` requires a `schema` in the file metadata. Live captures are often
schema-less, so for those use the specific `read_dbn_<schema>()` reader.

## Usage

```sql
-- Inspect a file first
SELECT * FROM dbn_metadata('capture.dbn.zst');

-- Query a schema
SELECT count(*), avg(price) FROM read_dbn_trades('capture.dbn.zst');

-- Glob many files (read in order)
SELECT * FROM read_dbn_mbp1('data/2024-01-*.mbp-1.dbn.zst') WHERE instrument_id = 5482;

-- Replacement scan (schema-bearing files)
SELECT count(*) FROM 'historical.trades.dbn';
```

### Symbol resolution

Live captures carry the `instrument_id → raw symbol` binding in interleaved
`SymbolMappingMsg` records (`metadata.symbols` is empty). `symbols := true` resolves
each record's symbol inline, in a single in-order pass:

```sql
SELECT ts_event, instrument_id, symbol, bid_price, ask_price
FROM read_dbn_tcbbo('OPRA.PILLAR.tcbbo.dbn.zst', symbols := true);
```

The `symbol` column is `NULL` for any record whose mapping hasn't been seen yet.
You can also read the mappings directly with `read_dbn_symbol_mapping(path)`.

## Performance

The readers are single-threaded and fast enough for ad-hoc work, but reading a
large `.dbn.zst` is **decompression-bound** (a single Zstd frame must be
decompressed serially). For **load-once / query-many** workloads, convert to a
columnar format once and query that — it reads only the columns you touch and is
dramatically faster:

```sql
COPY (SELECT * FROM read_dbn_tcbbo('big.dbn.zst'))
  TO 'big.parquet' (FORMAT parquet, COMPRESSION zstd);
-- then query big.parquet
```

DuckDB's native time-series features (ASOF JOIN, `time_bucket`, window functions)
work well on the result for quote/trade alignment and bar building.

## Building

See **[BUILD.md](BUILD.md)** for the full walkthrough (Windows/x64 specifics
included). In brief, with submodules initialized and vcpkg configured:

```sh
make            # builds build/release/extension/dbn/dbn.duckdb_extension + a duckdb shell
make test       # runs the SQL test suite in test/sql
```

Load the built extension in any DuckDB started with `-unsigned`:

```sql
LOAD 'build/release/extension/dbn/dbn.duckdb_extension';
```

## Correctness

The reader is differentially verified against the official `databento` Python
decoder over every fixture and DBN version: decoding is bit-identical (see
`tools/diff_databento.py`). The SQL test suite lives in `test/sql/`.

## Maintenance & publishing

See **[MAINTAINING.md](MAINTAINING.md)** (releasing, CI, regenerating fixtures,
bumping the DuckDB version).

## Acknowledgements

Built from the [DuckDB extension template](https://github.com/duckdb/extension-template).
DBN is a format by [Databento](https://databento.com); this is an independent
reader and is not affiliated with Databento.
