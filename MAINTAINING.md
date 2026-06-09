# Maintaining & publishing `duckdb-dbn`

Notes for whoever maintains this extension. For end-user docs see
[README.md](README.md); for the first-time build walkthrough see
[BUILD.md](BUILD.md).

## Repository layout

| Path | What |
|---|---|
| `src/dbn_extension.cpp` | All table functions, registration, pushdown, parallel paths. |
| `src/dbn_native_decoder.{hpp,cpp}` | The DBN byte decoder (`DbnFileReader`): metadata parse, Zstd streaming, block-buffered record reader. |
| `src/include/dbn_emit_helpers.hpp` | Small per-cell emit helpers (chars, cstrings). |
| `test/sql/*.test` | SQL test suite (sqllogictest format). |
| `test/data/` | Binary fixtures (`.dbn`, `.dbn.zst`). |
| `tools/` | Fixture generators + the `databento` differential harness. |
| `third_party/databento-cpp` | Submodule: record structs / enums (currently v0.59.0). |
| `duckdb`, `extension-ci-tools` | Submodules pinned to the target DuckDB release. |
| `.github/workflows/MainDistributionPipeline.yml` | CI (build + test + code quality). |
| `docs/UPDATING.md` | Template doc: bumping to a new DuckDB version. |

## Build & test (local)

Full instructions (incl. Windows/x64) are in [BUILD.md](BUILD.md). Quick form
once submodules + vcpkg are set up:

```sh
make                 # build extension + duckdb shell + unittest
make test            # run test/sql via the unittest runner
# or directly:
./build/release/test/unittest.exe --test-dir . "[sql]"
```

On this Windows machine the canonical build is x64 via `vcvars64`; see BUILD.md
(a plain `cmake` picks the 32-bit toolchain and OOMs on real files).

## Correctness: the `databento` differential

`tools/diff_databento.py` is the gold-standard regression check — it decodes every
fixture with both this reader (exported to Parquet via the built `duckdb.exe`) and
the official `databento` Python decoder, and compares every column.

```sh
pip install databento pandas pyarrow      # one-time
python tools/diff_databento.py            # expects build/release/duckdb.exe present
python tools/diff_databento.py trades     # restrict to one fixture/version
```

Comparison is **exact** for integer / timestamp / string columns and uses a small
tolerance only for genuine 1e-9 fixed-point price columns. It **exits nonzero** on
any mismatch or reader error (CI-usable). The one **intentional** divergence —
`statistics.quantity`, where we surface the undefined `INT32_MAX` sentinel as SQL
`NULL` while databento keeps the raw int (an int column can't hold `NaN`) — is
allowlisted (`EXPECTED_DIFF`), so a clean run reports all `OK` / exit 0. Run this
after any change to decoding or column emission.

## Regenerating fixtures

Fixtures are committed binaries. The generators need Julia + the DBN.jl writer
(`DatabentoBinaryEncoding.jl`); see the header of each script for the exact
`--project` path.

```sh
julia --project=<path-to-DatabentoBinaryEncoding.jl> tools/generate_fixtures.jl
julia --project=<path-to-DatabentoBinaryEncoding.jl> tools/gen_tcbbo_symbols.jl
julia --project=<path-to-DatabentoBinaryEncoding.jl> tools/gen_bodyfilter_fixture.jl
python tools/generate_expected.py         # expected-result helpers, if used
```

`generate_fixtures.jl` regenerates the canonical per-schema fixtures; the `gen_*`
scripts produce purpose-built fixtures for specific tests (symbol resolution,
the body-filter regression). Regenerate deliberately and re-run `make test` +
the differential afterward.

## Code quality

CI runs `format;tidy` (clang-format + clang-tidy) via the extension-ci-tools
`_extension_code_quality.yml` workflow. Match the existing style (tabs, the
established naming/comment density) so the format check stays green.

## CI

`.github/workflows/MainDistributionPipeline.yml` runs on every push / PR and calls
DuckDB's reusable workflows:
- `duckdb-stable-build` — builds the per-platform `dbn.duckdb_extension` binaries
  and runs the test suite (`duckdb_version: v1.5.2`).
- `code-quality-check` — `format;tidy`.

There is **no deploy/release job** — see Publishing below.

## Publishing a release

No automated deployment is configured today. Two paths:

**A. DuckDB Community Extensions (recommended for public distribution).**
Submit a descriptor to <https://github.com/duckdb/community-extensions>; once
merged, users install with plain `INSTALL dbn; LOAD dbn;` from the community repo.
This is the lowest-friction route for consumers and is the intended home for an
extension like this.

**B. Self-host the binaries.** Add a deploy job to the pipeline (the
extension-ci-tools distribution workflow supports an S3 deploy step — provide the
bucket + AWS credentials as repo secrets), then users point at it:
```sql
SET custom_extension_repository = 'your-bucket.s3.<region>.amazonaws.com/dbn/latest';
INSTALL dbn; LOAD dbn;
```

**Versioning.** `EXT_VERSION_DBN` is derived by extension-ci-tools from `git
describe`. To stamp a release version, **create an annotated git tag**
(`git tag -a v0.1.0 -m ... && git push --tags`) before building/publishing; the
build embeds it (surfaced via DuckDB's `duckdb_extensions()`).

Pre-release checklist:
1. `make test` green (and the `databento` differential clean).
2. Submodules pinned to the intended DuckDB release (`git submodule status`).
3. Tag the release.
4. Build/publish per A or B.

## Bumping the DuckDB version

Follow [docs/UPDATING.md](docs/UPDATING.md): set the `duckdb` and
`extension-ci-tools` submodules to the new release, and bump `duckdb_version` /
`ci_tools_version` in `MainDistributionPipeline.yml`. The extension builds against
DuckDB's internal C++ API, so expect occasional source fixups on upgrade — the
table-function and `Vector`/`FlatVector` APIs are the usual touch points. Rebuild,
run the suite + differential, and check the parallel scan paths still register
(`MaxThreads` / `init_local` / `get_partition_data` API).

## Notable internals (so changes stay correct)

- **Undefined sentinels → NULL.** Prices (`INT64_MAX`), optional timestamps
  (`UINT64_MAX`), and stat quantity are surfaced as SQL `NULL` via
  `EmitPx`/`EmitPxV`/`EmitTsOpt`. Any new price/timestamp column must go through
  these, or aggregates get silently corrupted by the raw sentinel.
- **Empty mid-stream chunks terminate a scan.** `ScanWithBodyFilter` (and
  `dbn_records`) loop until a non-empty chunk or true EOF — never return an empty
  chunk before EOF, or a body filter that rejects a whole 2048-row chunk truncates
  the result. See `test/sql/body_filter_empty_chunk.test`.
- **Record order is preserved.** Readers emit records in file order; enrichment
  (e.g. `symbols`) is resolved inline in one pass, never via a reordering join.
- **Performance is decompression-bound** for compressed files (single Zstd frame,
  serial). Parallelizing the reader was investigated and does not pay off; columnar
  conversion is the lever. Don't re-litigate without new evidence.
