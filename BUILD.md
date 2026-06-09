# Building `duckdb-dbn` on Windows

This document is the Phase 0 walkthrough: get the unmodified extension template building and loading in DuckDB before any DBN code is added. If this works, the rest of the project is straightforward.

## What's already done

The repo was scaffolded from `duckdb/extension-template` and renamed via the template's `bootstrap-template.py` script. Extension name is `dbn`. Sources live in `src/dbn_extension.cpp` and `src/include/dbn_extension.hpp`. Two placeholder scalar functions exist (`dbn(name)`, `dbn_openssl_version(name)`) — they'll be replaced in Phase 1 by a `read_dbn(path)` table function.

The repo has two git submodules that were NOT cloned yet (to keep the initial scaffold small): `duckdb/` (the full DuckDB source tree, several hundred MB) and `extension-ci-tools/` (build infrastructure). You'll initialize them in step 2 below.

## Prerequisites

Confirmed present on this machine:

- Git 2.54 ✓
- CMake 3.29 ✓ (needs ≥ 3.5; template's `CMakeLists.txt` line 1)
- Python 3.13 ✓ (used by template scripts)
- Visual Studio 2022 ✓ (need the "Desktop development with C++" workload — verify in VS Installer if unsure)

Still required:

- **vcpkg** — not installed. Step 3 below covers this.
- **GNU make** — the template's `Makefile` is the canonical build entrypoint, but `nmake` won't work here. Options:
  - Use `make` from Git for Windows (`C:\Program Files\Git\usr\bin\make.exe`) — usually present, just not on PATH.
  - Use WSL (Ubuntu) for the whole build flow — simplest if Windows-native is fighting you.
  - Skip `make` and invoke CMake directly (documented below).
- **DuckDB CLI** — download the prebuilt Windows binary from <https://duckdb.org/docs/installation/> if you want to test extension loading. The build also produces a `duckdb.exe` with the extension preloaded, so this is optional.

## Build steps

### 1. Open a Developer PowerShell for VS 2022

Critical — without this, `cl.exe` and the MSVC runtime aren't on PATH. Start menu → "Developer PowerShell for VS 2022", or run:
```powershell
& 'C:\Program Files\Microsoft Visual Studio\2022\Community\Common7\Tools\Launch-VsDevShell.ps1'
```
(adjust the edition path if you have Professional/Enterprise)

Verify:
```powershell
cl.exe   # should print "Microsoft (R) C/C++ Optimizing Compiler..."
```

### 2. Initialize submodules (this is the slow step)

```powershell
cd C:\Users\tbeas\Documents\GitHub\duckdb-dbn
git submodule update --init --recursive
```

Expect several hundred MB and several minutes. The DuckDB submodule has its own submodules.

### 3. Install vcpkg

```powershell
git clone https://github.com/Microsoft/vcpkg.git C:\vcpkg
C:\vcpkg\bootstrap-vcpkg.bat
$env:VCPKG_TOOLCHAIN_PATH = "C:\vcpkg\scripts\buildsystems\vcpkg.cmake"
```

For convenience, add `VCPKG_TOOLCHAIN_PATH` as a persistent user env var via System Properties → Environment Variables.

### 4. Build

**Option A — via the template's Makefile (if `make` is available):**
```powershell
$env:Path = "C:\Program Files\Git\usr\bin;$env:Path"   # put git-bash make on PATH
make
```

**Option B — direct CMake invocation (more reliable on Windows):**

> **Critical: choose your architecture.** Calling `cmake -B build/release -S .`
> from a plain shell on this machine picks the 32-bit (x86) toolchain by
> default — that caps the entire build (and runtime!) at ~2 GB virtual
> address space, which OOMs on any non-trivial DBN file. Use `vcvars64.bat`
> and pass `-S duckdb -DVCPKG_TARGET_TRIPLET=x64-windows` explicitly.

```powershell
# x64 release build (do this from a plain PowerShell, not the dev shell —
# the `vcvars64.bat` call inside `cmd /c` sets the toolchain for the
# embedded cmake invocation):
$repo = "C:\Users\tbeas\Documents\GitHub\duckdb-dbn"
cd $repo
cmd /c '"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat" && ' +
       'cmake -G Ninja -S duckdb -B build\release ' +
       '-DCMAKE_BUILD_TYPE=Release ' +
       '-DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake ' +
       '-DVCPKG_TARGET_TRIPLET=x64-windows ' +
       "-DVCPKG_MANIFEST_DIR=$repo " +
       '-DEXTENSION_STATIC_BUILD=1 ' +
       "-DDUCKDB_EXTENSION_CONFIGS=$repo\extension_config.cmake"

cmd /c '"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat" && ' +
       'cmake --build build\release --config Release ' +
       '--target dbn_extension dbn_loadable_extension unittest shell'
```

The `-S duckdb` (not `-S .`) is required — `CMakeLists.txt` at the repo
root is the extension's own config; the build wants the DuckDB submodule
as its source. The Makefile wrapper does this automatically.

Verify the resulting binary is 64-bit:
```powershell
$b = [IO.File]::ReadAllBytes("build\release\duckdb.exe")
$pe = [BitConverter]::ToInt32($b, 0x3C)
switch ([BitConverter]::ToUInt16($b, $pe + 4)) {
    0x8664 { "x64 (good)" }
    0x014c { "x86 (rebuild with vcvars64)" }
}
```

First build is slow (vcpkg compiles OpenSSL; DuckDB itself compiles). Subsequent rebuilds use the cache. Expect 15–30 minutes for the first run.

**Switching architectures.** vcpkg caches the *x86* and *x64* triplets in
different `build/release/vcpkg_installed/` subdirs, so the `build/release`
tree is architecture-specific. To switch (e.g. from a legacy x86 build to
x64), wipe `build/release` and re-configure — don't try to incrementally
rebuild.

### 5. Locate and load the extension

After a successful build:
```
build/release/extension/dbn/dbn.duckdb_extension
build/release/duckdb.exe          (DuckDB CLI with extension pre-linked)
```

Load it manually in any DuckDB CLI:
```sql
-- Launch DuckDB CLI with unsigned extensions allowed
-- > duckdb.exe -unsigned

LOAD 'C:/Users/tbeas/Documents/GitHub/duckdb-dbn/build/release/extension/dbn/dbn.duckdb_extension';
SELECT dbn('Phase 0 works');
-- Expected: ...........🦆 Phase 0 works
```

Or use the build's preloaded `duckdb.exe`:
```powershell
.\build\release\duckdb.exe
SELECT dbn('Phase 0 works');
```

### 6. Run the template's test suite

```powershell
make test
# or:
.\build\release\test\unittest.exe --test-dir . "[sql]"
```

The test in `test/sql/dbn.test` asserts the two placeholder functions work. Pass = Phase 0 done.

## Common failure modes

- **`cl.exe not found`** — you're not in a Developer PowerShell. Re-launch.
- **vcpkg `find_package(OpenSSL)` fails** — `VCPKG_TOOLCHAIN_PATH` not exported. Echo it (`$env:VCPKG_TOOLCHAIN_PATH`) to confirm.
- **Linker errors about missing DuckDB symbols** — submodules not initialized. Re-run `git submodule update --init --recursive`.
- **CMake picks the wrong generator** — explicitly pass `-G "Visual Studio 17 2022" -A x64` to the configure step.
- **Build runs out of disk** — vcpkg + DuckDB build artifacts together are 5–10 GB. Have headroom.

## When Phase 0 is done

You should be able to launch a DuckDB CLI, `LOAD` the built extension, and run `SELECT dbn('hi');` returning the duck emoji string. At that point we know the toolchain is healthy and Phase 1 (replacing the placeholder functions with `read_dbn(path)` backed by the vendored DBN record structs) is just C++.

## When Phase 0 doesn't work after a real effort

If the Windows-native path keeps fighting, fall back to WSL Ubuntu. The DuckDB extension community largely develops on Linux/macOS and the template's `make` flow works there with no fuss. The resulting extension binary is per-platform anyway — you'd cross-build for Windows from CI later (Phase 5).
