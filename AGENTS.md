# dbzero

dbzero is a state management system for persisting process state without a database. It lets Python processes keep their in-memory state durable across restarts — no separate DB server, schemas, or ORM. The core is C++ with Python bindings. See https://docs.dbzero.io for user-facing documentation.

## Development workflow

### TDD is required

When implementing new features, follow test-driven development:

1. Write a failing test first (Python tests in `python_tests/`, C++ tests under `tests/` / `subprojects/`).
2. Implement the minimum code to make the test pass.
3. Refactor while keeping tests green.

All tests must pass before a change is considered complete.

### Building

- Debug build: `./scripts/build.sh -d` (equivalent to `./scripts/build.sh`; debug is the default)
- Release build: `./scripts/build.sh -r`
- Release build with C++ unit test binary: `./scripts/build.sh -r -t`
- For regular development and focused Python-side test runs, prefer the release build because it is significantly faster. Unless C++ tests are important for the current change, build without `-t`; skipping the C++ test binary is much faster.
- Use debug builds only when tracking a specific deep bug that needs assertions/debug-level checks, or when the user explicitly asks for debug validation. Do not run debug validation as a default final handoff step.

### Running tests

- Python tests: `./scripts/run_tests.sh`
- Final Python test checks: `./scripts/run_tests.sh -j 6`
- C++ tests after a `-t` build: `./build/release/tests.x`
- Broad debug/release builds and full-suite checks are final handoff validation only; run them when the user explicitly asks for handoff.
- For normal development and handoff, validate against release builds. Run debug-mode tests only when debugging a hard-to-locate problem or when the user explicitly requests them.
- During development, do not run stress tests by default; they are intentionally slow. Run focused tests specific to the feature or refactor being worked on before finalization.
- If any C++ source under the native/core part of the project was modified, also run the C++ test suite during final handoff validation (do not rely on the Python tests alone to cover native changes).

Never mark a task done while tests are failing.

## Implementation notes

### Scope discipline

Never make cosmetic changes unless they are explicitly requested by the task.

### v_object constructor conventions

Types derived from `v_object` should follow the project-wide constructor pattern:

- New durable instances are constructed from `Memspace &` plus any type-specific creation arguments.
- Existing durable instances are reopened from `mptr` plus any type-specific runtime dependencies.

### Overlaid type inheritance

Variable-size overlaid types that derive from another overlaid type must use `db0::o_ext<Derived, BaseOverlay, VER, STORE_VER>` rather than directly inheriting from an `o_base`-derived overlay such as `o_list`. Direct inheritance bypasses `o_ext` sizing, version, and dynamic-area handling and can corrupt overlaid layout assumptions.

### C++ style

- Use snake_case for parameter names and local variable names in C++ code. Parameter names should be concise yet informative. Keep method names consistent with the surrounding code.
- Project types often avoid implicit bool conversion because it can hide subtle ownership, state, and null-check bugs. Use explicit double-negation checks such as `if (!!obj)` or `while (!!item)` when a type supports `operator!()`.

### Python binding wrapper access

When accessing a C++ object stored inside a Python wrapper, use `ext()` for read-only operations and for operations that are explicitly documented as non-mutating wrapper/runtime attachment updates.

Use `modifyExt()` for real object mutations, especially durable state changes. Do not use `const_cast` on `ext()` to call a mutating method. If a wrapper currently exposes only a const object but needs a mutating API, change the wrapper type or access path so the mutation can go through `modifyExt()`.

### Python C API safety helpers

When iterating over Python objects in C++, use `Py_FOR(item, iterator)` from `PySafeAPI.hpp` with an owned iterator, for example `auto iterator = Py_OWN(PyObject_GetIter(obj));`. The loop owns each yielded item and avoids manual `Py_DECREF` paths.

For Python container/object writes, use the `PySafe_*` helpers from `PySafeAPI.hpp` instead of the raw C API when a helper exists, such as `PySafeList_SetItem`, `PySafeTuple_SetItem`, `PySafeDict_SetItem`, `PySafeDict_SetItemString`, `PySafeSet_Add`, and `PySafeModule_AddObject`.

### MorphingBIndex: address and type can change on mutation

A `MorphingBIndex` does not behave like a typical container. On mutation (`insert`, `erase`) it may morph into a different internal storage variant (itty / array_2..4 / vector / bindex), and the morph can change both its **address** and its **type**.

Consequences for any code that mutates a `MorphingBIndex`:

- Any externally stored `{address, type}` pair referring to the bindex is potentially invalidated after every `insert` or `erase` call. Lookups through a stale pair read pre-mutation storage and return wrong data.
- A live handle to the bindex remains valid across the mutation and reflects the new storage; prefer re-reading `bindex.getAddress()` / `bindex.getIndexType()` from the handle over trusting any previously captured copy.
- Destructive shortcuts (destroying and rebuilding the whole bindex, or erasing it entirely from its parent) avoid the issue since no stale reference remains.

When adding a new mutating path that operates on a `MorphingBIndex`, treat re-syncing any externally held `{address, type}` as mandatory, not an optimization. Collection-specific handling (where these pairs live, which paths must re-sync) is documented at the top of the relevant `.cpp` files.
