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

- Debug build: `./docker/dbzero-build.sh`
- Release build: `./docker/dbzero-build.sh -r`

### Running tests

- Python tests: `./scripts/run_tests.sh`
- If any C++ source under the native/core part of the project was modified, also run the C++ test suite (do not rely on the Python tests alone to cover native changes).

Never mark a task done while tests are failing.

## Implementation notes

### MorphingBIndex: address and type can change on mutation

A `MorphingBIndex` does not behave like a typical container. On mutation (`insert`, `erase`) it may morph into a different internal storage variant (itty / array_2..4 / vector / bindex), and the morph can change both its **address** and its **type**.

Consequences for any code that mutates a `MorphingBIndex`:

- Any externally stored `{address, type}` pair referring to the bindex is potentially invalidated after every `insert` or `erase` call. Lookups through a stale pair read pre-mutation storage and return wrong data.
- A live handle to the bindex remains valid across the mutation and reflects the new storage; prefer re-reading `bindex.getAddress()` / `bindex.getIndexType()` from the handle over trusting any previously captured copy.
- Destructive shortcuts (destroying and rebuilding the whole bindex, or erasing it entirely from its parent) avoid the issue since no stale reference remains.

When adding a new mutating path that operates on a `MorphingBIndex`, treat re-syncing any externally held `{address, type}` as mandatory, not an optimization. Collection-specific handling (where these pairs live, which paths must re-sync) is documented at the top of the relevant `.cpp` files.
