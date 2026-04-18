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
