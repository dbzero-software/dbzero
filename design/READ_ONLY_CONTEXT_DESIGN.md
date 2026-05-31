# Read-Only Context Design

This document describes the implementation plan for `db0.read_only`, a Python
context manager that guarantees a block performs no dbzero mutations.

## Goal

`db0.read_only` lets callers mark a block as non-mutating and fail immediately
if dbzero detects an attempted write. The guarantee covers direct writes and
internal durable updates such as reference count changes.

Example:

```python
obj = next(iter(db0.find(Person)))

with db0.read_only():
    print(obj.given_name)

    with db0.read_only():
        print(obj.surname)

        with db0.atomic():
            obj.surname = "Kowalski"
```

The assignment raises `RuntimeError` because it attempts to mutate durable
state while a read-only context is active.

## User Semantics

The Python API is:

```python
with db0.read_only():
    ...
```

`read_only` contexts are process-local and thread-local. Entering the context
increments a read-only depth counter for the current thread. Exiting the
context decrements it. A mutation is rejected whenever the depth is greater than
zero.

Behavior rules:

- Read-only contexts can be nested.
- Mutations attempted inside read-only contexts raise `RuntimeError`.
- Read-only contexts are allowed inside `db0.atomic()` operations.
- `db0.atomic()` blocks started inside `db0.read_only()` are optimized out and
  have no effect.
- Exceptions raised by user code still propagate normally.
- Exiting a read-only context must restore the previous depth even when the
  block exits by exception.

The context is intentionally stricter than opening a prefix in read-only mode.
Read-only prefix access prevents writes to that prefix. `db0.read_only`
prevents all dbzero durable mutations visible through the current Python thread.

## Mutation Detection

The native mutation check belongs at the Python API mutation boundary. In the
current codebase, durable update paths instantiate `db0::FixtureLock` before
performing updates. `FixtureLock` is therefore the primary enforcement point.

When constructing `FixtureLock`:

1. Check whether a Python read-only context is active for the current thread.
2. If active, raise a Python `RuntimeError` with a clear message such as
   `"dbzero read_only context forbids mutation"`.
3. Keep the existing prefix access check for `AccessType::READ_WRITE`.
4. Mark the fixture updated only after the read-only check succeeds.

The check must happen before any mutation side effect. In particular, it must
run before `Fixture::onUpdated()`.

This design intentionally treats reference count updates, tag/index
bookkeeping, `touch`, materialization that creates durable state, and collection
updates as writes because those paths are expected to acquire `FixtureLock`.

## Native State

Add a small native read-only context state holder, for example
`db0::ReadOnlyContext` under `src/dbzero/workspace/`.

Responsibilities:

- Maintain `thread_local unsigned int s_depth`.
- Expose `static bool isActive()`.
- Expose `static unsigned int depth()` for testing/debugging if useful.
- Increment depth in the constructor.
- Decrement depth in `close()` and the destructor.
- Make `close()` idempotent so Python wrappers can safely call it from
  `__exit__` and deallocation paths.

The read-only state does not need to acquire workspace locks because it is only
a per-thread guard. It also should not interact with autocommit directly:
autocommit commits already-created mutations, while `read_only` prevents new
mutations from being created in the guarded block.

## Python Binding

Expose two native functions or one native context object:

- `begin_read_only() -> ReadOnlyContext`
- `read_only_is_active() -> bool` only if needed by Python or RPC integration

The shape should match `PyAtomic` and `PyLocked`:

- Add `PyReadOnly.hpp/.cpp`.
- Define `dbzero.ReadOnlyContext`.
- Provide a `close()` method.
- Register `begin_read_only` in `src/dbzero/bindings/python/dbzero.cpp`.

The pure Python wrapper should live in `dbzero/dbzero/read_only.py`:

```python
from .dbzero import begin_read_only


class ReadOnlyManager:
    def __init__(self):
        self.__ctx = None

    def __enter__(self):
        self.__ctx = begin_read_only()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        if self.__ctx is not None:
            self.__ctx.close()
            self.__ctx = None


def read_only() -> ReadOnlyManager:
    return ReadOnlyManager()
```

Export it from `dbzero/dbzero/__init__.py` and add type stubs to
`dbzero/dbzero/dbzero.pyi`.

## Atomic Interaction

Starting `db0.atomic()` inside `db0.read_only()` should be a no-op. The no-op
must avoid acquiring the atomic mutex and must avoid constructing
`db0::AtomicContext`, because there can be no valid mutation inside the block.

Implement this in the Python `AtomicManager` layer:

- Add a native or Python-visible `read_only_is_active()` check.
- In `AtomicManager.begin()`, if read-only is active, store a no-op sentinel
  instead of calling `begin_atomic()`.
- `close()` and `cancel()` on the no-op sentinel do nothing.

This keeps existing native atomic behavior unchanged for normal operations and
avoids introducing special inactive states into `AtomicContext`.

If `db0.read_only()` is entered while an atomic operation is already active,
the read-only block is allowed. Any attempted mutation is still rejected by
`FixtureLock`. On exit, the surrounding atomic operation remains active.

## RPC Interaction

`db0.read_only` must also reject mutating remote invocation through `db0-rpc` at
the caller site before the remote call is attempted.

The db0-rpc package is outside this repository, so dbzero should expose a small
public predicate for integration:

```python
db0.in_read_only() -> bool
```

`db0-rpc` should call this predicate before invoking a remote method known to be
mutating. If it returns `True`, db0-rpc raises `RuntimeError` locally and must
not send the request.

Mutating methods should be identified using the same metadata that db0-rpc
already uses to distinguish read methods from mutators. If db0-rpc uses
reflection metadata, `CallableType.MUTATOR` is the relevant classification.

Read-only remote calls are allowed.

## Error Type

The user-facing exception must be `RuntimeError`.

Native code may throw an internal C++ exception type only if the Python API
translation maps it to `PyExc_RuntimeError`. Prefer adding a specific exception
path or helper so tests assert `pytest.raises(RuntimeError)` reliably.

The exception message should include `read_only` and `mutation` so failures are
diagnosable without depending on exact wording.

## Test Plan

Follow TDD. Start with Python tests in `python_tests/test_read_only.py`.

Required Python tests:

- Reading a field inside `db0.read_only()` succeeds.
- Assigning a memo field inside `db0.read_only()` raises `RuntimeError`.
- Mutating a dbzero list, set, dict, bytearray, index, and tags inside
  `db0.read_only()` raises `RuntimeError`.
- `db0.touch(obj)` inside `db0.read_only()` raises `RuntimeError`.
- Creating/materializing a new durable object inside `db0.read_only()` raises
  `RuntimeError`.
- Nested `db0.read_only()` blocks are allowed and keep rejecting mutations until
  the outermost context exits.
- After a failed mutation inside a nested read-only block, later writes outside
  all read-only blocks still work.
- `db0.read_only()` inside `db0.atomic()` is allowed and mutation attempts raise
  `RuntimeError`.
- `db0.atomic()` inside `db0.read_only()` does not change state by itself and
  mutation attempts inside it raise `RuntimeError`.
- `atomic.cancel()` inside a no-op atomic created under `db0.read_only()` is
  accepted and has no effect.
- Exception exit from `db0.read_only()` restores the depth, allowing later
  mutations outside the block.
- A db0-rpc mutating call made under `db0.read_only()` raises locally without
  sending the remote request. This belongs in the db0-rpc test suite or in a
  dbzero-side integration test with a fake rpc module if practical.

Recommended native tests:

- `ReadOnlyContext` depth increments and decrements correctly.
- Nested contexts restore depth correctly.
- `FixtureLock` rejects when read-only depth is active before calling
  `Fixture::onUpdated()`.

## Implementation Slices

1. Add failing Python tests for direct memo assignment, nesting, and no-op
   atomic inside read-only.
2. Add native `ReadOnlyContext` state and Python binding.
3. Add `read_only.py`, `__init__.py` export, and type stubs.
4. Add `FixtureLock` enforcement and exception translation to `RuntimeError`.
5. Update `AtomicManager` to skip `begin_atomic()` when read-only is active.
6. Add collection, tag, touch, materialization, and exception-unwind tests.
7. Add the public `db0.in_read_only()` predicate for db0-rpc.
8. Add or coordinate db0-rpc caller-side mutator rejection tests.

## Open Questions

- Should `db0.in_read_only()` be documented as public API or kept as a
  semi-private integration hook for db0-rpc? The RPC requirement suggests public
  API is cleaner.
- Should creating non-materialized Python memo objects inside `read_only` be
  allowed if no durable state is created? This design allows ordinary Python
  object construction but rejects any materialization or durable registration
  that reaches `FixtureLock`.
- Should a read-only context affect other Python threads? This design says no.
  Cross-thread enforcement would require a workspace-level guard and would be a
  different feature.
