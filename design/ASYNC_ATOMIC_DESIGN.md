# Async Atomic Design

This document describes the planned split between synchronous `db0.atomic()`
and a new async-aware atomic context manager.

## Goal

`db0.atomic()` is a synchronous critical section. It can safely serialize
Python threads by blocking, but it cannot safely block an asyncio event-loop
thread while another task owns the atomic context. Blocking the event-loop
thread can prevent the owner task from resuming and closing the atomic context.

The goal is to make this behavior explicit:

- `db0.atomic()` is rejected when entered from an asyncio task.
- `db0.async_atomic()` is the supported API for async code.
- `db0.async_atomic()` serializes concurrent async atomic blocks by awaiting an
  async lock instead of blocking the event-loop thread.
- Unguarded dbzero mutations from another async task while an async atomic block
  is active fail fast instead of deadlocking.

## User Semantics

Synchronous code keeps using `db0.atomic()`:

```python
with db0.atomic():
    obj.name = "Alice"
    obj.count += 1
```

Async code must use `db0.async_atomic()`:

```python
async def update(obj):
    async with db0.async_atomic():
        obj.name = "Alice"
        await publish_update()
        obj.count += 1
```

Calling regular `db0.atomic()` from an asyncio task raises `RuntimeError`:

```python
async def update(obj):
    with db0.atomic():
        obj.name = "Alice"
```

Expected error shape:

```text
RuntimeError: db0.atomic is synchronous; use db0.async_atomic() inside asyncio tasks
```

This rule applies even when the block does not contain an `await`. The important
property is that the caller is running as an async task and can be interleaved
with other tasks on the same OS thread.

## Why Regular Atomic Cannot Serialize Async Tasks

Two asyncio tasks commonly run on the same OS thread. A native
`std::recursive_mutex` is thread-based, so it cannot distinguish those tasks by
itself. If task A owns the atomic context and suspends at `await`, task B can run
on the same thread.

If task B then blocks waiting for task A's atomic context, the event loop is
blocked. Task A cannot resume to release the atomic context, producing a
deadlock.

Therefore regular `db0.atomic()` must not be used from async task code. The
async API must wait using `await`, not by blocking the OS thread.

## Async API

Add a Python API:

```python
async with db0.async_atomic() as atomic:
    ...
```

The returned `atomic` object exposes the same explicit rollback operation as the
sync context:

```python
async with db0.async_atomic() as atomic:
    obj.value = 10
    atomic.cancel()
```

Context manager behavior:

- Normal exit closes the native atomic context.
- Exceptional exit cancels the native atomic context.
- `atomic.cancel()` is idempotent with the same semantics as sync atomic.
- Nested `async_atomic()` in the same task is allowed.
- Concurrent `async_atomic()` blocks in the same event loop are serialized by
  awaiting an asyncio lock.
- Concurrent threaded atomic operations are still serialized by the native
  atomic runtime.

## Python Coordination Layer

Implement `AsyncAtomicManager` in `dbzero/dbzero/atomic.py`.

Responsibilities:

- Require a running asyncio task in `__aenter__`.
- Acquire a per-event-loop async lock before opening the native atomic context.
- Track same-task nesting with `contextvars.ContextVar`.
- Call a native `begin_async_atomic()` entrypoint after the async lock is held.
- Close or cancel the native atomic context in `__aexit__`.
- Release the per-event-loop async lock when the outermost async atomic block
  exits.

The per-loop lock registry can be a `weakref.WeakKeyDictionary` guarded by a
small `threading.Lock`:

```python
_async_atomic_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]
```

The nesting state should include both depth and owner task identity. ContextVars
are copied into child tasks, so depth alone is not sufficient. A child task must
not accidentally inherit the parent's right to bypass the lock.

## Native Entry Points

Keep the existing native entrypoint for synchronous atomic:

```cpp
begin_atomic()
```

Add a distinct async entrypoint:

```cpp
begin_async_atomic()
```

`begin_atomic()` rejects calls from an asyncio task before acquiring the native
atomic lock. `begin_async_atomic()` allows async task execution because the
Python async coordinator has already serialized same-loop async users.

The native runtime should keep the current optimization pattern:

- Fast path: if no atomic operation is active, avoid additional ownership work
  on ordinary mutations.
- Active path: compare the current execution identity with the atomic owner.
- Owner path: allow nested/reentrant atomic behavior.
- Non-owner same Python thread and different async context: raise immediately.
- Non-owner different OS thread: wait on the native atomic mutex.

## Async Task Detection

The Python binding needs a helper that determines whether the current execution
is inside an asyncio task.

Preferred implementation is native-side detection with CPython APIs, exposed as
a small internal predicate:

```python
db0._in_async_task() -> bool
```

The implementation may call the Python-level `asyncio.current_task()` from C++
or use equivalent CPython-visible state if available. It must treat "no running
event loop" as `False`.

`begin_atomic()` uses this predicate and raises if it returns `True`.
`begin_async_atomic()` uses it and raises if it returns `False`, so sync code
does not accidentally use the async API.

The existing atomic execution identity should continue to include:

- native thread id,
- Python thread state id,
- Python context identity.

That identity is used for active mutation checks and deadlock avoidance. The
async task predicate is an API admission check, not the only safety mechanism.

## Mutation Boundary Behavior

When any mutating Python API enters dbzero while an atomic operation is active:

1. If the current execution owns the atomic context, proceed.
2. If the owner is on a different OS thread, wait for the native atomic mutex.
3. If the owner is on the same Python thread but a different async context,
   raise `RuntimeError`.

This prevents unguarded async mutations from deadlocking the event loop:

```python
async def owner(obj):
    async with db0.async_atomic():
        obj.x = 1
        await asyncio.sleep(0)

async def unguarded(obj):
    obj.y = 2  # raises while owner is suspended inside async_atomic
```

Code that needs async serialization must use `async_atomic()`:

```python
async def participant(obj):
    async with db0.async_atomic():
        obj.y = 2
```

## Interaction With Existing Atomic Runtime

`AtomicRuntime` remains the native serialization point for all atomic
operations. The async layer does not replace it. Instead:

- The Python async lock prevents same-event-loop tasks from blocking in native
  code.
- The native atomic mutex serializes cross-thread atomic operations.
- The native owner check catches accidental unguarded async access.
- The total active-depth counter preserves the fast path for normal code.

`Workspace::commit()` and autocommit must continue to serialize against active
atomic operations. A commit attempted by the active atomic owner remains invalid
and should raise. A commit from another OS thread waits. A commit from another
async task on the same Python thread raises rather than blocking the event loop.

## Limitations

`async_atomic()` prevents dbzero from blocking the event loop for cooperative
dbzero atomic users. It does not solve arbitrary application-level async
deadlocks. For example, task A can still hold `async_atomic()` and await task B
while task B waits for the same async atomic lock. That is a user-level circular
wait, not a native dbzero mutex deadlock.

Unguarded dbzero mutations from async tasks are intentionally rejected while
another async task owns an atomic context. This is required to avoid blocking the
event loop.

## Test Plan

Add focused Python tests in `python_tests/test_atomic.py`.

Required tests:

- `db0.atomic()` succeeds in synchronous code.
- `db0.atomic()` raises `RuntimeError` when called from an asyncio task.
- `db0.async_atomic()` raises when used without a running asyncio task.
- `async with db0.async_atomic()` commits changes on normal exit.
- `async with db0.async_atomic()` cancels changes on exception.
- Explicit `atomic.cancel()` inside `async_atomic()` reverts changes.
- Nested `async_atomic()` in the same task works and preserves nested rollback
  semantics.
- Two same-loop tasks using `async_atomic()` serialize without blocking the
  event loop. The first task should `await` inside the block while the second
  waits on the async lock, and both should complete.
- An unguarded mutation from another async task while `async_atomic()` is active
  raises `RuntimeError`.
- A Python thread mutating while an async atomic block is active waits for the
  native atomic operation and then proceeds.
- `db0.commit()` from another async task while `async_atomic()` is active raises
  instead of deadlocking.
- Stress test: several async tasks repeatedly enter `async_atomic()`, perform
  nested commits/cancels/reverts, and `await` inside the block. Mark this as
  slow/stress so it does not run by default.

## Implementation Order

Follow TDD:

1. Add tests that prove `db0.atomic()` is rejected in asyncio tasks.
2. Add the `async_atomic()` API and tests for basic commit/cancel behavior.
3. Add same-loop serialization tests with two async tasks.
4. Add fail-fast tests for unguarded mutations and commit attempts from another
   async task.
5. Wire the native `begin_atomic()` / `begin_async_atomic()` split.
6. Keep performance benchmarks for ordinary read and mutation paths to confirm
   the no-active-atomic fast path remains effectively unchanged.
