# Data Filters Design

This is a design document for dbzero data filters: a security mechanism that prevents unauthorized access to durable objects through `find`, deserialized queries, `fetch`, and object references.

## Goal

Data filters let application developers declare access-controlled memo types and provide an execution-context predicate that dbzero applies automatically at every application-visible object access boundary. The application should not need to duplicate fragile authorization checks around each query, fetch, or field dereference.

The mechanism is intentionally conservative. Once a type is marked as access controlled, dbzero must assume that every access to instances of that type needs filtering unless explicitly running in debug mode with a null predicate.

## Python API

Data filters are initialized process-locally:

```python
from contextvars import ContextVar

predicate: ContextVar[db0.ObjectIterable | None] = ContextVar("predicate")

db0._init_data_filter(
    predicate,
    prefix=get_current_prefix(),
    mode="DEBUG",
)
```

Arguments:

- `predicate`: a `ContextVar` that yields the current filtering predicate. The value is an `ObjectIterable` query expression or `None`.
- `prefix`: an optional prefix name or collection of prefix names where filtering is enabled. `None` means all current prefixes and prefixes opened later.
- `mode`: optional mode string. The default is `RELEASE`, and `None` is treated the same as `RELEASE`. `DEBUG` must be specified explicitly. `DEBUG` allows a null predicate and treats it as filtering disabled. `RELEASE` requires a non-null predicate whenever an access-controlled object is read.

Access-controlled memo types are declared with the memo decorator:

```python
@db0.memo(access_control=True)
@dataclass
class RestrictedData:
    value: str
```

A predicate is not limited to a plain list of tags. It can be any dbzero `ObjectIterable` query expression, including a complex tag-based statement composed from tags, object references, nested `find` queries, alternatives, negation, and other query operators.

A simple predicate can grant access through an explicit tag relation:

```python
pred = db0.find(db0.as_tag("GRANT-ACCESS", account))
predicate.set(pred)
```

A more selective predicate can combine multiple query clauses:

```python
pred = db0.find(
    [
        db0.as_tag("GRANT-ACCESS", account),
        db0.as_tag("GRANT-ACCESS", "PUBLIC"),
    ],
    db0.no(db0.as_tag("DENY-ACCESS", account)),
)
predicate.set(pred)
```

## Activation Model

Initialization only enables data filtering for selected prefixes. It does not make every type restricted.

Filtering is active for an object access when:

- The target prefix is configured for data filtering.
- The access target is restricted.
- The operation is not in `DEBUG` mode with a null predicate.

An access target is restricted when any of these are true:

- The target type is directly decorated with `access_control=True`.
- The target type inherits from a base type decorated with `access_control=True`.
- The queried type has any descendant decorated with `access_control=True` that may appear in that query's result set.

The descendant-to-base rule is necessary for typed base queries. If any `MemoBase` descendant defines access restriction, the whole base query must be evaluated with the current predicate. This effectively filters inaccessible objects out of the complete result set, rather than trying to filter only restricted descendants after results have already been produced. Restricted derived classes therefore make their queryable bases require the same protections.

## Security Invariants

The implementation must preserve these invariants:

- Typeless `find` is not allowed for filtered prefixes.
- An access-controlled type cannot be queried unless data filtering has been initialized for the relevant prefix.
- An access-controlled object cannot be fetched unless the current predicate includes it.
- An access-controlled object cannot be exposed through an application-visible durable reference unless the current predicate includes it.
- Deserialized queries must be checked as if they had been constructed in-process.
- Null predicates are allowed only in `DEBUG` mode.
- Missing initialization, missing predicates, and rejected objects raise `PermissionError`, except for public UUID fetch predicate exclusion.
- UUID fetch must not distinguish between a missing object and an unauthorized access-controlled object.

These checks are part of dbzero's access path, not a convenience wrapper around public APIs.

## `find` Behavior

When `db0.find(...)` is called, dbzero first determines the requested prefix and whether data filtering is enabled for that prefix.

If prefix-level filtering is enabled:

- A query without an explicit type raises `PermissionError`.
- This applies to direct calls and deserialized queries.

If a query has an explicit type, dbzero checks whether that type requires access control. This type check happens even when prefix-level data filtering is disabled. If the type is access controlled but data filters are not initialized for the prefix, dbzero raises `PermissionError` explaining that data filtering must be initialized before the query can run.

If the type is access controlled and filtering is initialized:

- Resolve the predicate from the configured `ContextVar`.
- If the predicate is `None` and mode is not `DEBUG`, raise `PermissionError`.
- If the predicate is `None` and mode is `DEBUG`, run the original typed query without adding a filter.
- If the predicate is non-null, attach it to the query before sorting or range/index ordering is applied.

Conceptually:

```python
query = db0.find(RestrictedData, requested_tags)
secured = db0.find(query, predicate.get())
result = index.sort(secured)
```

The predicate is an additional intersection constraint. It must not widen the result set.

## `fetch` Behavior

`fetch` can begin without knowing the target type, especially for UUID-based fetches. dbzero should unload only enough object metadata to determine the object's type.

Once the type is known:

- If the type is not access controlled, continue with normal fetch behavior.
- If the type is access controlled and filtering is not initialized for the object's prefix, raise `PermissionError`.
- Resolve the predicate from the configured `ContextVar`.
- If the predicate is `None` and mode is not `DEBUG`, raise `PermissionError`.
- If the predicate is `None` and mode is `DEBUG`, continue with normal fetch behavior.
- If the predicate is non-null, test the single object against the predicate.

The authorization check can be represented as:

```python
allowed = db0.find(obj, predicate)
if not allowed:
    raise PermissionError  # or the missing-object error for public UUID fetch
```

The object is returned only if the single-instance filtered query contains that object.

For UUID-based fetch, dbzero must use a non-distinguishing error policy: callers must not be able to tell whether the UUID is missing or whether it names an access-controlled object excluded by the current predicate. The public error should match the normal missing-object fetch behavior for both cases. Internal diagnostics may preserve the distinction, but it must not be observable through the public API.

## Dereference Behavior

Dereference uses the same authorization rule as `fetch`, but the enforcement point is member unload.

In this design, dereference means exposing an object to application code by reading a durable reference stored in another object, such as accessing a memo field or an item inside a dbzero list, dict, or set. It does not mean every internal read of a durable address.

When `unloadMember` resolves a durable object reference for application-visible field or collection access:

- Determine the referenced object's type.
- If the type is not access controlled, return the reference normally.
- If the type is access controlled, resolve and apply the current predicate exactly as in `fetch`.
- Raise `PermissionError` if the predicate is required but missing, or if the predicate does not include the referenced object.

This makes member access, list/dict/set traversal, embedded object references, weak proxies, and other application-visible reference-based access paths follow the same policy as explicit `fetch`.

Internal maintenance operations must not be blocked by data filters merely because they read object addresses or object metadata. This includes reference counting, garbage collection, tag maintenance, deletion checks, index maintenance, serialization/deserialization internals, flush/reopen internals, consistency repair, and other storage-engine bookkeeping that does not expose the protected object to application code. These paths may still need to read restricted objects in order to preserve storage correctness.

## Prefix Semantics

`prefix` controls where data filtering is enabled:

- A single prefix enables filtering for that prefix.
- Multiple prefixes enable filtering for each listed prefix.
- `None` enables filtering globally, including prefixes opened after initialization.

The implementation should store both an explicit enabled-prefix set and a global-enabled flag. Access checks must use the prefix of the object or query, not only the current default prefix.

## Predicate Resolution

Predicate resolution should be centralized so `find`, `fetch`, and dereference share one error policy.

The resolver should return one of three states:

- Filtering disabled by debug null predicate.
- A concrete `ObjectIterable` predicate.
- A `PermissionError` with an operation-specific message.

Errors should distinguish:

- Data filters were never initialized for an access-controlled type.
- The current predicate is missing outside debug mode.
- The current predicate does not include the requested object.
- A typeless query was attempted while prefix filtering is enabled.

For public UUID fetch errors, predicate exclusion must be converted to the same observable error used for missing objects. Other operations that already expose a candidate object or typed query context may still raise `PermissionError` for authorization failures.

## Predicate Lifetime

The value stored in the predicate `ContextVar` may be a lazy query object. Data filters must not evaluate that query in whatever ambient prefix, head, or snapshot happens to be active later.

When an access-controlled operation needs a predicate, dbzero should:

1. Read the current `ContextVar` value.
2. If the value is non-null, serialize the predicate query expression at retrieval time.
3. Deserialize the predicate inside the operation context that will perform the access check. This context is either the current head or the specific snapshot being queried.
4. Evaluate the access check using that context-local predicate.

The deserialized predicate may be cached for as long as the operation context is preserved. A cached predicate must not be reused across a different head, snapshot, prefix context, or transaction context where query resolution could produce different results.

This gives predicates snapshot-consistent behavior: a filtered snapshot query uses the predicate as interpreted inside that snapshot, while a head query uses the predicate as interpreted against the current head.

## Query Composition

Data predicates are authorization filters and must be composed as intersections. A predicate may already be a complex query expression; dbzero must treat that expression as a single authorization constraint and intersect it with the requested access query. It should be attached before sorting so ordering cannot influence authorization.

This matters for:

- Tag queries.
- Query objects passed into `find`.
- Deserialized queries.
- `index.sort(...)`.
- `index.range(...)` and other index operations that can unload object groups without directly relying on tags.

Initial implementation can use the existing query-composition path. Later speedups may push predicate filtering into specific index implementations, but those optimizations must preserve the same visible behavior and error policy.

## Type Metadata

Memo type decoration needs persistent metadata for `access_control=True`.

Requirements:

- The decorator accepts `access_control`.
- Class metadata records whether the type is directly access controlled.
- Query planning can determine whether a requested type requires filtering because of direct decoration or restricted descendants.
- Reopened type metadata preserves the flag.
- Redeclaring a type with an incompatible access-control flag after durable instances exist should be rejected or handled consistently with existing type-contract validation.

The base-type propagation rule should be computed through the type hierarchy and cached where practical. Cache invalidation must account for registering new derived types.

## Deserialized Queries

Serialized query payloads must not bypass type and predicate checks. Query deserialization should preserve explicit type information and reject or mark typeless queries so the normal `find` authorization path can raise `PermissionError` under filtered prefixes.

Do not rely on the Python caller to re-wrap a deserialized query with a type or predicate.

## Debug Mode

`mode="DEBUG"` exists to allow incremental adoption and tests that need to initialize the mechanism before a predicate is available. It must be specified explicitly; omitting `mode` or passing `None` selects `RELEASE`.

In debug mode:

- A null predicate disables predicate filtering for access-controlled operations.
- Typeless `find` is still rejected when prefix filtering is enabled.
- Access-controlled typed queries still require data-filter initialization for that prefix.

In release mode:

- A null predicate always raises `PermissionError` for access-controlled operations.
- `RELEASE` is the default when `mode` is omitted.
- Passing `mode=None` is equivalent to `mode="RELEASE"`.

Debug mode should be explicit. Release mode is the secure default.

## Development Guidance

Follow TDD for this feature. Start with Python behavior tests for public access paths, then add native tests for type metadata and query enforcement.

Recommended implementation slices:

1. Add decorator parsing and persistent type metadata for `access_control=True`.
2. Add data-filter initialization state, prefix matching, and predicate resolution.
3. Reject typeless `find` under filtered prefixes.
4. Enforce typed `find` checks and attach predicates before sorting.
5. Enforce `fetch` checks after minimal type discovery.
6. Enforce application-visible dereference checks from `unloadMember`.
7. Cover deserialized query behavior.
8. Add predicate serialization/deserialization for head and snapshot contexts.
9. Add index/range-focused tests and optimize only after behavior is stable.

Tests should cover:

- `_init_data_filter` accepts one prefix, multiple prefixes, and `None`.
- `_init_data_filter` defaults to `RELEASE` when `mode` is omitted or `None`.
- `_init_data_filter` enables null predicates only when `mode="DEBUG"` is explicitly specified.
- Typeless `find` raises `PermissionError` under a filtered prefix.
- Typeless `find` works normally for unfiltered prefixes.
- Typed `find` on an unrestricted type is unchanged.
- Typed `find` on an access-controlled type raises when filters are not initialized.
- Typed `find` on an access-controlled type intersects with the current predicate.
- Predicate queries are serialized on retrieval and deserialized in the current head or snapshot context.
- Deserialized predicates are cached only while the same operation context is preserved.
- A null predicate raises outside `DEBUG`.
- A null predicate is allowed in `DEBUG`.
- Base-type queries are restricted when any derived type is access controlled.
- `fetch(uuid)` authorizes after discovering the object type.
- `fetch(uuid)` uses the same public error for missing objects and unauthorized access-controlled objects.
- `fetch(Type, uuid)` follows the same authorization outcome as `fetch(uuid)`.
- Application-visible member dereference raises when the referenced object is not in the predicate.
- Internal maintenance paths such as reference counting, tag maintenance, index maintenance, and flush/reopen are not blocked by data filters.
- Deserialized typeless queries cannot bypass the typeless-query rule.
- Deserialized typed queries receive the same predicate filtering as direct typed queries.
- Filtering uses the target object's prefix rather than only the current default prefix.
