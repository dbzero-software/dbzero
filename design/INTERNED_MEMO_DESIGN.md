# Content-Addressed Objects Design

This is a design document for content-addressed immutable memo objects. It is intended to complement `IMMUTABLE_OBJECTS_DESIGN.md` without modifying that original design.

## Goal

Content-addressed objects are immutable memo instances whose durable identity is bound to their contents. Creating the same immutable value more than once should not create multiple durable instances. Instead, dbzero resolves later materializations to the already materialized instance with identical content.

The Python-facing feature is exposed as `intern=True` on immutable memo classes:

```python
@memo(immutable=True, intern=True)
class HomeAddress:
    city: str
    street: str
    country: str

addr_1 = HomeAddress("Marszalkowska", "Warszawa", "Polska")
addr_2 = HomeAddress("Marszalkowska", "Warszawa", "Polska")

assert db0.materialized(addr_1) is db0.materialized(addr_2)
assert db0.materialized(addr_2) is addr_1
```

The term `intern` is used because it describes the user-visible behavior: pooling and de-duplication of equal immutable values.

## User Semantics

`intern=True` may only be set on immutable memo classes. A mutable class cannot be content-addressed because its identity would no longer remain bound to stable contents.

Interning is part of the class materialization contract. It cannot be changed after instances of the class have already been materialized, because existing durable instances would have been created under different identity rules.

When a non-materialized interned instance is materialized, dbzero first looks for an existing instance of the same interned class with identical content:

- If a match exists, the new wrapper is resolved in place to the existing instance.
- If no match exists, the new instance is materialized and submitted to the per-class content index.

This uniqueness guarantee applies both to standalone root objects and embedded immutable objects.

## Class Restrictions

Interned instances may only reference other interned instances. This restriction keeps equality and identity stable across the whole reachable immutable graph.

The allowed field graph is:

- Simple immutable values.
- Embedded immutable values.
- References to interned immutable memo instances.
- Immutable containers whose nested values satisfy the same requirements.

The disallowed field graph is:

- Mutable memo objects.
- Non-interned memo object references.
- Values whose durable comparison depends on mutable external state.

## Content Index

Each interned memo class has an additional per-class `ContentIndex`. The index maps normalized object content to the unique durable address for an existing matching instance.

Conceptually:

```text
ContentIndex[class_id]
  content_hash -> candidate UniqueAddress list
```

The hash is used for lookup, but equality must be confirmed by comparing the normalized binary content of the candidate object. Hash equality alone is never sufficient for uniqueness.

The index stores `UniqueAddress` values for existing interned instances. It does not own those instances and must not increment their reference counts.

## Reference Counting And Lifetime

`ContentIndex` is a discovery structure, not an ownership structure. An interned object with no external references must not be kept alive only because it appears in the content index.

Lifetime rules:

- Interned instances are not added to the normal materialized index solely because of interning.
- `ContentIndex` does not increment the referenced instance.
- When an interned instance's reference count drops to zero, it is removed from `ContentIndex`.
- Stale index entries must not resolve to deleted content. Removal can be eager during ref-count transition or lazily validated during lookup, but lookups must never return a dead instance.

This ensures interning provides de-duplication without turning every interned value into a permanently retained object.

## Materialization Flow

Interned objects use the same deferred materialization phases as other immutable objects, with an added content lookup step:

```text
empty stub
  -> fully initialized non-materialized instance
  -> content lookup on materialization
  -> existing durable instance, new wrapper resolved in place
     or
     new durable root instance submitted to ContentIndex
     or
     embedded object view submitted to ContentIndex
```

Before a newly initialized interned instance becomes durable or embedded, dbzero computes or scans its normalized content and performs a `ContentIndex` lookup for its class.

If the lookup finds an equal object, materialization returns the existing object. The new wrapper becomes a defunct/resolved wrapper whose valid use is subsequent materialization-style resolution to the canonical object.

If the lookup misses, materialization proceeds normally. Once the object has a stable durable address, that address is registered in `ContentIndex`.

## Embedded Interned Objects

Interned instances may be embedded inside immutable root objects, including deeply nested immutable structures. Embedded and referenced forms must compare as equal when they represent the same interned value.

This means equality and index matching are based on normalized object content, not on whether a nested value is physically embedded or referenced.

Example:

```python
addr_1 = HomeAddress("Marszalkowska", "Warszawa", "Polska")
company = Company(address=addr_1)

addr_2 = db0.materialized(HomeAddress("Marszalkowska", "Warszawa", "Polska"))
```

If `Company` is immutable and embeds `addr_1`, `addr_2` must resolve to the same logical interned `HomeAddress` value. If the canonical instance is embedded, the resolved result may be a view into the containing root allocation.

## Embedded Versus Referenced Equality

References are treated on par with embedded contents. A field containing an embedded interned object and a field containing a reference to the same interned object compare as equal for content-addressing purposes.

The comparison is performed against the binary contents of the `o_embedded_object` inner structure after normalizing reference fields. A referenced interned object contributes the same normalized bytes as the embedded content of that object.

Implementation requirements:

- Content comparison must be class-aware; equal bytes for different memo classes are not interchangeable.
- The normalized representation must be stable across process restarts.
- Reference fields to interned objects must be expanded or canonicalized before comparison.
- Embedded-object offsets must not leak into content equality, because offset is placement detail rather than value content.

## Lazy Index Updates

`ContentIndex` operations are lazy, following the same buffering model as `TagIndex`.

Expected behavior:

- New index insertions are buffered during materialization.
- Removals caused by zero reference count are buffered.
- The persistent index collection is updated on flush.
- Reads must observe pending buffered updates in addition to persisted entries.

The lookup path must account for both persisted and pending state so uniqueness is preserved before and after flush.

## Uniqueness Guarantee

Content-addressing provides a strong uniqueness guarantee. Every materialization of an interned object must go through content lookup before a new durable identity can be exposed.

The guarantee covers:

- Explicit `db0.materialized(obj)` calls.
- Implicit materialization by storing an interned object in another durable object.
- Embedded immutable instances.
- Deeply nested embedded instances.
- Reopened objects after process restart.

The main caveat is retrieval cost. If the canonical interned object is first materialized as an embedded object inside a large root object, future lookups may need to pull the containing root allocation to compare or expose the embedded value.

Users can avoid surprising retrieval costs by explicitly materializing small interned values before embedding them:

```python
addr_1 = HomeAddress("Marszalkowska", "Warszawa", "Polska")
person = Person(address=db0.materialized(addr_1))
```

## Materialization Scenarios

### Referenced Before Duplicate Construction

```python
addr_1 = HomeAddress("Marszalkowska", "Warszawa", "Polska")
user(address=addr_1)

addr_2 = db0.materialized(HomeAddress("Marszalkowska", "Warszawa", "Polska"))
```

Expected behavior:

- `addr_1` starts as a fully initialized non-materialized instance.
- Passing it to `user` externally references it, so it is materialized.
- Materialization registers it in `ContentIndex`.
- `addr_2` resolves to the same dbzero object by looking up the content index.

### Embedded Before Duplicate Construction

```python
addr_1 = HomeAddress("Marszalkowska", "Warszawa", "Polska")
company = Company(address=addr_1)

addr_2 = db0.materialized(HomeAddress("Marszalkowska", "Warszawa", "Polska"))
```

Expected behavior:

- `addr_1` starts as a fully initialized non-materialized instance.
- If `Company` is immutable and the embedding cost model accepts the field, `addr_1` is embedded into `company`.
- The interned address value is registered in `ContentIndex`.
- `addr_2` resolves to the canonical interned value, which may be represented as an embedded-object view.

## Development Guidance

Follow TDD for this feature. Start with Python behavior tests for the user-visible guarantees, then add native tests for index storage, normalization, lifetime, and embedded-object lookup.

Recommended implementation slices:

1. Validate decorator semantics: `intern=True` requires `immutable=True` and cannot change after materialization.
2. Add a per-class `ContentIndex` abstraction with buffered insert/remove behavior.
3. Add normalized content hashing and equality for immutable root objects with simple fields.
4. Route interned materialization through lookup before durable identity exposure.
5. Add wrapper in-place resolution when a duplicate is found.
6. Add reference-count driven `ContentIndex` removal.
7. Extend normalized comparison to interned references and embedded objects.
8. Add embedded interned object registration and lookup.
9. Add restart/flush tests for persisted index behavior.
10. Add retrieval-cost tests or benchmarks for embedded canonical instances in large root objects.

Tests should cover:

- `intern=True` is rejected without `immutable=True`.
- Interning cannot be enabled or disabled after class instances exist.
- Two independently constructed equal interned objects materialize to the same object.
- Unequal interned objects materialize to distinct objects.
- Duplicate materialization resolves the later wrapper to the canonical instance.
- `ContentIndex` does not keep an object alive after all external references are gone.
- A later equal value is re-created after the prior canonical instance is dropped.
- Interned objects cannot reference non-interned memo instances.
- Embedded interned values and referenced interned values compare as equal.
- Deeply nested interned embedded values resolve through `ContentIndex`.
- Buffered index updates are visible before flush.
- Persisted index entries resolve correctly after reopening.
- Stale index entries are ignored or removed and never return dead objects.

Native implementation must preserve existing project conventions:

- Use the established `v_object` constructor pattern.
- Use `db0::o_ext<Derived, BaseOverlay, VER, STORE_VER>` for variable-size overlaid inheritance.
- Use camelCase for C++ locals, lambdas, and method names.
- Use explicit double-negation checks such as `if (!!obj)` when a project type supports `operator!()`.
- Use `modifyExt()` for real durable state mutations from Python wrappers.
- Do not use `const_cast` on `ext()` to call mutating methods.
- Use `Py_FOR(item, iterator)` and `PySafe_*` helpers for Python C API iteration and container writes.
