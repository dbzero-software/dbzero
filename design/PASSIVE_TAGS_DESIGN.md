# Passive Tags Design

This is a design document for passive tags: tag index entries that behave like normal tags for query matching and removal, but do not hold the tagged object alive.

Passive tags are intended for high-churn labels such as security and access predicates. They reduce mutation overhead by skipping object tag reference bookkeeping. The tradeoff is that passive tag entries may outlive the object they point at and may require periodic cleanup.

## Goals

Passive tags should:

- Use the existing `db0.tags(...).add(...)` and `db0.find(...)` tag grammar.
- Avoid object lifetime coupling: assigning a passive tag must not make the target object durable.
- Require at least one additional positive predicate when queried.
- Match regular tag values transparently in multi-predicate queries.
- Remove through the normal tag removal API, without requiring a passive flag.
- Work for simple and composite tags.
- Preserve existing regular tag behavior and type-tag durability semantics.

Passive tags should not expose a separate public tag type, iterator type, or query API. The only public API addition is the keyword-only `passive` argument on `db0.tags(...)`.

## Python API

`db0.tags` gains a keyword-only boolean argument:

```python
db0.tags(obj, passive=True).add("SECURITY-GROUP")
db0.tags(obj).remove("SECURITY-GROUP")
```

Rules:

- `passive` defaults to `False`.
- `passive=True` affects only subsequent `add` calls through that tag manager.
- `passive` has no effect for `remove`.
- `passive` is keyword-only. Positional use must fail with the normal Python argument error.
- `passive=True` is accepted for direct object targets and query targets.

Example:

```python
obj = MemoType(123)

db0.tags(obj, passive=True).add("SOME-NEW-TAG")

# Re-application does not promote the passive tag to a regular tag.
db0.tags(obj).add("SOME-NEW-TAG")

# Passive-only lookup is invalid.
db0.find("SOME-NEW-TAG")          # raises

# A passive tag may be queried together with another positive predicate.
db0.find(MemoType, "SOME-NEW-TAG")

# Removal does not need passive=True.
db0.tags(obj).remove("SOME-NEW-TAG")
```

## Behavioral Semantics

Passive tag assignment creates an index relationship from tag to object address, but does not create a tag reference on the object. If an otherwise unreachable object has only passive tags, it remains eligible for normal drop behavior.

Regular and passive tag entries share the same logical tag identity. A query for `"A"` should match both the regular `"A"` entry and the passive `"A"` entry when the query is valid.

If the same object already has a passive entry for a tag, adding the regular version of the same tag is a no-op for the relationship. It must not convert the entry to regular and must not increment the object's tag reference count. This is the "passive obscures regular" rule.

If the same object already has a regular entry for a tag, adding the passive version of the same tag should also be a no-op for the relationship. The existing regular durability should remain regular. This avoids weakening an existing durable tag through an accidental passive add.

Removing a tag removes both passive and regular forms for that object and logical tag. Removal should release regular tag references only for entries that were actually regular.

## Query Semantics

A query that contains only a passive-capable tag predicate is invalid because the index may contain stale object addresses that no longer refer to live objects. The caller must provide at least one additional positive predicate that can validate the object set, such as a memo type or another regular tag.

Invalid:

```python
db0.find("SECURITY-GROUP")
db0.find(db0.as_tag("SECURITY-GROUP"))
db0.find(["SECURITY-GROUP", "OTHER-SECURITY-GROUP"])
```

Valid:

```python
db0.find(MemoType, "SECURITY-GROUP")
db0.find("REGULAR-TAG", "SECURITY-GROUP")
db0.find(db0.as_tag("ACCESS", account), MemoType)
```

The implementation cannot know from the public tag value alone whether all matching entries are regular or passive unless it inspects the index. The conservative rule should therefore be syntactic and index-aware:

- A root query must contain at least one positive non-passive predicate before any passive-capable predicate may be used.
- Explicit type predicates count as non-passive positive predicates.
- Direct memo-object predicates count as non-passive positive predicates.
- `ObjectIterable` native predicates count as non-passive positive predicates only if their query planning metadata says they are anchored by a non-passive predicate.
- Negated predicates do not count.

For user-facing errors, prefer a clear `InputException` message such as:

```text
Passive tag queries require at least one non-passive positive predicate
```

## Storage Representation

Short tags currently fit into the low bits of a 64-bit `ShortTagT`. Passive tags should use the highest-order bit as a persisted passive flag:

```cpp
static constexpr ShortTagT PASSIVE_TAG_BIT = 1ull << 63;
static constexpr ShortTagT SHORT_TAG_VALUE_MASK = ~PASSIVE_TAG_BIT;
```

All logical comparisons and lookups must ignore `PASSIVE_TAG_BIT`. Storage operations that need to distinguish passive from regular entries must preserve it. This means the stored key may be passive or regular depending on which form was inserted first, while searches and duplicate detection treat both keys as the same logical tag.

Helpers should centralize this logic in `TagIndex` or a small tag-id helper:

```cpp
constexpr bool isPassiveTagKey(ShortTagT tag);
constexpr ShortTagT makePassiveTagKey(ShortTagT tag);
constexpr ShortTagT stripPassiveTagKey(ShortTagT tag);
constexpr bool sameLogicalTag(ShortTagT lhs, ShortTagT rhs);
```

The existing note says regular short tags use only the low 50 bits. The implementation should still guard this explicitly with a debug assertion or static invariant wherever short tag keys are constructed, because passive tags depend on the high bit being available for metadata.

Long tags should not need a separate passive encoding for the initial implementation. A foreign tag that cannot be represented as a local short tag can continue through the long-tag path as regular-only. If passive foreign tags are required later, the passive flag should be applied to the local short component inside the `LongTagT` pair only after verifying that full-text index comparison can mask that component consistently.

## Index Layout

The preferred implementation is to store passive and regular forms in the existing short-tag `FT_BaseIndex`, using the high bit to distinguish the persisted entry mode:

- Regular entry: `tag`.
- Passive entry: `tag | PASSIVE_TAG_BIT`.
- Logical query for `tag`: search with `stripPassiveTagKey(tag)` and compare with `sameLogicalTag`.
- Logical duplicate detection: treat regular and passive keys as equal.
- Logical removal for `tag`: remove the matching stored key whether it is regular or passive.

This preserves the current index structure and avoids a second full-text index. It does require the full-text index comparison points used by `TagIndex` to mask `PASSIVE_TAG_BIT`. If those comparison points are generic templates, prefer a tag-key comparator or traits parameter over changing all `FT_BaseIndex` users. Non-tag indexes must not start masking high bits accidentally.

Iterator metadata and serialized query tag sequences should store logical, stripped tag values. Reopened queries must resolve through the same masked comparison rules, not depend on whether the original stored key happened to be passive.

## Object Lifetime And Reference Counting

Regular tag flushing currently increments the tagged object reference count through `add_tag_callback` and decrements it through `remove_tag_callback`. Passive entries must bypass those object callbacks.

Implementation approach:

- Add a separate passive batch operation for short tags, for example `m_batch_op_short_passive`.
- Flush passive entries with callbacks that update tag-token references as needed but do not call `LangToolkit::incRefMemo(true, ...)` or `LangToolkit::decRefMemo(true, ...)`.
- Keep regular `m_batch_op_short` behavior unchanged.
- Type tags must remain regular-only.
- Passive tag assignment must not trigger auto-assignment of default type tags when it is the object's first tag.

Tag string/token reference counting still matters. A passive tag entry should keep the tag token definition alive for as long as the passive index entry exists, otherwise queries by that tag value may become unresolvable. Object lifetime and tag-token lifetime are separate concerns.

## Duplicate And Obscuring Rules

The duplicate rule follows from masked comparison: regular and passive forms are the same logical index key, but the persisted key keeps the mode of the first successful insert.

Required behavior:

- Adding passive when regular exists: no new passive entry; leave regular state unchanged.
- Adding regular when passive exists: no new regular entry; leave passive state unchanged.
- Adding the same mode twice: no-op, as existing tag add semantics already imply.
- Removing: remove the one stored physical key for the logical tag.

If `FT_BaseIndex::BatchOperationBuilder` only detects duplicates through exact integer equality, it must be made passive-aware for tag indexes or `TagIndex` must explicitly check the alternate key before enqueueing an add. It should check both persisted index state and pending batch state. Pending checks are important for sequences like:

```python
db0.tags(obj, passive=True).add("A")
db0.tags(obj).add("A")
```

before a flush.

## Composite Tags

Passive tags are allowed as composite tags. The passive flag applies to the leaf relationship, not the composite path keys.

For a composite tag such as `("ACCESS", account)`:

- The path keys (`"ACCESS"`) identify nested `TagIndex` instances and should stay unflagged.
- The leaf key (`account`) is stored as regular or passive according to the tag manager's add mode.
- Query planning for the composite leaf should use stripped logical keys and masked comparison.
- Removal should remove the stored leaf form whether it is regular or passive.

This keeps the nested index map stable. If passive bits were applied to path keys, the same logical composite prefix could create separate nested indexes and break query equivalence.

Composite query validation follows the same passive-predicate rule. A query made only of one passive-capable composite tag is invalid; adding a type or another positive non-passive predicate makes it valid.

## Python Binding Changes

`makeObjectTagManager` currently accepts only `METH_FASTCALL` positional arguments. To support a keyword-only `passive` argument, change the module method registration and parser:

- Register `db0.tags` as `METH_FASTCALL | METH_KEYWORDS`.
- Parse positional targets as the existing object/query target list.
- Parse keyword-only `passive` as `bool`, defaulting to `False`.
- Reject unknown keywords.

Thread the parsed flag through:

- `PyObjectTagManager`.
- `ObjectTagManager::makeNew`.
- `ObjectTagManager` constructor.
- `ObjectTagManager::add`.
- `ObjectTagManager::ObjectInfo::add`.
- `TagIndex::addTags` and composite add helpers.

Do not thread `passive` through removal.

## C++ API Changes

Add passive-aware overloads rather than changing every call site implicitly:

```cpp
void TagIndex::addTags(ObjectPtr memo_ptr, ObjectPtr const *lang_args, std::size_t nargs, bool passive);
void TagIndex::addTag(ObjectPtr memo_ptr, ShortTagT tag_addr, bool is_type, bool passive = false);
std::shared_ptr<TagIndex> addComposite(ObjectPtr memo_ptr, ShortTagT key);
```

`is_type` and `passive` must never both be true. Assert this in debug builds and reject it if a public path can trigger it.

Type-tag assignment in `ObjectTagManager::ObjectInfo::add` should run only for regular adds:

```cpp
if (!passive && !m_has_tags) {
    // assign default type tags
}
```

`m_has_tags` currently reflects durable tag refs. Passive-only objects should continue to report false for this field so that the first later regular tag still assigns default type tags.

## Query Planning Changes

`TagIndex::addIterator` should track whether each query branch is passive-capable and whether the root query has a non-passive positive anchor.

One practical structure is:

```cpp
struct QueryPredicateInfo {
    bool contributes_results = false;
    bool may_read_passive_entries = false;
    bool is_non_passive_anchor = false;
};
```

For simple short tags:

- Add an iterator for the stripped logical tag key. The underlying tag-index comparison must match either stored form.
- Set `may_read_passive_entries = true`.
- Set `is_non_passive_anchor = false` unless the argument is a type or direct memo object.

For explicit type filters:

- Query only the regular type-tag key.
- Set `is_non_passive_anchor = true`.

For direct memo-object predicates:

- Use the fixed-key iterator.
- Set `is_non_passive_anchor = true`.

For nested OR/AND/list/tuple queries:

- Propagate `may_read_passive_entries`.
- Propagate `is_non_passive_anchor` only when the branch semantics guarantee a positive anchor is applied to every returned object. An AND tuple may propagate an anchor from any positive child. An OR list should not count as a root anchor unless every OR branch has a non-passive anchor.

After root planning, reject if `may_read_passive_entries` is true and no root non-passive anchor exists.

## Cleanup

Passive tags are intentionally not cleaned when the underlying object is dropped. This can degrade index size and query performance.

The initial implementation may defer cleanup, but should keep enough structure to add it later:

- Passive entries are physically distinguishable through `PASSIVE_TAG_BIT`.
- Query execution should already intersect passive results with another live-object predicate, preventing stale entries from being exposed.
- A future cleanup task can scan passive entries, test whether the object address still names a live object, and remove stale entries in batches.

Do not perform cleanup opportunistically inside normal `find` iteration in the first implementation. Query paths may run against read-only snapshots, and mutating cleanup there would complicate snapshot consistency.

## Tests

Follow TDD and add Python tests before implementation.

Core Python tests:

- `db0.tags(obj, passive=True).add("A")` does not keep an otherwise unreferenced object alive across flush/reopen.
- `db0.find("A")` raises after passive assignment.
- `db0.find(MemoType, "A")` returns the object while it is still live.
- Passive then regular add remains passive: dropping all ordinary references still drops the object.
- Regular then passive add remains regular: the object remains durable as it did before.
- `db0.tags(obj).remove("A")` removes a passive tag.
- `db0.tags(obj).remove("A")` removes a regular tag even if called after passive-capable operations.
- `passive` is keyword-only and unknown keywords are rejected.
- `passive=True` on a query target applies tags to every object in the query.

Composite tests:

- Passive composite tag can be added and found with an explicit type predicate.
- Passive composite-only query raises.
- Passive then regular composite add does not promote the relationship.
- Composite removal removes passive entries without requiring `passive=True`.

Native-focused tests, if C++ sources are modified:

- Short tag helper masking.
- Query matching from logical key to either stored regular or passive physical key.
- Duplicate obscuring checks across pending and persisted entries.
- Passive flush does not call object inc/dec tag ref callbacks.

Before final handoff for an implementation that changes native code, run a release build with C++ tests and the relevant Python tests, then a debug build with the relevant Python tests, per repository policy.

## Open Questions

- Whether passive long/foreign tags should be supported in the first implementation. The current design treats them as regular-only unless a safe masked representation is added for `LongTagT`.
- Whether serialized query metadata must record that a query branch may read passive entries. If serialized queries can reopen passive-capable iterators without going through normal planning, the passive anchor validation must be preserved in serialization.
- Whether a public cleanup API is needed immediately or a background/internal maintenance hook is sufficient for the first release.
