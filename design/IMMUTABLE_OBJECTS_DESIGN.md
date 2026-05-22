# Immutable Objects Design

This is a temporary design document for agentic development of immutable objects. Remove it after the feature is complete and the durable design has moved into permanent code comments or project documentation.

## Goal

Immutable memo objects can be optimized because, after construction, their fields cannot be modified. The only permitted post-construction changes are external reference and tag bookkeeping. This lets dbzero use a compact object layout that avoids mutable-object structures and can embed selected nested values directly into the root allocation.

The Python programming model should remain transparent: immutable embedded objects should behave like normal memo instances for reads, references, weak references, tags, and tag-based lookup.

## Layout Changes

Immutable objects may deviate from the regular memo-object layout in these ways:

- The KV-map is eliminated because adding fields after construction is not allowed.
- Nested tuples, strings, and byte arrays may be embedded directly in the object structure to avoid extra references and allocations.
- Other immutable nested memo objects may be embedded.
- Immutable collections such as `list`, `dict`, and `set` may be embedded into the root object when the cost model supports it.

The layout keeps:

- `POS-VT` and `INDEX-VT` segments, unchanged, for fixed-size members such as dates, datetimes, memo references, floats, or low-fidelity buffers.
- A new `OFFSET-MAP` structure, based on the `o_dict` implementation, mapping field index to offset. Both index and offset are stored as packed `uint32`.
- A variable-length member block (`VL-BLOCK`) immediately after the `OFFSET-MAP`.

Offsets in `OFFSET-MAP` are calculated from the beginning of `VL-BLOCK`. Variable-length member types are stored in `VL-BLOCK` immediately before their contents. This allows dbzero to calculate the addresses of embedded nested members without needing the mutable KV-map.

## Embedding Cost Model

Embedding is not always the best storage model. It can reduce construction work and allocation count, but it can also make retrieval more expensive because fetching the root object may fetch embedded fields that the caller never reads.

Use this criterion:

```text
SavedCost > EmbeddedCost
```

Where:

```text
SavedCost =
      SeparateStorageBytes
    + AllocationsAvoided * AllocationCost

EmbeddedCost =
      EmbeddedBytes
    + ExtraPagesFetched * PageFetchCost
    + AddressabilityCost
    + ViewCost
```

Suggested constants:

- `AllocationCost = 64b`
- `PageFetchCost = page_size / 2`
- `AddressabilityCost = 128` for nested memo objects only
- `ViewCost = 64` for simple nested objects
- `ViewCost = 128` for collections

Inputs to consider:

- Relative size of the embedded element as a proportion of the entire object.
- Absolute size of the embedded element.
- Allocation savings, especially for collections like sets and dicts.
- Administrative storage savings, including avoided pointers and headers.
- Expected read patterns, especially whether large embedded fields are commonly skipped.

## Nested Object References

Embedded nested objects must not be distinguishable from regular memo objects in Python code.

Example:

```python
@db0.memo
@dataclass
class InnerData:
    inner_value: int

@db0.memo
class OuterData:
    value: int
    inner_data: InnerData

    def __init__(self, value, inner_value):
        self.value = value
        self.inner_data = InnerData(inner_value)

outer = OuterData(1, 2)

# Reference to embedded instance.
other.ref = outer.inner_data

# Weak reference to an embedded instance.
other_px.long_ref = db0.weak_proxy(outer.inner_data)

# Assigning tags.
db0.tags(outer.inner_data).add("INNER")

# Lookup by tags may retrieve the inner reference.
db0.find(InnerData, "INNER")
```

Implementation requirements:

- Field retrieval returns an object view of the root object that exposes only the nested fields for read access.
- The view must maintain the lock or lifetime guard of the top-level object while nested fields are accessed.
- References to embedded objects point to a memory location inside the root allocation and also carry the nested member offset. The offset may be deeply nested.
- Embedded object offsets are byte offsets relative to the persisted `o_immutable_object` data structure overlaid on the root allocation, not relative to the C++ `ObjectImmutableImpl` wrapper instance.
- The lifecycle of an embedded object is tied to the root instance because the root owns the allocation containing the full embedded tree.
- The embedded member is identified by its own address, but that address is inside the allocation and is not the allocation start.
- The allocator must be able to recover allocation metadata from an inner address. This allows embedded object addresses to use the same 50-bit representation as regular object addresses.
- A parent object can still be referenced by the parent allocation address.
- Strong references to embedded objects increment the root allocation's reference count, and deleting the holding object must decrement that same root reference by resolving the embedded address back to its allocation base.
- Root immutable objects store an exact compact index of valid nested embedded-object offsets. Lookup by offset must validate against this index and raise a bad-address error for invalid, out-of-range, or non-object offsets.
- The offset index uses packed integer encoding grouped by packed size class so offsets remain compact while supporting logarithmic exact membership checks. Most offsets are expected to fit in 3-4 packed bytes, but the representation must support larger offsets.

## Object Views

Nested embedded objects require specialized views rather than independent opened objects.

Object views should:

- Expose the same read interface expected for a memo object of the nested type.
- Resolve fields relative to the nested object offset inside the root allocation.
- Keep the root object allocation and lock valid for the duration of access.
- Reject mutation except for operations explicitly allowed for immutable objects, such as reference and tag bookkeeping.
- Support reference creation, weak proxy creation, and tag operations using the nested address.

Collection views should follow the same model but account for collection-specific traversal and lookup costs. Use the higher `ViewCost` constant for collection embedding decisions.

## Embedded Simple Sets

The first embedded-set slice is `o_set`, a variable-length overlaid object for simple immutable set values. It uses the same tagged embedded item representation as `o_tuple_item`, so payload bytes live inside the set allocation rather than in side allocations.

Layout:

```text
o_set
  packed count
  packed element_block_byte_size
  packed bucket_block_byte_size
  o_tuple_item element[count]
  uint32 bucket_offset_plus_one[capacity]
  o_tuple bucket[occupied_slots]
```

Construction removes duplicate simple descriptors before arranging members. The first occurrence determines physical order in the main element stream. `count` stores the unique item count and `element_block_byte_size` stores the exact byte extent of that stream. The hash index is a direct bucket table: slot `hash % capacity` stores `bucket_block_offset + 1`, and `0` means empty. Each occupied slot points to an embedded `o_tuple` containing the elements that landed in that hash bucket. Lookup reads one slot and scans only that bucket tuple to resolve collisions. `sizeOf()` and `safeSizeOf()` rely on the stored element byte size, count-derived index size, and stored bucket byte size for the total extent.

## Deferred Materialization

Embedding pre-existing immutable dbzero instances is allowed only when the instance has no external references yet, because its final durable address is not known until it is embedded or otherwise materialized.

Introduce deferred materialization for immutable objects:

- Create immutable instances initially without a durable external address when possible.
- Materialize the instance when it is first externally referenced or embedded.
- If embedded, transform the Python wrapper into an object view whose lifetime is tied to the containing root object.
- If externally referenced first, materialize it as a standalone durable object and store normal references to it.

Simple constructor example:

```python
outer = OuterData(1, InnerData(3))
```

Expected behavior:

- `InnerData` is created without external references.
- `OuterData` construction sees that the inner value has no external references.
- `InnerData` is embedded into `OuterData`.

Pre-bound local example:

```python
inner = InnerData(3)
outer = OuterData(1, inner)
```

Expected behavior:

- `InnerData` is created without durable external references. Only the Python local reference exists.
- `OuterData` embeds `inner`.
- The `inner` Python wrapper is transformed in place into an object view tied to `outer`.
- Python code continues to behave as if `inner` were a regular immutable memo object.

## Development Guidance

Follow TDD for this feature. Start with Python behavior tests for transparent semantics, then add C++ tests for native layout, allocator/address handling, and view behavior.

Recommended implementation slices:

1. Define immutable-object construction semantics and prevent post-construction field mutation.
2. Add deferred materialization for immutable memo instances.
3. Add the immutable root layout without embedded nested objects.
4. Add `OFFSET-MAP` and `VL-BLOCK` handling for variable-length members.
5. Add object views for embedded nested memo objects.
6. Add reference, weak reference, and tag support for embedded object addresses.
7. Add collection and large variable-length value embedding behind the cost model.
8. Add retrieval benchmarks or focused performance tests for embedding tradeoffs.

Tests should cover:

- Post-construction field assignment is rejected for immutable objects.
- Immutable objects can still be referenced, weak-referenced, tagged, and found by tags.
- Embedded nested memo objects read like standalone memo objects.
- References and weak references to embedded nested objects survive reopening the root object.
- Tag lookup can return embedded nested objects.
- Pre-bound deferred instances transform into views after embedding.
- Previously externalized immutable instances are referenced rather than embedded.
- Large fields are not embedded when the cost model rejects embedding.
- Views keep the root object alive and locked while nested fields are accessed.

Native implementation must preserve existing project conventions:

- Use the established `v_object` constructor pattern.
- Use camelCase for C++ locals, lambdas, and method names.
- Use `modifyExt()` for real durable state mutations from Python wrappers.
- Do not use `const_cast` on `ext()` to call mutating methods.
