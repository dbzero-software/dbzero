# Overlaid Types Notes

This note captures the local overlaid-object conventions that matter for embedded tuple/list work.

## Core Model

An overlaid type is a C++ view over bytes already placed in dbzero-managed memory. It is not copied or moved as a normal C++ object. Construction happens with `T::__new(buf, args...)`, which placement-constructs the object at `buf`; reopening happens with `T::__const_ref(buf)` / `T::__ref(buf)`.

There are two broad categories:

- Fixed-size overlays derive from `o_fixed<T>`. They have constant `sizeOf() == true_size_of<T>()`, can be addressed with normal array arithmetic, and can be stored in `o_array<T>`, `o_micro_array<T>`, `o_unbound_array<T>`, or fixed C++ fields.
- Variable-size overlays derive from `o_base<T, VER, STORE_VER>` or `o_ext<T, BaseT, VER, STORE_VER>`. Their physical extent must be found by calling `sizeOf()` / `safeSizeOf()`. They must not be walked with `ptr + index` unless every element is known fixed-size.

`true_size_of<T>()` is `sizeof(T) - std::is_empty<T>()`. This matters for empty CRTP bases like `o_base` and `o_fixed_null`, which are intended to add no payload.

## Required API

A usable overlaid type should provide:

- `static T &__new(void *buf, Args&&...)`, inherited from `o_base` or `o_fixed`.
- `static T &__ref(void *buf)` and `static const T &__const_ref(const void *buf)`, inherited in most cases.
- `static std::size_t measure(args...)`, the exact bytes needed before construction.
- `std::size_t sizeOf() const`, the exact bytes occupied by an existing instance.
- `template <typename BufT> static std::size_t safeSizeOf(BufT buf)`, which scans an existing instance and advances through a bounded buffer for validation.
- `static auto type()`, inherited from `o_base`/`o_fixed`, so `Foundation::Arranger` and `Meter` can instantiate it.

`o_base` supplies default `sizeOf()` as `T::safeSizeOf(this)`, so variable-size types usually implement `safeSizeOf()` and may implement a faster instance `sizeOf()` when they store a total size.

## `safeSizeOf` And Bounds

`safeSizeOf(buf)` has two jobs:

- With a regular pointer, behave like normal size calculation and return the byte extent.
- With `bounded_buf_t` / `const_bounded_buf_t`, advance the checked buffer through the bytes it calculates. If that walks out of bounds, the buffer itself raises through its configured exception path.

`safeSizeOf` should not throw its own out-of-bounds exception. It should calculate/scan the extent and use buffer advancement or child `safeSizeOf` calls to perform validation. This means an implementation must not read fixed header fields from `__const_ref(buf)` until the bytes containing those fields have already been bounds-checked. The local idiom is:

```cpp
template <typename BufT> static std::size_t safeSizeOf(BufT buf)
{
    auto checked = buf;
    checked += super_t::baseSize();      // validates fixed fields for bounded buffers
    auto &self = T::__const_ref(buf);    // now header reads are safe
    ...
}
```

For ordinary pointers, `checked += ...` is plain pointer arithmetic and has no special cost beyond the calculation. For bounded buffers it is the operation that detects truncated input.

`Foundation::SafeSize` follows the same principle for simple static member chains. When code calls:

```cpp
auto safeSize = super_t::sizeOfMembers(buf);
safeSize = safeSize(MemberT::type());
```

the member scanner obtains a bounded sub-buffer through `&buf[sizeSoFar]`; that indexed access validates that the member start is inside the bounded range, and the member's own `safeSizeOf` validates its extent. For raw pointers, the same code falls back to ordinary pointer arithmetic.

For dynamic element streams, prefer an explicit cursor over an accumulator:

```cpp
template <typename BufT> static std::size_t safeSizeOf(BufT buf)
{
    auto start = buf;
    auto cursor = buf;
    cursor += baseSize();              // validate header/fixed fields
    auto &self = T::__const_ref(buf);   // now fixed fields are safe to read
    for (std::uint32_t i = 0; i < self.count(); ++i) {
        cursor += ElementT::safeSizeOf(cursor);
    }
    return cursor - start;
}
```

This keeps the current checked buffer as the single source of truth. Each element receives the same `buf_t` category as the parent (`const_bounded_buf_t` or raw pointer), validates itself, and the parent advances to the next element by exactly the size the element reported.

If a type stores both count and byte-size metadata for a dynamic element stream, choose one source of truth for `safeSizeOf`. Scanning actual elements validates the nested layout and returns the scanned extent; using the stored byte-size validates the declared extent by advancing to it. Do not add a separate direct throw for mismatches in `safeSizeOf`; corruption policy belongs outside the bounds-walking primitive.

## Dynamic Members

`o_base` places fixed C++ fields first, then dynamic members after `baseSize()`:

```cpp
class DB0_PACKED_ATTR Example : public o_base<Example, 0, true> {
    std::uint32_t m_id;

    Example(std::uint32_t id, const std::string &name)
        : m_id(id)
    {
        arrangeMembers()
            (o_string::type(), name);
    }

    static std::size_t measure(std::uint32_t, const std::string &name) {
        return measureMembers()
            (o_string::type(), name);
    }

    template <typename BufT> static std::size_t safeSizeOf(BufT buf) {
        return sizeOfMembers(buf)
            (o_string::type());
    }

    const o_string &name() const {
        return getDynFirst(o_string::type());
    }
};
```

Access to later dynamic members uses the prior member’s `sizeOf()`:

```cpp
const o_string &first() const { return getDynFirst(o_string::type()); }
const o_string &last() const { return getDynAfter(first(), o_string::type()); }
```

The same order must be used in constructor, `measure()`, `safeSizeOf()`, and accessors. Versioned layouts use `[version]` in `arrangeMembers()`, `measureMembers()`, and `sizeOfMembers()`.

## Variable Elements In A Sequence

`o_list<T>` is the main precedent for an embedded sequence of variable-length elements:

- It stores `size_of` and `count` in the list header.
- Its constructor repeatedly calls `arranger(T::type(), itemArgs...)`.
- Iteration starts at `beginOfDynamicArea()`.
- `operator++` advances by `item->sizeOf()`.
- `end()` is computed as `beginOfMemberArea() + size_of`, not `begin() + count`.

This is the important pattern for embedded tuples/lists:

```cpp
const_iterator &operator++()
{
    item = reinterpret_cast<const T *>(
        reinterpret_cast<const std::byte *>(item) + item->sizeOf()
    );
    return *this;
}
```

By contrast, `o_array<T>` and `o_micro_array<T>` store contiguous fixed-size values and use pointer arithmetic. They are not appropriate for elements that may themselves be `o_string`, `o_binary`, nested tuple, or any other variable-size overlay.

## Conditional Dynamic Layouts

`o_change_log` demonstrates a conditional layout: after a fixed boolean, the next member is either an RLE sequence or a plain list. Its `safeSizeOf()` manually reads the boolean and then dispatches to the correct type:

```cpp
buf += super_t::safeBaseSize(buf);
auto isRle = o_simple<bool>::__const_ref(buf);
buf += isRle.sizeOf();
if (isRle.value()) {
    buf += o_rle_sequence<std::uint64_t>::safeSizeOf(buf);
} else {
    buf += o_list<o_simple<std::uint64_t>>::safeSizeOf(buf);
}
```

This pattern is relevant for a tagged union element where the next payload type depends on `StorageClass`.

## Existing Variable-Length Examples

`o_base_string<StrT>` stores a packed length followed by raw string bytes. `measure()` is packed-length bytes plus content bytes. `safeSizeOf()` reads the packed length then advances by that many bytes.

`o_binary` stores a `uint32_t m_bytes` followed by a flexible one-byte member `m_buf`. `measure(size)` is `sizeof(uint32_t) + size`. `begin()` returns `&m_buf`; `safeSizeOf()` reads `m_bytes` and advances by header plus payload.

`o_packed_int` is itself variable-length. It encodes directly into the object bytes and has no fixed payload field beyond the CRTP base. Its `safeSizeOf()` scans continuation bits. Small metadata fields that are commonly below 128, such as tuple item count and element-block byte size, should use `packed_int32` instead of fixed `uint32_t` fields when the object is already variable-length.

`o_packed_array<ItemT, SizeT, MAX_BYTES>` is fixed-size as a container but stores variable-length items inside an internal byte array. Its iterator advances by `ItemT::sizeOf()`, not by `sizeof(ItemT)`.

`PosVT` shows a mixed pattern: `o_micro_array<StorageClass, true>` is fixed-size and self-sized, then `o_unbound_array<Value>` follows. Because `o_unbound_array` has no own size header, `safeSizeOf()` must derive its size from the preceding `types().size()`.

## Requirements For Embedded Tuple/List

For embedded tuple/list elements, `o_tuple_item` must not be `o_fixed`. An item is a tagged overlaid union:

- Fixed/simple payloads may store their value inline after the tag, using `o_simple<T>` or another fixed overlay.
- Variable-length payloads must be embedded immediately after the item tag/header as overlaid objects such as `o_string`, `o_binary`, or nested `o_tuple`.
- The item’s `sizeOf()` must dispatch on the tag and include the embedded payload’s actual `sizeOf()`.
- The item’s `safeSizeOf()` must perform the same dispatch using bounded-safe scanning.
- A tuple/list sequence must advance from one item to the next by `item.sizeOf()`.
- Random access by index requires either linear scan or a separate offset table. A C++ array of item descriptors is not enough if payloads are embedded in the item stream.
- If an offset table is added, offsets should point to item starts relative to the element block, not to separately allocated payloads.
- Construction descriptors should be cheap tagged views, not structs containing every possible expensive payload. Use a union-style payload for primitives and string/byte views; callers must keep viewed variable-length data alive until construction finishes.

The minimum correct first implementation should model:

```text
o_tuple
  header: packed count, packed element_block_byte_size
  element block:
    o_tuple_item
    o_tuple_item
    ...

o_tuple_item
  storage_class
  payload selected by storage_class
```

For the first slice, supported payload classes can be limited to:

- `NONE`
- `BOOLEAN`
- `INT64`
- `FP_NUMERIC64`
- `STRING_REF` as embedded `o_string`
- `DB0_BYTES` as embedded `o_binary`
- nested `TUPLE` / `LIST` as embedded `o_tuple`

The names `STRING_REF` and `DB0_BYTES` are imperfect for embedded storage because existing code uses them for separately allocated members. Until a dedicated embedded storage class exists, accessors must treat these tags as embedded only inside `o_tuple_item`.

## Common Pitfalls

- Do not put variable-length elements in `o_micro_array<T>` or `o_unbound_array<T>` unless `T` is truly fixed-size or the size is externally supplied and access is not by `T* + index`.
- Do not use a descriptor table that stores payload offsets and calls that “embedded” if the payloads are outside the element stream; embedded means the payload object bytes live inside the parent allocation.
- Keep constructor, `measure()`, `safeSizeOf()`, `sizeOf()`, and accessors layout-equivalent.
- When copying an overlaid object, raw-byte copy is the normal pattern only when the entire object extent is known.
- Bounds-safe scanning matters because these objects may be reopened from persisted storage.
