# Pandas DataFrame Integration Design

This document describes a first-class pandas DataFrame integration for dbzero.
The integration should allow pandas DataFrames to be stored as durable memo
members while keeping the storage model based on overlaid types, `v_object`, and
the existing `ObjectBase` lifecycle.

## Goal

dbzero should support pandas DataFrames as durable live objects:

- Assigning a `pandas.DataFrame` to a memo field persists the frame in dbzero.
- Reading the field returns a dbzero DataFrame wrapper backed by durable storage.
- Mutating common DataFrame locations through the wrapper updates durable state.
- Reopening the prefix reconstructs the same frame contents, labels, and core
  dtypes.
- Users can convert explicitly between pandas and dbzero with `db0.dataframe(df)`
  and `db0_df.to_pandas()`.

The storage representation must not be a pickle or opaque serialized blob. The
frame should be decomposed into durable metadata and column storage that dbzero
can validate, reference count, detach, commit, and eventually optimize.

## Non-Goals

The first implementation should not try to implement the whole pandas API.
Pandas is a large Python library with a wide surface area, and a partial wrapper
that claims complete compatibility would be brittle.

The following are out of scope for v1:

- Full pandas method parity.
- Persistence of arbitrary object columns.
- Pickle/blob fallback for unsupported columns.
- MultiIndex for rows or columns.
- Categorical columns.
- Pandas extension arrays and nullable extension dtypes.
- Sparse arrays.
- Timezone-aware datetime columns.
- Depending on pandas internals such as `BlockManager` layout.
- Adding pandas as a mandatory dbzero runtime dependency.

Unsupported features should fail clearly at construction or assignment time.
They should not silently degrade to object storage or lossy conversion.

## Python API

The feature has both transparent and explicit construction paths.

Transparent memo assignment:

```python
@db0.memo
class Model:
    pass

obj = Model()
obj.frame = pandas.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})

assert obj.frame.shape == (2, 2)
```

Explicit construction:

```python
df = pandas.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
durable = db0.dataframe(df)
obj.frame = durable
```

Reading a DataFrame field returns a dbzero wrapper, not a pandas copy:

```python
frame = obj.frame
frame.loc[0, "a"] = 10
db0.commit()
```

Conversion back to pandas is explicit:

```python
pandas_df = obj.frame.to_pandas()
```

The returned pandas DataFrame is a copy. Mutating that copy does not persist
unless the user assigns it back through the dbzero wrapper or through a memo
field.

## Wrapper Surface

The v1 wrapper should expose common pandas-style access and mutation:

- `frame.shape`
- `frame.columns`
- `frame.index`
- `frame.dtypes`
- `frame.to_pandas()`
- `frame["column"]`
- `frame["column"] = values`
- `frame.loc[row_label, column_label]`
- `frame.loc[row_selector, column_selector] = values`
- `frame.iloc[row_index, column_index]`
- `frame.iloc[row_selector, column_selector] = values`

Scalar `loc` and `iloc` reads return Python scalar values. Slice/list reads may
return pandas `Series` or `DataFrame` copies. Mutations through `loc`, `iloc`,
and column assignment update durable storage.

The wrapper should not expose direct mutable views into durable storage. Any
pandas `Series` or `DataFrame` returned from read operations is a copy unless it
is another dbzero wrapper explicitly documented as durable.

## Dependency Model

Pandas and numpy must remain optional.

dbzero module import must not import pandas or numpy. The integration should use
lazy runtime imports only when DataFrame functionality is used:

- `db0.dataframe(...)`
- transparent assignment of a pandas DataFrame
- `.to_pandas()`
- pandas-copy read paths such as slice reads

If pandas is not installed:

- Normal dbzero usage is unchanged.
- `db0.dataframe(...)` raises a clear import/runtime error.
- Reading an existing dbzero DataFrame should either raise a clear error when a
  pandas object is required or support metadata/scalar access that does not need
  pandas. The v1 default can require pandas for wrapper use.

Packaging should not add pandas to `project.dependencies`. A future optional
extra such as `dbzero[pandas]` is acceptable.

## Type And Storage Registration

Add new type identifiers:

- `TypeId::PANDAS_DATAFRAME`: a native pandas DataFrame input object.
- `TypeId::DB0_DATAFRAME`: a dbzero DataFrame wrapper object.
- `StorageClass::DB0_DATAFRAME`: a durable DataFrame member reference.

Registration must be added to:

- `PyTypeManager` detection and extraction helpers.
- `StorageClassMapper`.
- `createMember` and `unloadMember`.
- `unrefMember`.
- schema reporting and type names.
- GC0 type registration.
- fetch/load handling.
- module initialization.
- Python stubs.

The existing `PyTypeManager` should detect pandas DataFrames without importing
pandas at dbzero import time. A reasonable strategy is to lazily import pandas
on first DataFrame check and cache the `pandas.DataFrame` type object if the
import succeeds.

## Native Object Model

Add a new native subsystem under `src/dbzero/object_model/pandas/`.

The primary object should follow the project-wide constructor convention:

```cpp
class DataFrame
    : public db0::ObjectBase<DataFrame, v_dataframe, StorageClass::DB0_DATAFRAME>
{
public:
    DataFrame() = default;
    DataFrame(db0::swine_ptr<Fixture> &, PyObject *pandas_df, AccessFlags = {});
    DataFrame(db0::swine_ptr<Fixture> &, Address, AccessFlags = {});
};
```

The overlaid root stores metadata and addresses for column storage:

```text
o_dataframe
  o_unique_header
  row_count
  column_count
  index_kind
  column_metadata_address
  index_metadata_address
```

Column metadata should be durable and fixed-size where possible:

```text
o_dataframe_column
  dtype_kind
  null_mask_address
  data_address
  label_address
  flags
```

The root object owns its column metadata, row index metadata, labels, null masks,
and column data blocks. Destruction and unref paths must release all owned
allocations.

## Column Storage

v1 should support core dtypes:

- signed integers: `int8`, `int16`, `int32`, `int64`
- unsigned integers: `uint8`, `uint16`, `uint32`, `uint64`
- floating point: `float32`, `float64`
- boolean
- naive `datetime64[ns]`
- string/object-string columns with string or null values only

Fixed-width columns should use typed durable vectors:

```text
v_bvector<int64_t>
v_bvector<double>
v_bvector<uint8_t>
...
```

Each nullable column should store a null mask separately. For v1 this can be a
durable byte vector or bitset-like overlaid structure. Null handling should
round-trip pandas missing values as closely as possible within the supported
dtype set.

String columns should not store Python object pointers. Store strings as durable
overlaid data, for example:

```text
string column
  offsets: v_bvector<uint64_t>
  null mask
  payload bytes or string pool references
```

The exact string-column layout can be optimized later. The v1 requirement is
that the representation is durable, overlaid, and not a pickle.

## Pandas Column Injection Interface

Pandas DataFrames are column-oriented. The durable dbzero DataFrame should expose
each stored column through a pandas-compatible one-dimensional array object
rather than trying to make the whole DataFrame look like one contiguous NumPy
array.

The supported pandas integration point is `ExtensionArray` plus
`ExtensionDtype`. Pandas documents these as the custom one-dimensional array and
dtype interface. `ExtensionArray` instances may be stored directly inside a
`DataFrame` or `Series`, and pandas does not require a specific backing storage
layout. This is a better fit for dbzero than imitating every `numpy.ndarray`
operation because dbzero column storage may be backed by `v_bvector`, null-mask
blocks, string payload blocks, or other overlaid structures.

The dbzero design should use a thin Python-visible array wrapper backed by a
C++ durable column object:

```text
pandas Series/DataFrame column
  Db0ExtensionArray Python object
    Db0Column C++ wrapper
      typed durable column storage
      durable null mask
      durable label/dtype metadata
```

The C++ column wrapper is the low-level interface. The pandas `ExtensionArray`
methods delegate to this wrapper.

### Required Low-Level Column Operations

Every durable column implementation should provide these foundational
operations:

```cpp
class DataFrameColumn
{
public:
    std::size_t size() const;
    DataFrameDType dtype() const;
    std::size_t nbytes() const;

    PyObject *getScalar(std::size_t row) const;
    void setScalar(FixtureLock &, std::size_t row, PyObject *value);

    bool isNull(std::size_t row) const;
    void setNull(FixtureLock &, std::size_t row, bool is_null);

    std::shared_ptr<DataFrameColumn> slice(SliceSpec) const;
    std::shared_ptr<DataFrameColumn> take(
        FixtureLock *, const std::vector<std::int64_t> &indices,
        bool allow_fill, PyObject *fill_value
    ) const;

    void setMany(FixtureLock &, SelectionSpec rows, PyObject *values);
    std::shared_ptr<DataFrameColumn> copy(db0::swine_ptr<Fixture> &, bool deep) const;

    PyObject *toNumpy(bool copy, PyObject *dtype, PyObject *na_value) const;
    PyObject *toPandasArray() const;
};
```

Required semantics:

- `size()` is O(1).
- `getScalar()` returns a Python scalar or the dtype-specific missing value.
- `setScalar()` validates and writes one durable value through `modifyExt()`.
- `isNull()` reads the durable null mask.
- `take()` implements pandas positional selection, including `allow_fill`.
- `slice()` may return a view wrapper when safe, but may return a copy for v1.
- `setMany()` is the shared implementation for `.iloc`, `.loc`, and column
  assignment.
- `copy(deep=True)` creates independent durable storage in the target fixture.
- `toNumpy(copy=False)` may return a NumPy view only when the column has one
  contiguous memory buffer with a stable lifetime. Otherwise it returns a copy.

The low-level API should be intentionally smaller than pandas. Pandas-facing
behavior belongs in the `ExtensionArray` adapter; durable storage behavior
belongs in `DataFrameColumn`.

### Required ExtensionArray Methods

The pandas-facing wrapper should implement the abstract `ExtensionArray`
surface by delegating to the low-level column API:

- `_from_sequence`
- `_from_factorized`
- `__getitem__`
- `__len__`
- `__eq__`
- `dtype`
- `nbytes`
- `isna`
- `take`
- `copy`
- `_concat_same_type`
- `interpolate`

For useful performance and pandas compatibility, also implement:

- `__setitem__` for durable mutation.
- `to_numpy` and `__array__` for NumPy conversion.
- `_values_for_factorize` and `_from_factorized`.
- `_values_for_argsort`.
- `_reduce` for simple reductions where the dtype supports them.
- `__array_ufunc__` only after the basic storage path is stable.

For `__array_ufunc__`, return `NotImplemented` when any pandas `Series`,
`DataFrame`, or `Index` is present in the inputs. Pandas expects to unbox the
extension array and re-box the result itself.

### Required ExtensionDtype Methods

Each supported dbzero column kind should have a matching dtype object.

The dtype wrapper must provide:

- `type`
- `name`
- `construct_array_type`

It should also provide:

- `na_value`
- `_is_numeric` for numeric dtypes
- `_is_boolean` for boolean dtype
- `_get_common_dtype` for compatible dtype promotion

The dtype name should be explicit, for example `dbzero[int64]`,
`dbzero[float64]`, `dbzero[bool]`, `dbzero[datetime64ns]`, and
`dbzero[string]`. The exact public names can be changed before implementation,
but they must be stable once persisted in any user-visible schema.

### NumPy Protocol Support

NumPy interoperability is still useful, but it should not be the primary pandas
storage contract.

For fixed-width columns that can expose a stable contiguous memory range, the
column object may expose:

- Python buffer protocol.
- `__array_interface__`.
- `__array__`.

For dbzero's likely block-backed `v_bvector` layout, full-column zero-copy NumPy
views may not be possible. In that case:

- `__array__` returns a NumPy copy.
- `to_numpy(copy=False)` is best-effort and may still copy.
- pandas mutation must go through `ExtensionArray.__setitem__`, not through a
  NumPy view.

If a future column storage variant is explicitly contiguous, a NumPy view may be
returned with the dbzero column wrapper as the base object so the durable memory
stays alive for the lifetime of the view.

## Index And Labels

v1 should support:

- default `RangeIndex`
- simple single-level indexes containing supported scalar values
- string column labels

Column labels and row labels should be persisted separately from data columns.
`loc` resolves labels through the durable index metadata. `iloc` uses integer
positions directly.

MultiIndex is rejected in v1.

## Mutation Semantics

All mutating Python APIs must use `PY_MUTATING_API_FUNC` and route native
changes through `modifyExt()`.

Supported durable mutations:

- scalar cell assignment by `.loc` and `.iloc`
- shape-compatible row/column slice assignment
- full column add or replacement through `frame["column"] = values`

Mutation should validate:

- the target column exists, unless column assignment is intentionally adding a
  new column
- row and column selectors resolve to existing positions
- assigned value shape matches the selected region
- assigned values can be represented by the target dtype, or the whole column is
  replaced with a supported new dtype

For v1, scalar assignment should not silently widen column dtype. If a value
cannot be stored in the existing dtype, raise a clear error. Column replacement
may choose a new supported dtype based on the replacement values.

Mutations inside `db0.read_only()` must be rejected.

## Member Assignment

When a pandas DataFrame is assigned to a memo field:

1. `PyTypeManager` detects `TypeId::PANDAS_DATAFRAME`.
2. `StorageClassMapper` maps it to `PreStorageClass::DB0_DATAFRAME`.
3. `createMember<TypeId::PANDAS_DATAFRAME>` creates a new `DataFrame` object in
   the target fixture and imports supported columns.
4. The new durable DataFrame increments its object reference count.
5. The memo field stores the DataFrame address as `StorageClass::DB0_DATAFRAME`.

When a dbzero DataFrame wrapper is assigned:

1. `createMember<TypeId::DB0_DATAFRAME>` extracts the native `DataFrame`.
2. If it belongs to the same fixture, increment the reference count and store
   its address.
3. If it belongs to a different fixture, either auto-harden by moving the
   unreferenced DataFrame to the target fixture or reject cross-prefix
   assignment for v1. The conservative v1 default is to reject cross-prefix
   assignment until move semantics are implemented for owned column blocks.

## Unload, Fetch, And Load

`unloadMember<StorageClass::DB0_DATAFRAME>` returns a dbzero DataFrame wrapper.
It should use the language cache when possible, matching the behavior of other
dbzero collection wrappers.

`db0.fetch(uuid)` should support DataFrame object IDs if fetch-by-UUID for
collection-like objects is expected for the new storage class.

`db0.load()` and `db0.load_all()` should convert a dbzero DataFrame wrapper to a
pandas DataFrame copy. This keeps load output in ordinary Python/Pandas objects
rather than returning durable wrappers inside loaded graphs.

## Atomic, Detach, And GC Behavior

`DataFrame` must participate in the same lifecycle as existing dbzero
collections:

- `incRef` and `decRef` use the root header.
- `destroy()` releases column metadata, index metadata, null masks, and data
  blocks.
- `detach()` detaches all owned durable child objects and the root.
- `commit()` commits all owned durable child objects and the root.
- `beginModify()` integration should register wrappers with the atomic context
  so rollback can detach stale views.

If a mutation changes an owned child structure address, the root metadata must
be re-synced immediately, following the same discipline used for morphing
indexes and other address-changing structures.

## Error Policy

Errors should be explicit and early:

- Missing pandas when DataFrame functionality is used: `RuntimeError` or
  `ImportError` with an actionable message.
- Unsupported dtype: `TypeError`.
- Unsupported index shape: `TypeError`.
- Out-of-range `iloc`: `IndexError`.
- Missing `loc` label: `KeyError`.
- Shape mismatch on assignment: `ValueError`.
- Mutation in read-only context: `RuntimeError`.

No unsupported DataFrame content should be silently converted to string,
pickled, or dropped.

## Implementation Slices

Use TDD. Start with Python behavior tests, then add native tests for storage
layout and lifecycle.

Recommended slices:

1. Add failing Python tests for `db0.dataframe(df)` and memo assignment.
2. Add type IDs, storage class, schema names, and stub registration.
3. Add minimal native `DataFrame` object with row/column metadata and one
   numeric column type.
4. Add Python wrapper construction, unload, `.shape`, `.columns`, `.index`, and
   `.to_pandas()`.
5. Add fixed-width dtype coverage and null masks.
6. Add string column storage.
7. Add `frame["column"]` read and replacement.
8. Add `.iloc` scalar read/write.
9. Add `.loc` scalar read/write.
10. Add slice/list reads and shape-compatible assignment.
11. Add load/fetch integration and `.pyi` stubs.
12. Add debug/release validation and C++ tests.

## Tests

Python tests should use `pytest.importorskip("pandas")` so the suite remains
valid when pandas is not installed.

Behavior tests:

- `db0.dataframe(pd.DataFrame(...))` creates a dbzero DataFrame wrapper.
- A pandas DataFrame assigned as a memo member reopens with the same values,
  columns, index, and supported dtypes.
- A dbzero DataFrame assigned as a memo member reopens correctly.
- `.to_pandas()` round-trips supported numeric, bool, datetime64, and string
  columns.
- `frame["col"]` returns a pandas Series copy.
- `frame["col"] = values` persists across commit/reopen.
- `.iloc[row, col]` scalar get/set persists.
- `.loc[label, column]` scalar get/set persists.
- Slice/list reads return pandas copies.
- Shape-compatible slice/list assignment persists.
- Unsupported dtypes and MultiIndex raise clear errors.
- Mutations inside `db0.read_only()` raise.
- `db0.load(obj)` converts DataFrame members to pandas DataFrames.

Native tests:

- `o_dataframe` size and `safeSizeOf` validation.
- Column metadata can be created and reopened.
- Fixed-width column blocks persist values.
- Null masks persist missing values.
- String columns persist offsets and payload.
- `destroy`, `detach`, and `commit` process owned child structures.
- Address-changing child structures update root references.

## Open Questions

The following decisions can be deferred until implementation reaches the
relevant slice:

- Whether string column payloads should use dedicated payload blocks or existing
  string pool primitives.
- Whether cross-prefix DataFrame assignment should be rejected or auto-hardened.
- Whether `db0.load_all()` should always return pandas copies or preserve dbzero
  wrappers behind an option.
- Whether a future optional `dbzero[pandas]` package extra should be added.

## Feasibility

The dbzero architecture can support this feature. Existing collection wrappers
already provide most of the required lifecycle patterns: type detection,
storage-class mapping, `ObjectBase` reference counting, wrapper cache use,
member creation/unload, read-only enforcement, and atomic mutation registration.

The main implementation risk is pandas API breadth, not durable storage. v1
must keep a narrow compatibility surface and reject unsupported pandas features
clearly.
