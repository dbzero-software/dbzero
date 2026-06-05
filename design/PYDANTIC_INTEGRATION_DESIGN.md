# Pydantic Integration Design

This document describes a low-risk integration path between dbzero memo classes
and Pydantic. The integration should make memo classes usable in Pydantic
validation and serialization workflows without making memo classes inherit from
`pydantic.BaseModel`.

## Goal

dbzero memo classes should be accepted by Pydantic as first-class custom types:

- Existing memo instances validate as instances of their memo class.
- Dictionaries and other supported mappings can be validated and converted into
  new memo instances.
- Memo instances can serialize into plain Python values suitable for Pydantic
  model dumping and JSON schema workflows.
- The feature is optional and does not add Pydantic as a mandatory runtime
  dependency.

Example target behavior:

```python
import dbzero as db0
from dataclasses import dataclass
from pydantic import BaseModel


@db0.memo
@dataclass
class User:
    name: str
    age: int


class Event(BaseModel):
    user: User


existing = User("Ada", 36)
assert Event(user=existing).user is existing

created = Event(user={"name": "Grace", "age": 37}).user
assert isinstance(created, User)
assert created.age == 37
```

## Non-Goals

Memo classes should not be converted into Pydantic models. In particular, the
following patterns are not part of this design:

```python
@db0.memo
class User(pydantic.BaseModel):
    ...


@db0.memo
@pydantic.dataclasses.dataclass
class User:
    ...
```

These patterns conflict with dbzero's native Python extension type layout.
Pydantic models and Pydantic dataclasses expect to own instance state such as
`__dict__`, private attributes, validators, and model metadata. dbzero memo
instances instead route attribute access through native `tp_getattro` and
`tp_setattro` hooks and expose a synthetic read-only `__dict__`.

The integration also should not enable Pydantic assignment validation by
default. Assigning `obj.field = value` on a memo object is a durable mutation,
so automatic assignment validation would need explicit mutation semantics and
read-only-context handling.

## Current Compatibility

The following patterns already work without dbzero changes:

```python
from pydantic import BaseModel, ConfigDict


class Holder(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    user: User
```

This treats memo instances as opaque arbitrary types. It validates only that
the value is an instance of the target class.

DTO-style validation also works:

```python
class UserDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: str
    age: int


dto = UserDTO.model_validate(user)
```

This is useful for read-side schemas but does not make the memo class itself a
Pydantic-supported type.

## Recommended Integration

Add Pydantic v2 support by installing `__get_pydantic_core_schema__` on wrapped
memo classes.

Pydantic v2 uses `__get_pydantic_core_schema__` as the custom type hook. dbzero
can provide this hook on memo classes after `_wrap_memo_type` returns the native
wrapped type. The hook should be generated in the Python layer so Pydantic
remains optional and import failures are isolated.

Conceptually:

```python
@classmethod
def __get_pydantic_core_schema__(cls, source_type, handler):
    ...
```

The generated schema should:

1. Accept existing instances of `cls`.
2. Validate mappings against fields inferred from annotations, dataclass
   metadata, or constructor signature.
3. Construct a new `cls(**validated_values)` when input is a mapping.
4. Serialize memo instances through a dbzero load function.

This approach avoids changing the native memo object layout and avoids relying
on Pydantic internals beyond its public custom type hook.

## Dependency Model

Pydantic must remain optional.

Implementation should not import Pydantic at module import time. Instead:

- `dbzero.memo` can install a lightweight classmethod that imports Pydantic only
  when Pydantic asks for a schema.
- If Pydantic is unavailable, ordinary dbzero usage is unchanged.
- Packaging metadata should not add Pydantic to `project.dependencies`.
- If a dedicated extra is wanted later, use an optional extra such as
  `dbzero[pydantic]`.

## Field Discovery

The generated schema needs a stable field list.

Preferred sources, in order:

1. `__annotations__`.
2. Dataclass fields from `dataclasses.fields(cls)` when available.
3. `inspect.signature(cls.__init__)`, excluding `self`.

The existing bytecode-derived `py_init_vars` list is useful for dbzero field
layout and migrations, but it should not be the primary source for Pydantic
validation because it does not preserve type information.

Dynamic fields are intentionally not fully representable. If a memo class uses
`**kwargs` or assigns fields conditionally, Pydantic support should either:

- allow extra mapping keys and pass them through to the constructor when the
  constructor accepts `**kwargs`, or
- reject unknown fields by default for classes without `**kwargs`.

The default should be conservative: validate declared fields, pass through only
when the constructor shape makes that clearly intentional.

## Validation Semantics

Input handling should follow these rules:

- If input is already an instance of the memo class, return it unchanged.
- If input is a mapping, validate its declared fields and construct a new memo
  instance.
- If input is not a mapping or memo instance, raise a Pydantic validation error.
- Missing required constructor parameters should produce Pydantic validation
  errors before calling the memo constructor.
- Default values should be taken from dataclass fields or constructor
  signatures.
- Values that Pydantic validates successfully may still be rejected by dbzero if
  dbzero cannot persist them. That failure should propagate as a construction
  error.

Validation should not materialize immutable deferred objects unless normal memo
construction would do so. Pydantic validation must not introduce extra durable
side effects beyond constructing the memo object requested by the user.

## Serialization Semantics

Serialization should support both Python and JSON-oriented Pydantic dumping.

Recommended default:

```python
db0.load(obj)
```

This respects custom `__load__` methods and existing dbzero conversion rules.

A future option may allow `db0.load_all(obj)` for schemas that require every
field, but the initial integration should use the same default serialization
surface dbzero users already know.

Protected fields and access-control masking must be honored. Serialization
should read through normal Python/dbzero access paths rather than bypassing
field protection in native code.

## JSON Schema

Initial JSON schema support can be minimal:

- For annotated memo classes, expose an object schema with declared properties.
- For classes without useful annotations, expose a generic object schema.
- For opaque instance-only use, a plain custom type schema is acceptable.

Schema generation should not be allowed to force opening prefixes or
materializing dbzero classes. It should operate from Python type metadata only.

## Constructor And Prefix Handling

The mapping-to-instance validator should call the memo class constructor through
normal Python invocation:

```python
return cls(**validated_values)
```

This keeps existing dbzero behavior for:

- Static prefixes.
- Dynamic prefixes resolved inside `__init__`.
- `db0.set_prefix(self, prefix)` patterns.
- Singletons.
- Immutable and interned classes.
- Constructor-side tags and field assignments.

For singleton classes, Pydantic validation from a mapping may return an existing
singleton and ignore constructor arguments, matching normal dbzero semantics.
This should be documented in user-facing docs if the feature is exposed.

## Assignment Validation

Do not implement Pydantic `validate_assignment` support for memo fields in the
initial integration.

Durable assignment has dbzero-specific behavior:

- It mutates persistent state.
- It must respect `db0.read_only()`.
- It may materialize referenced immutable objects.
- It may update reference counts, tags, indexes, and atomic context state.

If assignment validation is added later, it should be an explicit helper such
as:

```python
db0.pydantic_assign(obj, "field", value)
```

or a decorator option that clearly documents durable mutation behavior.

## Implementation Plan

Follow TDD. Add failing Python tests first under
`python_tests/test_pydantic_integration.py`.

Implementation should be Python-side first:

1. Add tests for existing memo instance validation through a Pydantic model.
2. Add tests for mapping input constructing a memo instance.
3. Add tests for serialization through `model_dump`.
4. Add tests for optional dependency behavior when Pydantic is not imported.
5. Add a helper in `dbzero/dbzero/memo.py` that attaches Pydantic hooks to the
   wrapped memo class.
6. Keep native C++ changes out of the first implementation unless Python-side
   attachment cannot preserve the hook.

The hook attachment should happen after:

```python
wrapped = _wrap_memo_type(...)
```

The wrapped type currently preserves annotations and dataclass metadata, so the
schema helper can inspect the wrapped class.

## Test Plan

Required tests:

- A memo dataclass field in a Pydantic `BaseModel` accepts an existing memo
  instance without `arbitrary_types_allowed=True`.
- A memo dataclass field accepts a dictionary and constructs a memo instance.
- Pydantic coerces simple annotated field values before construction, such as
  `"7"` to `int`.
- Missing required fields produce a Pydantic validation error.
- Unknown fields are rejected for a constructor without `**kwargs`.
- Unknown fields are passed through for a memo class whose constructor accepts
  `**kwargs`.
- `model_dump()` serializes a memo field to a plain dictionary using normal
  dbzero loading.
- Custom memo `__load__` methods are respected by serialization.
- Existing memo instances validate by identity, not by copying.
- Singleton memo classes validate according to normal singleton construction
  semantics.
- Immutable memo classes validate without forcing unexpected materialization.
- Pydantic is not imported during normal `import dbzero`.

Optional tests:

- JSON schema for an annotated memo class contains object properties.
- A protected field masked from normal reads is not exposed by Pydantic
  serialization.
- `db0.read_only()` rejects mapping validation that would construct or mutate a
  durable memo instance.

Do not add tests that require direct inheritance from `BaseModel` or Pydantic
dataclasses. Those patterns are non-goals and should remain unsupported unless
the native object layout changes substantially.

## Risks

The main risk is surprising durable side effects. Pydantic validation is often
viewed as a pure data transformation, while constructing a dbzero memo object
persists state. Documentation and examples must make this clear.

Other risks:

- Pydantic's custom core-schema APIs may change across major versions.
- Pydantic can validate values that dbzero later rejects as unsupported durable
  field types.
- Dynamic memo classes may not have enough static metadata for precise schemas.
- Serialization may be expensive for large object graphs.
- Custom `__load__` methods may return shapes that differ from the validation
  schema.

These risks are acceptable if the first implementation is opt-in through
Pydantic's normal type usage and avoids changing dbzero construction semantics.

## Open Questions

- Should schema generation use `db0.load` or `db0.load_all` by default?
- Should there be a decorator option to disable Pydantic hook generation for a
  specific memo class?
- Should unknown mapping keys default to reject or pass through for non-dataclass
  classes with permissive constructors?
- Should user-facing docs recommend DTO models for read-only validation and memo
  schemas only for construction?
- Should Pydantic v1 be supported at all via `__get_validators__`, or should the
  integration target Pydantic v2 only?

## Recommendation

Implement Pydantic v2 support as an optional generated custom-type hook on memo
classes. Do not attempt to make memo classes Pydantic models. Keep the first
iteration Python-only, test-driven, and limited to validation from instances,
validation from mappings, and normal dbzero serialization.
