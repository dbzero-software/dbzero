# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import os
import subprocess
import sys
import textwrap

import pytest
import dbzero as db0
from dataclasses import dataclass
from dbzero.dbzero import _get_tag_fields
from .memo_test_types import MemoTestClass, KVTestClass


_TAG_FIELDS_ATTR = "__DBZERO_TAG_FIELDS_ATTR"

TagFieldDataclassDefaultStatus = db0.enum(
    "TagFieldDataclassDefaultStatus",
    values=["lead", "archived"],
)
TagFieldDataclassDefaultStatusValue = type(TagFieldDataclassDefaultStatus.lead)


@db0.memo
@db0.tag_fields("status")
@dataclass(eq=False)
class DataclassEnumDefaultTag:
    name: str
    status: TagFieldDataclassDefaultStatusValue = TagFieldDataclassDefaultStatus.lead


def _query_names(memo_type, *tags):
    return {obj.name for obj in db0.find(memo_type, *tags)}


def _query_uuids(memo_type, *tags):
    return {db0.uuid(obj) for obj in db0.find(memo_type, *tags)}


def test_tag_fields_are_empty_before_class_is_materialized(db0_fixture):
    @db0.memo
    @db0.tag_fields("status", "parent")
    class TagFieldsUnmaterialized:
        def __init__(self, status, parent=None):
            self.status = status
            self.parent = parent

    assert _get_tag_fields(TagFieldsUnmaterialized) == ()


def test_tag_fields_report_materialized_declared_fields_only(db0_fixture):
    @db0.tag_fields("status", "parent")
    @db0.memo
    class TagFieldsMaterialized:
        def __init__(self, status, parent=None):
            self.status = status

    TagFieldsMaterialized("open")

    assert _get_tag_fields(TagFieldsMaterialized) == ("status",)


def test_multiple_tag_fields_decorators_merge_in_order(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    @db0.tag_fields("parent")
    class TagFieldsMergedBeforeMemo:
        def __init__(self, parent, status):
            self.parent = parent
            self.status = status

    TagFieldsMergedBeforeMemo("root", "open")
    assert _get_tag_fields(TagFieldsMergedBeforeMemo) == ("parent", "status")

    @db0.tag_fields("owner")
    @db0.memo
    @db0.tag_fields("status")
    class TagFieldsMergedAroundMemo:
        def __init__(self, status, owner):
            self.status = status
            self.owner = owner

    TagFieldsMergedAroundMemo("open", "alice")
    assert _get_tag_fields(TagFieldsMergedAroundMemo) == ("status", "owner")


def test_duplicate_tag_fields_are_ignored(db0_fixture):
    @db0.memo
    @db0.tag_fields("status", "parent", "status")
    @db0.tag_fields("parent", "owner")
    class TagFieldsDeduplicated:
        def __init__(self, parent, owner, status):
            self.parent = parent
            self.owner = owner
            self.status = status

    TagFieldsDeduplicated("root", "alice", "open")
    assert _get_tag_fields(TagFieldsDeduplicated) == ("parent", "owner", "status")


def test_tag_fields_requires_string_names():
    with pytest.raises(TypeError):
        db0.tag_fields("status", 123)


def test_empty_tag_fields_declaration(db0_fixture):
    @db0.memo
    @db0.tag_fields()
    class TagFieldsEmpty:
        pass

    assert _get_tag_fields(TagFieldsEmpty) == ()


def test_existing_fields_are_synchronized_during_type_attachment(db0_fixture):
    @db0.memo
    class ExistingTagFields:
        def __init__(self, status):
            self.status = status

    ExistingTagFields("open")

    @db0.tag_fields("status", "missing")
    @db0.memo
    class ExistingTagFields:
        def __init__(self, status):
            self.status = status

    assert _get_tag_fields(ExistingTagFields) == ("status",)


def test_tag_fields_on_plain_class_only_stores_python_metadata():
    @db0.tag_fields("status")
    class PlainTagFieldsClass:
        pass

    assert not db0.is_memo(PlainTagFieldsClass)
    assert getattr(PlainTagFieldsClass, _TAG_FIELDS_ATTR) == ("status",)

    with pytest.raises(TypeError):
        _get_tag_fields(PlainTagFieldsClass)


def test_tag_fields_does_not_change_basic_memo_behavior(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class TagFieldsBasicMemo:
        def __init__(self, status):
            self.status = status

    obj = TagFieldsBasicMemo("open")

    assert db0.is_memo(obj)
    assert obj.status == "open"


def test_initial_tag_is_hidden_until_init_completes(db0_fixture):
    seen_during_init = []

    @db0.memo
    @db0.tag_fields("status")
    class BufferedInitialTag:
        def __init__(self, status):
            self.status = status
            seen_during_init.extend(db0.find(type(self), status))

    obj = BufferedInitialTag("open")

    assert seen_during_init == []
    assert list(db0.find(BufferedInitialTag, "open")) == [obj]


def test_dataclass_enum_default_is_valid_initial_tag(db0_fixture):
    obj = DataclassEnumDefaultTag("Avery")

    assert obj.status == TagFieldDataclassDefaultStatus.lead
    assert list(db0.find(DataclassEnumDefaultTag, TagFieldDataclassDefaultStatus.lead)) == [obj]


@pytest.mark.parametrize(
    "manual_tag_operation",
    [
        "db0.tags(self).add(db0.as_tag(self.agent))",
        'db0.tags(self).add("TAG_X")',
    ],
)
def test_manual_tag_during_dataclass_tag_field_post_init_does_not_crash(tmp_path, manual_tag_operation):
    script = textwrap.dedent(
        f"""
        from dataclasses import dataclass
        import os

        import dbzero as db0

        @db0.memo
        class Agent:
            def __init__(self, role):
                self.role = role

        @db0.memo
        @db0.tag_fields("agent")
        @dataclass
        class JobDef:
            agent: Agent

            def __post_init__(self):
                {manual_tag_operation}

        db_path = {str(tmp_path / "db0")!r}
        os.mkdir(db_path)
        db0.init(db_path, read_write=True)
        db0.open("test", "rw")

        agent = Agent("test-agent")
        job_def = JobDef(agent=agent)
        assert job_def.agent.role == "test-agent"
        db0.close()
        """
    )

    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONFAULTHANDLER"] = "1"
    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(
            f"manual tag during tag_fields post-init timed out for {manual_tag_operation!r}\n"
            f"stdout:\n{exc.stdout or ''}\n"
            f"stderr:\n{exc.stderr or ''}"
        )

    assert result.returncode == 0, (
        f"manual tag during tag_fields post-init failed for {manual_tag_operation!r} "
        f"with code {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def test_initial_tags_use_final_values_and_flush_together(db0_fixture):
    @db0.memo
    @db0.tag_fields("status", "owner")
    class BufferedInitialTags:
        def __init__(self):
            self.status = "draft"
            self.owner = "alice"
            self.status = "open"

    obj = BufferedInitialTags()

    assert list(db0.find(BufferedInitialTags, "draft")) == []
    assert list(db0.find(BufferedInitialTags, "open", "alice")) == [obj]


def test_initial_none_contributes_no_tag(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class InitialNoneTag:
        def __init__(self):
            self.status = None

    obj = InitialNoneTag()

    assert obj.status is None
    assert list(db0.find(InitialNoneTag, "unrelated")) == []


def test_failed_initialization_discards_buffered_tags(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class FailedInitialTag:
        def __init__(self):
            self.status = "orphaned"
            raise RuntimeError("construction failed")

    with pytest.raises(RuntimeError, match="construction failed"):
        FailedInitialTag()

    assert list(db0.find(FailedInitialTag, "orphaned")) == []


def test_rejected_initial_tag_assignment_preserves_buffer_and_field(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class RejectedInitialTag:
        def __init__(self):
            self.status = "open"
            try:
                self.status = ["invalid"]
            except TypeError:
                self.value_after_rejection = self.status

    obj = RejectedInitialTag()

    assert obj.value_after_rejection == "open"
    assert obj.status == "open"
    assert list(db0.find(RejectedInitialTag, "open")) == [obj]


def test_unsupported_initial_tag_value_fails_construction(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class UnsupportedInitialTag:
        def __init__(self):
            self.status = 42

    with pytest.raises(TypeError):
        UnsupportedInitialTag()

    assert list(db0.find(UnsupportedInitialTag, "unrelated")) == []


def test_post_init_tag_assignment_replaces_and_removes_tag(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class RuntimeTagAssignment:
        def __init__(self):
            self.status = "open"

    obj = RuntimeTagAssignment()
    obj.status = "closed"

    assert list(db0.find(RuntimeTagAssignment, "open")) == []
    assert list(db0.find(RuntimeTagAssignment, "closed")) == [obj]

    obj.status = None
    assert obj.status is None
    assert list(db0.find(RuntimeTagAssignment, "closed")) == []


def test_reassigning_same_effective_tag_keeps_tag(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class SameRuntimeTag:
        def __init__(self):
            self.status = "open"

    obj = SameRuntimeTag()
    obj.status = "".join(("o", "p", "e", "n"))

    assert list(db0.find(SameRuntimeTag, "open")) == [obj]


def test_deleting_tag_field_removes_passive_tag(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class DeletedRuntimeTag:
        def __init__(self):
            self.status = "open"

    obj = DeletedRuntimeTag()
    del obj.status

    assert list(db0.find(DeletedRuntimeTag, "open")) == []
    with pytest.raises(AttributeError):
        _ = obj.status


@pytest.mark.parametrize("unsupported", [["open"], ("open",), {"open"}])
def test_rejected_runtime_tag_containers_preserve_field_and_tag(db0_fixture, unsupported):
    @db0.memo
    @db0.tag_fields("status")
    class RejectedRuntimeTag:
        def __init__(self):
            self.status = "open"

    obj = RejectedRuntimeTag()

    with pytest.raises(TypeError):
        obj.status = unsupported

    assert obj.status == "open"
    assert list(db0.find(RejectedRuntimeTag, "open")) == [obj]


def test_rejected_runtime_iterable_preserves_field_and_tag(db0_fixture):
    class UnsupportedIterable:
        def __iter__(self):
            return iter(("open",))

    @db0.memo
    @db0.tag_fields("status")
    class RejectedRuntimeIterable:
        def __init__(self):
            self.status = "open"

    obj = RejectedRuntimeIterable()

    with pytest.raises(TypeError):
        obj.status = UnsupportedIterable()

    assert obj.status == "open"
    assert list(db0.find(RejectedRuntimeIterable, "open")) == [obj]


def test_rejected_runtime_scalar_preserves_field_and_tag(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class RejectedRuntimeScalar:
        def __init__(self):
            self.status = "open"

    obj = RejectedRuntimeScalar()

    with pytest.raises(TypeError):
        obj.status = 42

    assert obj.status == "open"
    assert list(db0.find(RejectedRuntimeScalar, "open")) == [obj]


def test_non_tag_fields_and_manual_passive_tags_are_unchanged(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class MixedRuntimeFields:
        def __init__(self):
            self.status = "open"
            self.note = ["ordinary", "container"]

    obj = MixedRuntimeFields()
    obj.note = ["updated"]
    db0.tags(obj, passive=True).add("manual")

    assert obj.note == ["updated"]
    assert list(db0.find(MixedRuntimeFields, "manual")) == [obj]


def test_passive_tag_field_then_regular_foreign_tag_preserves_regular_target(db0_fixture):
    @db0.memo
    class Scope:
        pass

    @db0.memo
    @db0.tag_fields("scope")
    class PassiveRecord:
        def __init__(self, scope):
            self.scope = scope

    @db0.memo
    class Error:
        pass

    def add_error(scope):
        error = Error()
        db0.tags(error).add(db0.as_tag(scope))

    scope = Scope()

    PassiveRecord(scope)
    add_error(scope)

    assert len(list(db0.find(Error, db0.as_tag(scope)))) == 1


def test_mixed_passive_regular_foreign_tag_remove_releases_regular_target(db0_fixture):
    @db0.memo
    class Scope:
        pass

    @db0.memo
    @db0.tag_fields("scope")
    class PassiveRecord:
        def __init__(self, scope):
            self.scope = scope

    @db0.memo
    class RegularTarget:
        def __init__(self):
            pass

    scope = Scope()
    tag = db0.as_tag(scope)
    PassiveRecord(scope)

    target = RegularTarget()
    target_uuid = db0.uuid(target)
    db0.tags(target).add(tag)
    db0.commit()

    assert list(db0.find(RegularTarget, tag)) == [target]
    assert db0.getrefcount(target) == 1

    db0.tags(target).remove(tag)
    db0.commit()

    assert list(db0.find(RegularTarget, tag)) == []
    assert db0.getrefcount(target) == 0

    del target
    db0.commit()

    assert not db0.exists(target_uuid)


def test_tag_field_removal_preserves_manual_collision_semantics(db0_fixture):
    @db0.memo
    @db0.tag_fields("status")
    class ManualTagCollision:
        def __init__(self):
            self.status = "open"

    obj = ManualTagCollision()
    db0.tags(obj, passive=True).add("open")
    obj.status = "closed"

    assert list(db0.find(ManualTagCollision, "open")) == []
    assert list(db0.find(ManualTagCollision, "closed")) == [obj]


def test_tag_field_added_declaration_migrates_existing_objects(db0_fixture):
    @db0.memo
    class MigratedAddedTagField:
        def __init__(self, status):
            self.status = status

    obj = MigratedAddedTagField("open")
    assert list(db0.find(MigratedAddedTagField, "open")) == []

    @db0.tag_fields("status")
    @db0.memo
    class MigratedAddedTagField:
        def __init__(self, status):
            self.status = status

    matches = list(db0.find(MigratedAddedTagField, "open"))
    assert len(matches) == 1
    assert db0.uuid(matches[0]) == db0.uuid(obj)
    assert _get_tag_fields(MigratedAddedTagField) == ("status",)


def test_tag_field_removed_declaration_migrates_existing_objects(db0_fixture):
    @db0.tag_fields("status")
    @db0.memo
    class MigratedRemovedTagField:
        def __init__(self, status):
            self.status = status

    obj = MigratedRemovedTagField("open")
    assert list(db0.find(MigratedRemovedTagField, "open")) == [obj]

    @db0.memo
    class MigratedRemovedTagField:
        def __init__(self, status):
            self.status = status

    assert list(db0.find(MigratedRemovedTagField, "open")) == []
    assert _get_tag_fields(MigratedRemovedTagField) == ()


def test_renamed_tag_field_preserves_identity(db0_fixture):
    @db0.tag_fields("status")
    @db0.memo
    class RenamedTagField:
        def __init__(self, status):
            self.status = status

    obj = RenamedTagField("open")
    assert list(db0.find(RenamedTagField, "open")) == [obj]

    db0.rename_field(RenamedTagField, "status", "state")

    assert _get_tag_fields(RenamedTagField) == ("state",)
    assert list(db0.find(RenamedTagField, "open")) == [obj]

    obj.state = "closed"
    assert list(db0.find(RenamedTagField, "open")) == []
    assert list(db0.find(RenamedTagField, "closed")) == [obj]

    @db0.tag_fields("state")
    @db0.memo
    class RenamedTagField:
        def __init__(self, state):
            self.state = state

    assert _get_tag_fields(RenamedTagField) == ("state",)
    assert list(db0.find(RenamedTagField, "closed")) == [obj]


def test_failed_tag_field_migration_rolls_back_pending_tag_updates(db0_fixture):
    @db0.memo
    class FailedMigratedTagField:
        def __init__(self, name, status):
            self.name = name
            self.status = status

    valid = FailedMigratedTagField("valid", "open")
    invalid = FailedMigratedTagField("invalid", 42)
    active_type = FailedMigratedTagField

    @db0.tag_fields("status")
    @db0.memo
    class FailedMigratedTagField:
        def __init__(self, name, status):
            self.name = name
            self.status = status

    with pytest.raises(RuntimeError, match="scalar tag"):
        FailedMigratedTagField("new", "closed")

    assert _get_tag_fields(active_type) == ()
    assert _query_names(active_type, "open") == set()
    assert valid.status == "open"
    assert invalid.status == 42


def test_no_auto_migrate_raises_and_keeps_previous_active_declaration(tmp_path):
    db0.init(str(tmp_path), no_auto_migrate=True)
    db0.open("tag-field-migration")
    try:
        @db0.tag_fields("status")
        @db0.memo
        class NoAutoMigratedTagField:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        obj = NoAutoMigratedTagField("open", "alice")
        assert list(db0.find(NoAutoMigratedTagField, "open")) == [obj]

        @db0.tag_fields("owner")
        @db0.memo
        class NoAutoMigratedTagField:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        with pytest.raises(db0.MigrateError):
            NoAutoMigratedTagField("closed", "bob")

        assert _get_tag_fields(NoAutoMigratedTagField) == ("status",)
        obj.status = "closed"
        assert list(db0.find(NoAutoMigratedTagField, "closed")) == [obj]
        assert list(db0.find(NoAutoMigratedTagField, "alice")) == []
    finally:
        db0.close()


def test_explicit_migrate_applies_current_tag_field_declaration(tmp_path):
    db0.init(str(tmp_path), no_auto_migrate=True)
    db0.open("tag-field-explicit-migration")
    try:
        @db0.tag_fields("status")
        @db0.memo
        class ExplicitMigratedTagField:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        obj = ExplicitMigratedTagField("open", "alice")

        @db0.tag_fields("owner")
        @db0.memo
        class ExplicitMigratedTagField:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        with pytest.raises(db0.MigrateError):
            ExplicitMigratedTagField("closed", "bob")

        db0.migrate(ExplicitMigratedTagField)

        assert _get_tag_fields(ExplicitMigratedTagField) == ("owner",)
        assert list(db0.find(ExplicitMigratedTagField, "open")) == []
        matches = list(db0.find(ExplicitMigratedTagField, "alice"))
        assert len(matches) == 1
        assert db0.uuid(matches[0]) == db0.uuid(obj)
    finally:
        db0.close()


def test_auto_migrate_same_direction_inherited_tag_fields(db0_fixture):
    @db0.tag_fields("status")
    @db0.memo
    class AutoSameDirectionBase:
        def __init__(self, name, status, owner):
            self.name = name
            self.status = status
            self.owner = owner

    @db0.tag_fields("status")
    @db0.memo
    class AutoSameDirectionDerived(AutoSameDirectionBase):
        pass

    base = AutoSameDirectionBase("base", "open", "alice")
    derived = AutoSameDirectionDerived("derived", "closed", "bob")
    base_uuid = db0.uuid(base)
    derived_uuid = db0.uuid(derived)

    @db0.tag_fields("owner")
    @db0.memo
    class AutoSameDirectionBase:
        def __init__(self, name, status, owner):
            self.name = name
            self.status = status
            self.owner = owner

    @db0.tag_fields("owner")
    @db0.memo
    class AutoSameDirectionDerived(AutoSameDirectionBase):
        pass

    assert _query_uuids(AutoSameDirectionDerived) == {derived_uuid}

    assert _query_names(AutoSameDirectionBase, "open") == set()
    assert _query_names(AutoSameDirectionDerived, "closed") == set()
    assert _query_names(AutoSameDirectionBase, "alice") == {"base"}
    assert _query_names(AutoSameDirectionDerived, "bob") == {"derived"}
    assert _query_uuids(AutoSameDirectionBase) == {base_uuid, derived_uuid}
    assert _query_names(AutoSameDirectionBase, "bob") == {"derived"}


def test_auto_migrate_crossed_inherited_tag_fields(db0_fixture):
    @db0.tag_fields("status")
    @db0.memo
    class AutoCrossedBase:
        def __init__(self, name, status, owner):
            self.name = name
            self.status = status
            self.owner = owner

    @db0.tag_fields("owner")
    @db0.memo
    class AutoCrossedDerived(AutoCrossedBase):
        pass

    base = AutoCrossedBase("base", "open", "alice")
    derived = AutoCrossedDerived("derived", "closed", "bob")
    base_uuid = db0.uuid(base)
    derived_uuid = db0.uuid(derived)

    @db0.tag_fields("owner")
    @db0.memo
    class AutoCrossedBase:
        def __init__(self, name, status, owner):
            self.name = name
            self.status = status
            self.owner = owner

    @db0.tag_fields("status")
    @db0.memo
    class AutoCrossedDerived(AutoCrossedBase):
        pass

    assert _query_uuids(AutoCrossedDerived) == {derived_uuid}

    assert _get_tag_fields(AutoCrossedBase) == ("owner",)
    assert _get_tag_fields(AutoCrossedDerived) == ("owner", "status")
    assert _query_names(AutoCrossedBase, "open") == set()
    assert _query_names(AutoCrossedBase, "alice") == {"base"}
    assert _query_names(AutoCrossedBase, "bob") == {"derived"}
    assert _query_names(AutoCrossedBase, "closed") == {"derived"}
    assert _query_names(AutoCrossedDerived, "closed") == {"derived"}
    assert _query_names(AutoCrossedDerived, "bob") == {"derived"}
    assert _query_uuids(AutoCrossedBase) == {base_uuid, derived_uuid}


@pytest.mark.parametrize("order", ["base-first", "derived-first"])
def test_explicit_migrate_inherited_tag_fields_converges_by_order(tmp_path, order):
    db0.init(str(tmp_path), no_auto_migrate=True)
    db0.open(f"tag-field-inheritance-explicit-{order}")
    try:
        @db0.tag_fields("status")
        @db0.memo
        class ExplicitOrderBase:
            def __init__(self, name, status, owner):
                self.name = name
                self.status = status
                self.owner = owner

        @db0.tag_fields("status")
        @db0.memo
        class ExplicitOrderDerived(ExplicitOrderBase):
            pass

        base = ExplicitOrderBase("base", "open", "alice")
        derived = ExplicitOrderDerived("derived", "closed", "bob")
        base_uuid = db0.uuid(base)
        derived_uuid = db0.uuid(derived)

        @db0.tag_fields("owner")
        @db0.memo
        class ExplicitOrderBase:
            def __init__(self, name, status, owner):
                self.name = name
                self.status = status
                self.owner = owner

        @db0.tag_fields("owner")
        @db0.memo
        class ExplicitOrderDerived(ExplicitOrderBase):
            pass

        if order == "base-first":
            db0.migrate(ExplicitOrderBase)
            db0.migrate(ExplicitOrderDerived)
        else:
            db0.migrate(ExplicitOrderDerived)
            db0.migrate(ExplicitOrderBase)

        assert _get_tag_fields(ExplicitOrderBase) == ("owner",)
        assert _get_tag_fields(ExplicitOrderDerived) == ("owner",)
        assert _query_names(ExplicitOrderBase, "open") == set()
        assert _query_names(ExplicitOrderDerived, "closed") == set()
        assert _query_names(ExplicitOrderBase, "alice") == {"base"}
        assert _query_names(ExplicitOrderBase, "bob") == {"derived"}
        assert _query_names(ExplicitOrderDerived, "bob") == {"derived"}
        assert _query_uuids(ExplicitOrderBase) == {base_uuid, derived_uuid}
    finally:
        db0.close()


def test_explicit_migrate_derived_only_resolves_pending_base_tag_field_declaration(tmp_path):
    db0.init(str(tmp_path), no_auto_migrate=True)
    db0.open("tag-field-inheritance-explicit-derived-only")
    try:
        @db0.tag_fields("status")
        @db0.memo
        class ExplicitDerivedOnlyBase:
            def __init__(self, name, status, owner):
                self.name = name
                self.status = status
                self.owner = owner

        @db0.tag_fields("status")
        @db0.memo
        class ExplicitDerivedOnlyDerived(ExplicitDerivedOnlyBase):
            pass

        base = ExplicitDerivedOnlyBase("base", "open", "alice")
        derived = ExplicitDerivedOnlyDerived("derived", "closed", "bob")
        base_uuid = db0.uuid(base)
        derived_uuid = db0.uuid(derived)

        @db0.tag_fields("owner")
        @db0.memo
        class ExplicitDerivedOnlyBase:
            def __init__(self, name, status, owner):
                self.name = name
                self.status = status
                self.owner = owner

        @db0.tag_fields("owner")
        @db0.memo
        class ExplicitDerivedOnlyDerived(ExplicitDerivedOnlyBase):
            pass

        db0.migrate(ExplicitDerivedOnlyDerived)

        assert _get_tag_fields(ExplicitDerivedOnlyBase) == ("owner",)
        assert _get_tag_fields(ExplicitDerivedOnlyDerived) == ("owner",)
        assert _query_names(ExplicitDerivedOnlyBase, "open") == set()
        assert _query_names(ExplicitDerivedOnlyDerived, "closed") == set()
        assert _query_names(ExplicitDerivedOnlyBase, "alice") == {"base"}
        assert _query_names(ExplicitDerivedOnlyBase, "bob") == {"derived"}
        assert _query_names(ExplicitDerivedOnlyDerived, "bob") == {"derived"}
        assert _query_uuids(ExplicitDerivedOnlyBase) == {base_uuid, derived_uuid}
    finally:
        db0.close()


@pytest.mark.stress_test
def test_tag_field_migration_inheritance_stress(tmp_path):
    per_type_count = 25_000

    def expected_mod_count(modulus, value, groups=1):
        return groups * sum(1 for index in range(per_type_count) if index % modulus == value)

    def result_count(memo_type, *tags):
        return len(list(db0.find(memo_type, *tags)))

    def create_objects(memo_type, keepalive):
        for index in range(per_type_count):
            keepalive.append(
                memo_type(
                    index,
                    f"status-{index % 5}",
                    f"owner-{index % 4}",
                    f"region-{index % 3}",
                    f"stage-{index % 2}",
                    f"priority-{index % 7}",
                )
            )

    db0.init(str(tmp_path), no_auto_migrate=True, autocommit=False)
    db0.open("tag-field-inheritance-stress", slab_size=512 << 20)
    try:
        @db0.tag_fields("status")
        @db0.memo
        class TagFieldStressBase:
            def __init__(self, seq, status, owner, region, stage, priority):
                self.seq = seq
                self.status = status
                self.owner = owner
                self.region = region
                self.stage = stage
                self.priority = priority

        @db0.tag_fields("owner")
        @db0.memo
        class TagFieldStressDerivedA(TagFieldStressBase):
            pass

        @db0.tag_fields("region")
        @db0.memo
        class TagFieldStressDerivedB(TagFieldStressBase):
            pass

        @db0.tag_fields("stage")
        @db0.memo
        class TagFieldStressLeaf(TagFieldStressDerivedA):
            pass

        keepalive = []
        for memo_type in (
            TagFieldStressBase,
            TagFieldStressDerivedA,
            TagFieldStressDerivedB,
            TagFieldStressLeaf,
        ):
            create_objects(memo_type, keepalive)
        db0.commit()

        assert result_count(TagFieldStressBase) == 4 * per_type_count
        assert result_count(TagFieldStressDerivedA) == 2 * per_type_count
        assert result_count(TagFieldStressDerivedB) == per_type_count
        assert result_count(TagFieldStressLeaf) == per_type_count
        assert result_count(TagFieldStressBase, "status-0") == expected_mod_count(5, 0, groups=4)
        assert result_count(TagFieldStressBase, "region-0") == expected_mod_count(3, 0, groups=1)
        assert result_count(TagFieldStressDerivedA, "owner-0") == expected_mod_count(4, 0, groups=2)
        assert result_count(TagFieldStressLeaf, "stage-0") == expected_mod_count(2, 0)

        @db0.tag_fields("region")
        @db0.memo
        class TagFieldStressBase:
            def __init__(self, seq, status, owner, region, stage, priority):
                self.seq = seq
                self.status = status
                self.owner = owner
                self.region = region
                self.stage = stage
                self.priority = priority

        @db0.tag_fields("priority")
        @db0.memo
        class TagFieldStressDerivedA(TagFieldStressBase):
            pass

        @db0.tag_fields("owner", "stage")
        @db0.memo
        class TagFieldStressDerivedB(TagFieldStressBase):
            pass

        @db0.tag_fields("status")
        @db0.memo
        class TagFieldStressLeaf(TagFieldStressDerivedA):
            pass

        db0.migrate(TagFieldStressLeaf)
        db0.migrate(TagFieldStressDerivedB)
        db0.commit()

        assert _get_tag_fields(TagFieldStressBase) == ("region",)
        assert _get_tag_fields(TagFieldStressDerivedA) == ("region", "priority")
        assert _get_tag_fields(TagFieldStressDerivedB) == ("region", "owner", "stage")
        assert _get_tag_fields(TagFieldStressLeaf) == ("region", "priority", "status")
        assert result_count(TagFieldStressBase) == 4 * per_type_count
        assert result_count(TagFieldStressDerivedA) == 2 * per_type_count
        assert result_count(TagFieldStressDerivedB) == per_type_count
        assert result_count(TagFieldStressLeaf) == per_type_count
        assert result_count(TagFieldStressBase, "region-0") == expected_mod_count(3, 0, groups=4)
        assert result_count(TagFieldStressBase, "status-0") == expected_mod_count(5, 0)
        assert result_count(TagFieldStressDerivedA, "owner-0") == 0
        assert result_count(TagFieldStressDerivedA, "priority-0") == expected_mod_count(7, 0, groups=2)
        assert result_count(TagFieldStressDerivedB, "owner-0") == expected_mod_count(4, 0)
        assert result_count(TagFieldStressDerivedB, "stage-0") == expected_mod_count(2, 0)
        assert result_count(TagFieldStressLeaf, "stage-0") == 0
        assert result_count(TagFieldStressLeaf, "status-0") == expected_mod_count(5, 0)
        assert result_count(TagFieldStressLeaf, "priority-0") == expected_mod_count(7, 0)
    finally:
        db0.close()


def test_open_no_auto_migrate_prefix_override_raises(tmp_path):
    db0.init(str(tmp_path), no_auto_migrate=False)
    db0.open("tag-field-prefix-migration", no_auto_migrate=True)
    try:
        @db0.tag_fields("status")
        @db0.memo
        class PrefixNoAutoMigratedTagField:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        PrefixNoAutoMigratedTagField("open", "alice")

        @db0.tag_fields("owner")
        @db0.memo
        class PrefixNoAutoMigratedTagField:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        with pytest.raises(db0.MigrateError):
            PrefixNoAutoMigratedTagField("closed", "bob")
    finally:
        db0.close()


def test_open_no_auto_migrate_override_is_per_prefix(tmp_path):
    db0.init(str(tmp_path), no_auto_migrate=False)
    try:
        db0.open("tag-field-prefix-no-auto", no_auto_migrate=True)

        @db0.tag_fields("status")
        @db0.memo
        class PrefixScopedNoAuto:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        PrefixScopedNoAuto("open", "alice")

        @db0.tag_fields("owner")
        @db0.memo
        class PrefixScopedNoAuto:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        with pytest.raises(db0.MigrateError):
            PrefixScopedNoAuto("closed", "bob")

        db0.open("tag-field-prefix-auto")

        @db0.tag_fields("status")
        @db0.memo
        class PrefixScopedAuto:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        PrefixScopedAuto("open", "alice")

        @db0.tag_fields("owner")
        @db0.memo
        class PrefixScopedAuto:
            def __init__(self, status, owner=None):
                self.status = status
                self.owner = owner

        PrefixScopedAuto("closed", "bob")

        assert _get_tag_fields(PrefixScopedAuto) == ("owner",)
    finally:
        db0.close()
