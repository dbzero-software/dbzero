# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2026 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from dbzero.dbzero import _get_indexed_fields
from .conftest import DB0_DIR


_INDEXED_FIELDS_ATTR = "__DBZERO_INDEXED_FIELDS_ATTR"


def _indexed_names(memo_type, index, min_key=None, max_key=None, **kwargs):
    return [item.name for item in db0.find(memo_type, index.select(min_key, max_key, **kwargs))]


def _indexed_uuid_set(memo_type, index, min_key=None, max_key=None, **kwargs):
    return {db0.uuid(item) for item in db0.find(memo_type, index.select(min_key, max_key, **kwargs))}


def test_indexed_fields_are_empty_before_class_is_materialized(db0_fixture):
    @db0.memo
    @db0.indexed_fields("date", "priority")
    class IndexedFieldsUnmaterialized:
        def __init__(self, date, priority=None):
            self.date = date
            self.priority = priority

    assert _get_indexed_fields(IndexedFieldsUnmaterialized) == ()


def test_indexed_fields_report_materialized_declared_fields_only(db0_fixture):
    @db0.indexed_fields("date", "priority")
    @db0.memo
    class IndexedFieldsMaterialized:
        def __init__(self, date, priority=None):
            self.date = date

    IndexedFieldsMaterialized(20260715)

    assert _get_indexed_fields(IndexedFieldsMaterialized) == ("date",)


def test_multiple_indexed_fields_decorators_merge_in_order(db0_fixture):
    @db0.memo
    @db0.indexed_fields("priority")
    @db0.indexed_fields("date")
    class IndexedFieldsMergedBeforeMemo:
        def __init__(self, date, priority):
            self.date = date
            self.priority = priority

    IndexedFieldsMergedBeforeMemo(20260715, 1)
    assert _get_indexed_fields(IndexedFieldsMergedBeforeMemo) == ("date", "priority")

    @db0.indexed_fields("owner")
    @db0.memo
    @db0.indexed_fields("date")
    class IndexedFieldsMergedAroundMemo:
        def __init__(self, date, owner):
            self.date = date
            self.owner = owner

    IndexedFieldsMergedAroundMemo(20260715, 7)
    assert _get_indexed_fields(IndexedFieldsMergedAroundMemo) == ("date", "owner")


def test_duplicate_indexed_fields_are_ignored(db0_fixture):
    @db0.memo
    @db0.indexed_fields("priority", "date", "priority")
    @db0.indexed_fields("date", "owner")
    class IndexedFieldsDeduplicated:
        def __init__(self, date, owner, priority):
            self.date = date
            self.owner = owner
            self.priority = priority

    IndexedFieldsDeduplicated(20260715, 7, 1)
    assert _get_indexed_fields(IndexedFieldsDeduplicated) == ("date", "owner", "priority")


def test_indexed_fields_requires_string_names():
    with pytest.raises(TypeError):
        db0.indexed_fields("date", 123)


def test_empty_indexed_fields_declaration(db0_fixture):
    @db0.memo
    @db0.indexed_fields()
    class IndexedFieldsEmpty:
        pass

    assert _get_indexed_fields(IndexedFieldsEmpty) == ()


def test_existing_fields_are_resolved_during_type_attachment(db0_fixture):
    @db0.memo
    class ExistingIndexedFields:
        def __init__(self, date):
            self.date = date

    ExistingIndexedFields(20260715)

    @db0.indexed_fields("date", "missing")
    @db0.memo
    class ExistingIndexedFields:
        def __init__(self, date):
            self.date = date

    assert _get_indexed_fields(ExistingIndexedFields) == ("date",)


def test_indexed_fields_on_plain_class_only_stores_python_metadata():
    @db0.indexed_fields("date")
    class PlainIndexedFieldsClass:
        pass

    assert not db0.is_memo(PlainIndexedFieldsClass)
    assert getattr(PlainIndexedFieldsClass, _INDEXED_FIELDS_ATTR) == ("date",)

    with pytest.raises(TypeError):
        _get_indexed_fields(PlainIndexedFieldsClass)


def test_index_of_validates_arguments_and_materialization(db0_fixture):
    @db0.memo
    @db0.indexed_fields("date")
    class IndexedFieldsValidationUnmaterialized:
        def __init__(self, date):
            self.date = date

    with pytest.raises(Exception, match="memo type"):
        db0.index_of(object, "date")

    with pytest.raises(Exception, match="field_name must be a string"):
        db0.index_of(IndexedFieldsValidationUnmaterialized, 123)

    assert _get_indexed_fields(IndexedFieldsValidationUnmaterialized) == ()
    empty_index = db0.index_of(IndexedFieldsValidationUnmaterialized, "date")
    assert len(empty_index) == 0
    assert _get_indexed_fields(IndexedFieldsValidationUnmaterialized) == ("date",)

    @db0.indexed_fields("date")
    @db0.memo
    class IndexedFieldsValidation:
        def __init__(self, date):
            self.date = date
            self.owner = "alice"

    IndexedFieldsValidation(20260715)

    with pytest.raises(Exception, match="Unknown field"):
        db0.index_of(IndexedFieldsValidation, "missing")

    with pytest.raises(Exception, match="not an indexed field"):
        db0.index_of(IndexedFieldsValidation, "owner")


def test_index_of_accepts_declared_dynamic_indexed_field_before_assignment(db0_fixture):
    @db0.indexed_fields("later")
    @db0.memo
    class IndexedFieldsDynamic:
        pass

    item = IndexedFieldsDynamic()
    empty_index = db0.index_of(IndexedFieldsDynamic, "later")
    assert len(empty_index) == 0

    item.later = 7
    index = db0.index_of(IndexedFieldsDynamic, "later")
    assert len(index) == 1
    assert [obj.later for obj in db0.find(IndexedFieldsDynamic, index.select(7, 7))] == [7]


def test_index_of_explicit_scoped_prefix_must_be_open(db0_fixture):
    prefix_name = "indexed-fields-scoped-prefix"

    @db0.indexed_fields("priority")
    @db0.memo(prefix=prefix_name)
    class IndexedFieldsScoped:
        def __init__(self, priority):
            self.priority = priority

    with pytest.raises(Exception, match="Prefix is not open"):
        db0.index_of(IndexedFieldsScoped, "priority", prefix=prefix_name)

    implicit_index = db0.index_of(IndexedFieldsScoped, "priority")
    assert len(implicit_index) == 0


def test_index_of_returns_managed_index(db0_fixture):
    @db0.indexed_fields("priority")
    @db0.memo
    class IndexedFieldsManaged:
        def __init__(self, priority):
            self.priority = priority

    item = IndexedFieldsManaged(1)
    index = db0.index_of(IndexedFieldsManaged, "priority")

    assert len(index) == 1

    for operation in (
        lambda: index.add(2, item),
        lambda: index.remove(1, item),
        lambda: index.clear(),
        lambda: index.flush(),
    ):
        with pytest.raises(Exception, match="managed indexes are read-only"):
            operation()


def test_index_of_managed_index_remains_sealed_after_reopen_and_stored_reference(db0_fixture):
    @db0.indexed_fields("priority")
    @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-reopen-task")
    class IndexedFieldsReopenTask:
        def __init__(self, name, priority):
            self.name = name
            self.priority = priority

    @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-index-holder")
    class IndexedFieldsIndexHolder:
        def __init__(self, index, items):
            self.index = index
            self.items = items

    low = IndexedFieldsReopenTask("low", 1)
    high = IndexedFieldsReopenTask("high", 5)
    holder = IndexedFieldsIndexHolder(db0.index_of(IndexedFieldsReopenTask, "priority"), [low, high])
    holder_id = db0.uuid(holder)

    for operation in (
        lambda: holder.index.add(3, holder),
        lambda: holder.index.clear(),
    ):
        with pytest.raises(Exception, match="managed indexes are read-only"):
            operation()

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix")

    @db0.indexed_fields("priority")
    @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-reopen-task")
    class IndexedFieldsReopenTask:
        def __init__(self, name, priority):
            self.name = name
            self.priority = priority

    @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-index-holder")
    class IndexedFieldsIndexHolder:
        def __init__(self, index, items):
            self.index = index
            self.items = items

    reopened_holder = db0.fetch(holder_id, IndexedFieldsIndexHolder)
    stored_index = reopened_holder.index
    resolved_index = db0.index_of(IndexedFieldsReopenTask, "priority")

    assert {item.name for item in db0.find(IndexedFieldsReopenTask, stored_index.select(1, 5))} == {"low", "high"}
    assert {item.name for item in db0.find(IndexedFieldsReopenTask, resolved_index.select(1, 5))} == {"low", "high"}

    for operation in (
        lambda: stored_index.add(3, reopened_holder),
        lambda: stored_index.remove(1, reopened_holder),
        lambda: stored_index.clear(),
        lambda: stored_index.flush(),
        lambda: resolved_index.add(3, reopened_holder),
        lambda: resolved_index.remove(1, reopened_holder),
        lambda: resolved_index.clear(),
        lambda: resolved_index.flush(),
    ):
        with pytest.raises(Exception, match="managed indexes are read-only"):
            operation()


def test_indexed_field_rejects_unsupported_string_keys(db0_fixture):
    @db0.indexed_fields("code")
    @db0.memo
    class IndexedFieldsUnsupportedKey:
        def __init__(self, code):
            self.code = code

    with pytest.raises(Exception, match="Unsupported index key type"):
        IndexedFieldsUnsupportedKey("alpha")

    item = IndexedFieldsUnsupportedKey(3)
    index = db0.index_of(IndexedFieldsUnsupportedKey, "code")

    assert _get_indexed_fields(IndexedFieldsUnsupportedKey) == ("code",)
    assert len(index) == 1
    assert [obj.code for obj in db0.find(IndexedFieldsUnsupportedKey, index.select(3, 3))] == [3]

    with pytest.raises(Exception, match="does not allow adding key type|Unsupported index key type"):
        item.code = "beta"

    assert item.code == 3
    assert len(index) == 1
    assert [obj.code for obj in db0.find(IndexedFieldsUnsupportedKey, index.select(3, 3))] == [3]


def test_indexed_field_index_supports_queries_sorting_and_updates(db0_fixture):
    @db0.indexed_fields("priority")
    @db0.memo
    class IndexedFieldsTask:
        def __init__(self, name, priority):
            self.name = name
            self.priority = priority

    low = IndexedFieldsTask("low", 1)
    high = IndexedFieldsTask("high", 5)
    mid = IndexedFieldsTask("mid", 3)
    none_item = IndexedFieldsTask("none", None)
    index = db0.index_of(IndexedFieldsTask, "priority")

    assert len(index) == 4
    assert [item.name for item in db0.find(IndexedFieldsTask, index.select(2, 5))] == ["mid", "high"]
    with pytest.raises(Exception, match="Passive index queries require at least one non-passive positive predicate"):
        list(index.select(1, 5))

    assert [item.name for item in index.sort(db0.find(IndexedFieldsTask))] == ["low", "mid", "high", "none"]
    assert [item.name for item in index.sort(db0.find(IndexedFieldsTask), null_first=True)] == [
        "none", "low", "mid", "high",
    ]
    assert [item.name for item in db0.find(IndexedFieldsTask, index.select(None, None, null_first=True))] == [
        "none", "mid", "high", "low",
    ]

    mid.priority = 7
    assert [item.name for item in db0.find(IndexedFieldsTask, index.select(3, 3))] == []
    assert [item.name for item in db0.find(IndexedFieldsTask, index.select(7, 7))] == ["mid"]

    del high.priority
    assert len(index) == 3
    assert [item.name for item in db0.find(IndexedFieldsTask, index.select(5, 5))] == []

    high.priority = 2
    assert len(index) == 4
    assert [item.name for item in db0.find(IndexedFieldsTask, index.select(2, 2))] == ["high"]

    db0.delete(low)
    assert len(index) == 3
    assert {item.name for item in db0.find(IndexedFieldsTask, index.select(1, 7))} == {"high", "mid"}


def test_index_of_resolves_inherited_indexed_fields(db0_fixture):
    @db0.indexed_fields("date")
    @db0.memo
    class IndexedFieldsBase:
        def __init__(self, date):
            self.date = date

    @db0.indexed_fields("priority")
    @db0.memo
    class IndexedFieldsDerived(IndexedFieldsBase):
        def __init__(self, date, priority):
            super().__init__(date)
            self.priority = priority

    base = IndexedFieldsBase(1)
    derived = IndexedFieldsDerived(2, 10)

    base_index = db0.index_of(IndexedFieldsDerived, "date")
    assert {item.date for item in db0.find(IndexedFieldsBase, base_index.select(1, 2))} == {base.date, derived.date}

    derived_index = db0.index_of(IndexedFieldsDerived, "priority")
    assert [item.priority for item in db0.find(IndexedFieldsDerived, derived_index.select(10, 10))] == [10]

    with pytest.raises(Exception, match="Unknown field|not an indexed field"):
        db0.index_of(IndexedFieldsBase, "priority")


def test_indexed_field_added_declaration_migrates_existing_objects_and_descendants(db0_fixture):
    @db0.memo
    class MigratedIndexedBase:
        def __init__(self, name, priority=None, assign_priority=True):
            self.name = name
            if assign_priority:
                self.priority = priority

    @db0.memo
    class MigratedIndexedDerived(MigratedIndexedBase):
        pass

    base = MigratedIndexedBase("base", 3)
    derived = MigratedIndexedDerived("derived", 7)
    none_item = MigratedIndexedDerived("none", None)
    absent = MigratedIndexedBase("absent", assign_priority=False)

    @db0.indexed_fields("priority")
    @db0.memo
    class MigratedIndexedBase:
        def __init__(self, name, priority=None, assign_priority=True):
            self.name = name
            if assign_priority:
                self.priority = priority

    @db0.memo
    class MigratedIndexedDerived(MigratedIndexedBase):
        pass

    index = db0.index_of(MigratedIndexedBase, "priority")
    assert len(index) == 3
    assert _indexed_uuid_set(MigratedIndexedBase, index, 3, 7) == {db0.uuid(base), db0.uuid(derived)}
    assert [item.name for item in index.sort(db0.find(MigratedIndexedBase), null_first=True)] == [
        "none", "base", "derived",
    ]
    assert db0.uuid(absent) not in _indexed_uuid_set(MigratedIndexedBase, index, None, None, null_first=True)


def test_indexed_field_removed_declaration_destroys_old_index_and_stops_sync(db0_fixture):
    @db0.indexed_fields("priority")
    @db0.memo
    class RemovedMigratedIndexedField:
        def __init__(self, name, priority):
            self.name = name
            self.priority = priority

    item = RemovedMigratedIndexedField("item", 1)
    old_index = db0.index_of(RemovedMigratedIndexedField, "priority")
    assert _indexed_names(RemovedMigratedIndexedField, old_index, 1, 1) == ["item"]

    @db0.memo
    class RemovedMigratedIndexedField:
        def __init__(self, name, priority):
            self.name = name
            self.priority = priority

    assert _get_indexed_fields(RemovedMigratedIndexedField) == ()
    with pytest.raises(Exception, match="not an indexed field|Unknown field"):
        db0.index_of(RemovedMigratedIndexedField, "priority")

    item.priority = 2
    with pytest.raises(Exception):
        len(old_index)


def test_renamed_indexed_field_preserves_managed_index_identity(db0_fixture):
    @db0.indexed_fields("priority")
    @db0.memo
    class RenamedIndexedField:
        def __init__(self, name, priority):
            self.name = name
            self.priority = priority

    item = RenamedIndexedField("item", 1)
    old_index = db0.index_of(RenamedIndexedField, "priority")

    db0.rename_field(RenamedIndexedField, "priority", "rank")

    assert _get_indexed_fields(RenamedIndexedField) == ("rank",)
    assert _indexed_names(RenamedIndexedField, old_index, 1, 1) == ["item"]
    new_index = db0.index_of(RenamedIndexedField, "rank")
    item.rank = 5
    assert _indexed_names(RenamedIndexedField, old_index, 1, 1) == []
    assert _indexed_names(RenamedIndexedField, new_index, 5, 5) == ["item"]

    @db0.indexed_fields("rank")
    @db0.memo
    class RenamedIndexedField:
        def __init__(self, name, rank):
            self.name = name
            self.rank = rank

    assert _get_indexed_fields(RenamedIndexedField) == ("rank",)
    assert _indexed_names(RenamedIndexedField, db0.index_of(RenamedIndexedField, "rank"), 5, 5) == ["item"]


def test_failed_indexed_field_migration_rolls_back_new_indexes_and_declaration(db0_fixture):
    @db0.indexed_fields("priority")
    @db0.memo
    class FailedMigratedIndexedField:
        def __init__(self, name, priority, code):
            self.name = name
            self.priority = priority
            self.code = code

    valid = FailedMigratedIndexedField("valid", 1, 10)
    invalid = FailedMigratedIndexedField("invalid", 2, "bad-key")
    active_type = FailedMigratedIndexedField
    old_index = db0.index_of(active_type, "priority")

    @db0.indexed_fields("priority", "code")
    @db0.memo
    class FailedMigratedIndexedField:
        def __init__(self, name, priority, code):
            self.name = name
            self.priority = priority
            self.code = code

    with pytest.raises(RuntimeError, match="Unsupported index key type|does not allow adding key type"):
        FailedMigratedIndexedField("new", 3, 30)

    assert _get_indexed_fields(active_type) == ("priority",)
    assert set(_indexed_names(active_type, old_index, 1, 2)) == {"valid", "invalid"}
    with pytest.raises(Exception, match="not an indexed field|Unknown field"):
        db0.index_of(active_type, "code")


def test_no_auto_migrate_raises_and_keeps_previous_indexed_field_declaration(tmp_path):
    db0.init(str(tmp_path), no_auto_migrate=True)
    db0.open("indexed-field-no-auto-migration")
    try:
        @db0.indexed_fields("priority")
        @db0.memo
        class NoAutoMigratedIndexedField:
            def __init__(self, name, priority, owner):
                self.name = name
                self.priority = priority
                self.owner = owner

        item = NoAutoMigratedIndexedField("item", 1, 10)

        @db0.indexed_fields("owner")
        @db0.memo
        class NoAutoMigratedIndexedField:
            def __init__(self, name, priority, owner):
                self.name = name
                self.priority = priority
                self.owner = owner

        with pytest.raises(db0.MigrateError):
            NoAutoMigratedIndexedField("new", 2, 20)

        priority_index = db0.index_of(NoAutoMigratedIndexedField, "priority")
        assert _get_indexed_fields(NoAutoMigratedIndexedField) == ("priority",)
        item.priority = 3
        assert _indexed_names(NoAutoMigratedIndexedField, priority_index, 3, 3) == ["item"]
        with pytest.raises(Exception, match="not an indexed field|Unknown field"):
            db0.index_of(NoAutoMigratedIndexedField, "owner")
    finally:
        db0.close()


def test_explicit_migrate_applies_current_indexed_field_declaration_for_derived_type(tmp_path):
    db0.init(str(tmp_path), no_auto_migrate=True)
    db0.open("indexed-field-explicit-derived-migration")
    try:
        @db0.indexed_fields("priority")
        @db0.memo
        class ExplicitMigratedIndexedBase:
            def __init__(self, name, priority, owner):
                self.name = name
                self.priority = priority
                self.owner = owner

        @db0.indexed_fields("score")
        @db0.memo
        class ExplicitMigratedIndexedDerived(ExplicitMigratedIndexedBase):
            def __init__(self, name, priority, owner, score):
                super().__init__(name, priority, owner)
                self.score = score

        base = ExplicitMigratedIndexedBase("base", 1, 10)
        derived = ExplicitMigratedIndexedDerived("derived", 2, 20, 100)

        @db0.indexed_fields("owner")
        @db0.memo
        class ExplicitMigratedIndexedBase:
            def __init__(self, name, priority, owner):
                self.name = name
                self.priority = priority
                self.owner = owner

        @db0.indexed_fields("priority")
        @db0.memo
        class ExplicitMigratedIndexedDerived(ExplicitMigratedIndexedBase):
            def __init__(self, name, priority, owner, score):
                super().__init__(name, priority, owner)
                self.score = score

        with pytest.raises(db0.MigrateError):
            ExplicitMigratedIndexedDerived("new", 3, 30, 300)

        db0.migrate(ExplicitMigratedIndexedDerived)

        base_index = db0.index_of(ExplicitMigratedIndexedBase, "owner")
        derived_index = db0.index_of(ExplicitMigratedIndexedDerived, "priority")
        assert _get_indexed_fields(ExplicitMigratedIndexedBase) == ("owner",)
        assert _get_indexed_fields(ExplicitMigratedIndexedDerived) == ("priority",)
        assert _indexed_uuid_set(ExplicitMigratedIndexedBase, base_index, 10, 20) == {
            db0.uuid(base), db0.uuid(derived),
        }
        assert _indexed_uuid_set(ExplicitMigratedIndexedDerived, derived_index, 2, 2) == {db0.uuid(derived)}
    finally:
        db0.close()


def test_derived_indexed_field_rejects_ancestor_overlap_after_pending_materializes(db0_fixture):
    @db0.indexed_fields("shared")
    @db0.memo
    class PendingOverlapIndexedBase:
        pass

    @db0.indexed_fields("shared")
    @db0.memo
    class PendingOverlapIndexedDerived(PendingOverlapIndexedBase):
        pass

    item = PendingOverlapIndexedDerived()
    with pytest.raises(Exception, match="already indexed|ancestor|inherited"):
        item.shared = 1


@pytest.mark.stress_test
def test_indexed_field_migration_inheritance_persistence_stress(tmp_path):
    per_type_count = 62_500
    instance_count = 4 * per_type_count

    def expected_count(predicate, groups=1):
        return groups * sum(1 for seq in range(per_type_count) if predicate(seq))

    def query_count(memo_type, *predicates):
        return len(db0.find(memo_type, *predicates))

    def append_instances(memo_type, root):
        for seq in range(per_type_count):
            bad_key = "migration-must-rollback" if (
                memo_type.__name__ == "IndexedFieldsStressLeaf" and seq == per_type_count - 1
            ) else seq % 17
            root.items.append(
                memo_type(
                    seq,
                    seq % 11,
                    seq % 13,
                    seq % 5,
                    None if seq % 19 == 0 else seq % 7,
                    bad_key,
                )
            )
        db0.commit()

    def declare_types(base_fields, derived_a_fields, derived_b_fields, leaf_fields):
        @db0.indexed_fields(*base_fields)
        @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-stress-base")
        class IndexedFieldsStressBase:
            def __init__(self, seq, bucket, score, region, phase, bad_key):
                self.seq = seq
                self.bucket = bucket
                self.score = score
                self.region = region
                self.phase = phase
                self.bad_key = bad_key

        @db0.indexed_fields(*derived_a_fields)
        @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-stress-derived-a")
        class IndexedFieldsStressDerivedA(IndexedFieldsStressBase):
            pass

        @db0.indexed_fields(*derived_b_fields)
        @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-stress-derived-b")
        class IndexedFieldsStressDerivedB(IndexedFieldsStressBase):
            pass

        @db0.indexed_fields(*leaf_fields)
        @db0.memo(id="dbzero-software/dbzero/tests/indexed-fields-stress-leaf")
        class IndexedFieldsStressLeaf(IndexedFieldsStressDerivedA):
            pass

        return (
            IndexedFieldsStressBase,
            IndexedFieldsStressDerivedA,
            IndexedFieldsStressDerivedB,
            IndexedFieldsStressLeaf,
        )

    def declare_root():
        @db0.memo(
            singleton=True,
            id="dbzero-software/dbzero/tests/indexed-fields-stress-root",
        )
        class IndexedFieldsStressRoot:
            def __init__(self):
                self.items = []

        return IndexedFieldsStressRoot

    db0.init(str(tmp_path), no_auto_migrate=True, autocommit=False)
    db0.open("indexed-field-inheritance-stress", slab_size=1 << 30)
    try:
        (
            IndexedFieldsStressBase,
            IndexedFieldsStressDerivedA,
            IndexedFieldsStressDerivedB,
            IndexedFieldsStressLeaf,
        ) = declare_types(("bucket",), ("score",), ("region",), ("phase",))
        IndexedFieldsStressRoot = declare_root()

        root = IndexedFieldsStressRoot()
        for memo_type in (
            IndexedFieldsStressBase,
            IndexedFieldsStressDerivedA,
            IndexedFieldsStressDerivedB,
            IndexedFieldsStressLeaf,
        ):
            append_instances(memo_type, root)

        assert len(root.items) == instance_count
        old_bucket_index = db0.index_of(IndexedFieldsStressBase, "bucket")
        assert len(old_bucket_index) == instance_count

        # Adding bad_key must roll back after scanning the populated hierarchy:
        # one leaf has a string key, which managed indexes do not support.
        (
            IndexedFieldsStressBase,
            IndexedFieldsStressDerivedA,
            IndexedFieldsStressDerivedB,
            IndexedFieldsStressLeaf,
        ) = declare_types(("bucket", "bad_key"), ("score",), ("region",), ("phase",))

        with pytest.raises(RuntimeError, match="Unsupported index key type|does not allow adding key type"):
            db0.migrate(IndexedFieldsStressBase)

        assert _get_indexed_fields(IndexedFieldsStressBase) == ("bucket",)
        assert len(old_bucket_index) == instance_count
        assert query_count(
            IndexedFieldsStressBase,
            old_bucket_index.select(3, 3),
        ) == expected_count(lambda seq: seq % 11 == 3, groups=4)
        with pytest.raises(Exception, match="not an indexed field|Unknown field"):
            db0.index_of(IndexedFieldsStressBase, "bad_key")

        (
            IndexedFieldsStressBase,
            IndexedFieldsStressDerivedA,
            IndexedFieldsStressDerivedB,
            IndexedFieldsStressLeaf,
        ) = declare_types(("region",), ("bucket",), ("score",), ("phase",))

        db0.migrate(IndexedFieldsStressLeaf)
        db0.migrate(IndexedFieldsStressDerivedB)
        db0.commit()

        assert _get_indexed_fields(IndexedFieldsStressBase) == ("region",)
        assert _get_indexed_fields(IndexedFieldsStressDerivedA) == ("bucket",)
        assert _get_indexed_fields(IndexedFieldsStressDerivedB) == ("score",)
        assert _get_indexed_fields(IndexedFieldsStressLeaf) == ("phase",)

        db0.close()
        db0.init(str(tmp_path), no_auto_migrate=True, autocommit=False)
        db0.open("indexed-field-inheritance-stress", slab_size=1 << 30)

        (
            IndexedFieldsStressBase,
            IndexedFieldsStressDerivedA,
            IndexedFieldsStressDerivedB,
            IndexedFieldsStressLeaf,
        ) = declare_types(("region",), ("bucket",), ("score",), ("phase",))
        IndexedFieldsStressRoot = declare_root()

        assert len(IndexedFieldsStressRoot().items) == instance_count

        region_index = db0.index_of(db0.fields_of(IndexedFieldsStressBase).region)
        bucket_index = db0.index_of(IndexedFieldsStressDerivedA, "bucket")
        score_index = db0.index_of(IndexedFieldsStressDerivedB, "score")
        phase_index = db0.index_of(IndexedFieldsStressLeaf, "phase")

        assert len(region_index) == instance_count
        assert len(bucket_index) == 2 * per_type_count
        assert len(score_index) == per_type_count
        assert len(phase_index) == per_type_count

        assert query_count(
            IndexedFieldsStressBase,
            region_index.select(2, 2),
        ) == expected_count(lambda seq: seq % 5 == 2, groups=4)
        assert query_count(
            IndexedFieldsStressBase,
            region_index.select(None, 1),
        ) == expected_count(lambda seq: seq % 5 <= 1, groups=4)
        assert query_count(
            IndexedFieldsStressDerivedA,
            bucket_index.select(3, 3),
            region_index.select(2, 4),
        ) == expected_count(
            lambda seq: seq % 11 == 3 and 2 <= seq % 5 <= 4,
            groups=2,
        )
        assert query_count(
            IndexedFieldsStressLeaf,
            bucket_index.select(0, 10),
            phase_index.select(4, 4),
        ) == expected_count(lambda seq: seq % 19 != 0 and seq % 7 == 4)

        null_first = phase_index.sort(db0.find(IndexedFieldsStressLeaf), null_first=True)
        first_phases = []
        for item in null_first:
            first_phases.append(item.phase)
            if len(first_phases) == 32:
                break
        assert first_phases == [None] * 32

        descending_regions = region_index.sort(
            db0.find(IndexedFieldsStressDerivedB, score_index.select(3, 8)),
            desc=True,
        )
        first_regions = []
        for item in descending_regions:
            assert 3 <= item.score <= 8
            first_regions.append(item.region)
            if len(first_regions) == 128:
                break
        assert first_regions == sorted(first_regions, reverse=True)
    finally:
        db0.close()
