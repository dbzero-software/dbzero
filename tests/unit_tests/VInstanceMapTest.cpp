// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <set>
#include <string>
#include <vector>
#include <dbzero/core/collections/map/VInstanceMap.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/PyAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <utils/TestBase.hpp>
#include <utils/SubClass.hpp>

extern "C" PyObject *PyInit_dbzero(void);

namespace tests

{

    using namespace db0;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_v_instance_map_value: db0::o_fixed<o_v_instance_map_value>
    {
        std::uint32_t m_value = 0;
        std::uint32_t m_extra = 0;

        o_v_instance_map_value() = default;

        o_v_instance_map_value(std::uint32_t value, std::uint32_t extra)
            : m_value(value)
            , m_extra(extra)
        {
        }
    };
DB0_PACKED_END

    class VInstanceMapTest: public MemspaceTestBase
    {
    protected:
        VInstanceMapTest()
            : m_shared_object_list(1)
            , m_memspace(getMemspace())
            , m_cache(m_memspace, m_shared_object_list)
        {
        }

        FixedObjectList m_shared_object_list;
        Memspace m_memspace;
        VObjectCache m_cache;
    };

    TEST_F(VInstanceMapTest, testCanInsertAndLookupInstancesByNumericKey)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto inserted = cut.insert(42, 691, 17);

        auto found = cut.get(42);
        ASSERT_TRUE(found);
        ASSERT_EQ(inserted->getAddress(), found->getAddress());
        ASSERT_EQ(691u, (*found)->m_value);
        ASSERT_EQ(17u, (*found)->m_extra);
    }

    TEST_F(VInstanceMapTest, testKeysAreStoredInAscendingOrder)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        cut.insert(30, 3, 30);
        cut.insert(10, 1, 10);
        cut.insert(20, 2, 20);

        auto it = cut.begin();
        ASSERT_EQ(10u, (*it).key);
        ++it;
        ASSERT_EQ(20u, (*it).key);
        ++it;
        ASSERT_EQ(30u, (*it).key);
    }

    TEST_F(VInstanceMapTest, testEraseKeyRemovesTheMapping)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        cut.insert(1, 10, 1);
        cut.insert(2, 20, 2);

        ASSERT_TRUE(cut.erase(1));
        ASSERT_FALSE(cut.tryGet(1));
        ASSERT_TRUE(cut.tryGet(2));
        ASSERT_FALSE(cut.erase(3));
    }

    TEST_F(VInstanceMapTest, testEraseKeyDestroysUnderlyingInstance)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        Address value_address;
        {
            auto inserted = cut.insert(1, 10, 1);
            value_address = inserted->getAddress();
        }

        ASSERT_NO_THROW(m_memspace.getAllocator().getAllocSize(value_address));
        ASSERT_TRUE(cut.erase(1));
        ASSERT_FALSE(cut.tryGet(1));
        ASSERT_ANY_THROW(m_memspace.getAllocator().getAllocSize(value_address));
    }

    TEST_F(VInstanceMapTest, testReplacingExistingKeyDestroysOldInstance)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto first = cut.insert(1, 10, 1);
        auto first_address = first->getAddress();
        auto second = cut.insert(1, 20, 2);

        ASSERT_NE(first_address, second->getAddress());
        ASSERT_EQ(second->getAddress(), cut.get(1)->getAddress());
        ASSERT_ANY_THROW(m_memspace.getAllocator().getAllocSize(first_address));
    }

    TEST_F(VInstanceMapTest, testCanReopenInstanceAfterWeakCacheEntryExpired)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        {
            auto inserted = cut.insert(5, 81392, 99);
            ASSERT_EQ(81392u, (*inserted)->m_value);
        }

        auto reopened = cut.get(5);
        ASSERT_TRUE(reopened);
        ASSERT_EQ(81392u, (*reopened)->m_value);
    }

    TEST_F(VInstanceMapTest, testFindOrCreateReusesExistingValue)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto first = cut.findOrCreate(7, 100, 1);
        auto second = cut.findOrCreate(7, 200, 2);

        ASSERT_EQ(first->getAddress(), second->getAddress());
        ASSERT_EQ(100u, (*second)->m_value);
        ASSERT_EQ(1u, (*second)->m_extra);
    }

    TEST_F(VInstanceMapTest, testForAllVisitsActiveValuesEvenAfterVObjectCacheEviction)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto first = cut.insert(1, 10, 1);
        auto second = cut.insert(2, 20, 2);

        std::set<std::uint32_t> values;
        auto active_count = cut.forEachActive([&](ValueT &value) {
            values.insert(value->m_value);
        });

        ASSERT_EQ(2u, active_count);
        ASSERT_EQ(values, (std::set<std::uint32_t> { 10, 20 }));
        ASSERT_TRUE(first);
        ASSERT_TRUE(second);
    }

    TEST_F(VInstanceMapTest, testForAllCleansExpiredWeakValues)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        {
            auto first = cut.insert(1, 10, 1);
            ASSERT_TRUE(first);
        }
        auto second = cut.insert(2, 20, 2);

        std::vector<std::uint32_t> values;
        auto active_count = cut.forEachActive([&](ValueT &value) {
            values.push_back(value->m_value);
        });

        ASSERT_EQ(1u, active_count);
        ASSERT_EQ(values, (std::vector<std::uint32_t> { 20 }));

        std::size_t second_pass_count = cut.forEachActive([](ValueT &) {});
        ASSERT_EQ(1u, second_pass_count);
        ASSERT_TRUE(second);
    }

    TEST_F(VInstanceMapTest, testForAllCanStopEarly)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto first = cut.insert(10, 1, 10);
        auto second = cut.insert(20, 2, 20);

        std::size_t visited = 0;
        auto active_count = cut.forEachActive([&](ValueT &) {
            ++visited;
            return false;
        });

        ASSERT_EQ(1u, active_count);
        ASSERT_EQ(1u, visited);
        ASSERT_TRUE(first);
        ASSERT_TRUE(second);
    }

    class VInstanceMapTagIndexTest: public FixtureTestBase
    {
        void SetUp() override
        {
            auto fixture = getFixture();
            db0::object_model::initializer()(fixture, true, false, false);
        }
    };

    void initializeDbzeroPythonBindingsOnce()
    {
        static bool py_dbzero_initialized = false;
        if (!py_dbzero_initialized) {
            auto py_dbzero_module = Py_OWN(PyInit_dbzero());
            ASSERT_TRUE(py_dbzero_module.get());
            py_dbzero_initialized = true;
        }
    }

    db0::python::shared_py_object<db0::python::MemoObject*> makeMaterializedMemo(db0::swine_ptr<db0::Fixture> &fixture)
    {
        initializeDbzeroPythonBindingsOnce();

        static std::uint64_t memo_type_index = 0;
        auto class_name = "VInstanceMapCompositeMemo" + std::to_string(memo_type_index);
        auto type_id = "tests/" + class_name;
        ++memo_type_index;

        if (PyRun_SimpleString(("class " + class_name + ": pass\n").c_str()) != 0) {
            ADD_FAILURE() << "Failed to define Python memo test class";
            return {};
        }
        auto main_module = Py_BORROW(PyImport_AddModule("__main__"));
        EXPECT_TRUE(main_module.get());
        auto py_class = Py_OWN(PyObject_GetAttrString(*main_module, class_name.c_str()));
        EXPECT_TRUE(py_class.get());
        auto args = Py_OWN(PyTuple_Pack(1, py_class.get()));
        auto kwargs = Py_OWN(PyDict_New());
        EXPECT_TRUE(args.get());
        EXPECT_TRUE(kwargs.get());
        auto py_type_id = Py_OWN(PyUnicode_FromString(type_id.c_str()));
        EXPECT_TRUE(py_type_id.get());
        if (PyDict_SetItemString(*kwargs, "id", py_type_id.get()) != 0) {
            ADD_FAILURE() << "Failed to set Python memo type id";
            return {};
        }
        auto py_memo_type = Py_OWN(db0::python::PyAPI_wrapPyClass(nullptr, args.get(), kwargs.get()));
        EXPECT_TRUE(py_memo_type.get());

        auto object_type = getTestClass(fixture);
        auto memo_ptr = Py_OWN(db0::python::MemoObjectStub_new(reinterpret_cast<PyTypeObject *>(py_memo_type.get())));
        memo_ptr->makeNew(object_type);
        {
            FixtureLock lock(fixture);
            memo_ptr->modifyExt().postInit(lock);
        }
        return memo_ptr;
    }

    void assertTagIndexContainsObject(db0::object_model::TagIndex &tag_index,
        db0::object_model::TagIndex::ShortTagT tag, UniqueAddress object_address)
    {
        auto query = tag_index.makeIterator(tag);
        ASSERT_FALSE(query->isEnd());
        UniqueAddress result;
        query->next(&result);
        ASSERT_EQ(object_address, result);
        ASSERT_TRUE(query->isEnd());
    }

    void assertTagIndexDoesNotContainObject(db0::object_model::TagIndex &tag_index,
        db0::object_model::TagIndex::ShortTagT tag)
    {
        auto query = tag_index.makeIterator(tag);
        ASSERT_TRUE(query->isEnd());
    }

    TEST_F(VInstanceMapTagIndexTest, testCanStoreTagIndexInstances)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();

        VInstanceMap<std::uint64_t, TagIndex> cut(*fixture, fixture->getVObjectCache());
        auto inserted = cut.insert(
            11,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        auto found = cut.get(
            11,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        ASSERT_TRUE(found);
        ASSERT_EQ(inserted->getAddress(), found->getAddress());
        ASSERT_TRUE(found->empty());
    }

    TEST_F(VInstanceMapTagIndexTest, testTagIndexPersistsShortTagIndexMapAddress)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        ASSERT_TRUE(tag_index.tryGetShortTagIndexMap());
        ASSERT_NE(0u, tag_index->m_reserved[0]);
        ASSERT_EQ(tag_index.getShortTagIndexMap().getAddress().getOffset(), tag_index->m_reserved[0]);

        TagIndex reopened(
            fixture->myPtr(tag_index.getAddress()),
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        ASSERT_TRUE(reopened.tryGetShortTagIndexMap());
        ASSERT_EQ(tag_index->m_reserved[0], reopened.getShortTagIndexMap().getAddress().getOffset());
    }

    TEST_F(VInstanceMapTagIndexTest, testTagIndexLazilyCreatesShortTagIndexMapForLegacyZeroAddress)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );
        tag_index.modify().m_reserved[0] = 0;

        TagIndex reopened(
            fixture->myPtr(tag_index.getAddress()),
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        ASSERT_FALSE(reopened.tryGetShortTagIndexMap());
        const TagIndex &const_reopened = reopened;
        ASSERT_ANY_THROW(const_reopened.getShortTagIndexMap());

        auto &short_tag_index_map = reopened.getShortTagIndexMap();
        ASSERT_TRUE(reopened.tryGetShortTagIndexMap());
        ASSERT_NE(0u, reopened->m_reserved[0]);
        ASSERT_EQ(short_tag_index_map.getAddress().getOffset(), reopened->m_reserved[0]);
    }

    TEST_F(VInstanceMapTagIndexTest, testAddCompositeCreatesChildTagIndex)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();
        auto memo_ptr = makeMaterializedMemo(fixture);
        ASSERT_TRUE(memo_ptr.get());

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        constexpr TagIndex::ShortTagT composite_tag = 12345;
        ASSERT_FALSE(tag_index.tryUpdateComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag));
        auto child_tag_index = tag_index.addComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);

        ASSERT_TRUE(child_tag_index);
        ASSERT_EQ(
            child_tag_index->getAddress(),
            tag_index.getShortTagIndexMap().get(
                composite_tag,
                class_factory,
                enum_factory,
                fixture->getLimitedStringPool(),
                fixture->getVObjectCache(),
                mutation_log
            )->getAddress()
        );

        auto update_child_tag_index = tag_index.tryUpdateComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);
        ASSERT_TRUE(update_child_tag_index);
        ASSERT_EQ(child_tag_index->getAddress(), update_child_tag_index->getAddress());

        ASSERT_TRUE(child_tag_index->empty());
        update_child_tag_index.reset();
        child_tag_index.reset();
        memo_ptr.reset();
    }

    TEST_F(VInstanceMapTagIndexTest, testFlushForwardsToCompositeTagIndexes)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();
        auto memo_ptr = makeMaterializedMemo(fixture);
        ASSERT_TRUE(memo_ptr.get());

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        constexpr TagIndex::ShortTagT composite_tag = 12346;
        auto child_tag_index = tag_index.addComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);
        child_tag_index->addTag(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag, false);
        ASSERT_FALSE(tag_index.empty());

        ASSERT_TRUE(tag_index.flush());

        ASSERT_TRUE(tag_index.empty());
        assertTagIndexContainsObject(*child_tag_index, composite_tag, memo_ptr->ext().getUniqueAddress());
        child_tag_index.reset();
        memo_ptr.reset();
    }

    TEST_F(VInstanceMapTagIndexTest, testFlushReturnsFalseWhenTagIndexContainsNoElements)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();
        auto memo_ptr = makeMaterializedMemo(fixture);
        ASSERT_TRUE(memo_ptr.get());

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        ASSERT_FALSE(tag_index.flush());

        constexpr TagIndex::ShortTagT composite_tag = 12351;
        auto child_tag_index = tag_index.addComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);
        ASSERT_TRUE(child_tag_index->empty());
        ASSERT_FALSE(tag_index.flush());
        ASSERT_FALSE(tag_index.getShortTagIndexMap().tryGet(
            composite_tag,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        ));

        child_tag_index.reset();
        memo_ptr.reset();
    }

    TEST_F(VInstanceMapTagIndexTest, testRollbackForwardsToCompositeTagIndexes)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();
        auto memo_ptr = makeMaterializedMemo(fixture);
        ASSERT_TRUE(memo_ptr.get());

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        constexpr TagIndex::ShortTagT composite_tag = 12347;
        auto child_tag_index = tag_index.addComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);
        child_tag_index->addTag(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag, false);
        ASSERT_FALSE(tag_index.empty());

        tag_index.rollback();

        ASSERT_TRUE(tag_index.empty());
        ASSERT_TRUE(child_tag_index->empty());
        child_tag_index.reset();
        memo_ptr.reset();
    }

    TEST_F(VInstanceMapTagIndexTest, testCloseForwardsToCompositeTagIndexes)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();
        auto memo_ptr = makeMaterializedMemo(fixture);
        ASSERT_TRUE(memo_ptr.get());

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        constexpr TagIndex::ShortTagT composite_tag = 12348;
        auto child_tag_index = tag_index.addComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);
        child_tag_index->addTag(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag, false);
        ASSERT_FALSE(tag_index.empty());

        tag_index.close();

        ASSERT_TRUE(tag_index.empty());
        ASSERT_TRUE(child_tag_index->empty());
        child_tag_index.reset();
        memo_ptr.reset();
    }

    TEST_F(VInstanceMapTagIndexTest, testCommitForwardsToCompositeTagIndexes)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();
        auto memo_ptr = makeMaterializedMemo(fixture);
        ASSERT_TRUE(memo_ptr.get());

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        constexpr TagIndex::ShortTagT composite_tag = 12349;
        auto child_tag_index = tag_index.addComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);
        child_tag_index->addTag(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag, false);
        auto child_address = child_tag_index->getAddress();

        tag_index.commit();

        auto reopened_child = tag_index.getShortTagIndexMap().get(
            composite_tag,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );
        ASSERT_EQ(child_address, reopened_child->getAddress());
        assertTagIndexContainsObject(*reopened_child, composite_tag, memo_ptr->ext().getUniqueAddress());
        child_tag_index.reset();
        reopened_child.reset();
        memo_ptr.reset();
    }

    TEST_F(VInstanceMapTagIndexTest, testDetachForwardsToCompositeTagIndexes)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();
        auto memo_ptr = makeMaterializedMemo(fixture);
        ASSERT_TRUE(memo_ptr.get());

        TagIndex tag_index(
            *fixture,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        constexpr TagIndex::ShortTagT composite_tag = 12350;
        auto child_tag_index = tag_index.addComposite(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag);
        child_tag_index->addTag(reinterpret_cast<PyObject *>(memo_ptr.get()), composite_tag, false);

        tag_index.flush();
        tag_index.detach();

        assertTagIndexContainsObject(*child_tag_index, composite_tag, memo_ptr->ext().getUniqueAddress());
        child_tag_index.reset();
        memo_ptr.reset();
    }

}
