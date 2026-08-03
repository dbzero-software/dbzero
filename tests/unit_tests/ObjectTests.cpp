// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <utils/SubClass.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <cstring>
#include <iostream>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{

    namespace
    {
        Address makeString(db0::swine_ptr<Fixture> &fixture, const char *value)
        {
            return v_object<o_string>(*fixture, value).getAddress();
        }

        Address makeBinary(db0::swine_ptr<Fixture> &fixture, const char *value)
        {
            return v_object<o_binary>(
                *fixture, reinterpret_cast<const std::byte *>(value), std::strlen(value)
            ).getAddress();
        }

        Address findMemberAddress(const Object &object, StorageClass storage_class)
        {
            Address result;
            object.forAll([&](const std::string &, const XValue &xvalue, unsigned int) {
                if (xvalue.m_type == storage_class) {
                    result = xvalue.m_value.asAddress();
                    return false;
                }
                return true;
            });
            return result;
        }

        Address makeTuple(db0::swine_ptr<Fixture> &fixture, std::initializer_list<o_typed_item> items)
        {
            Tuple tuple(fixture, Tuple::tag_new_tuple(), items.size());
            std::size_t index = 0;
            for (auto item : items) {
                tuple.modify().items()[index++] = item;
            }
            tuple.incRef(false);
            return tuple.getAddress();
        }

        void assignFirstPosValue(Object &object, StorageClass storage_class, Value value)
        {
            auto &pos_vt = object.modify().pos_vt();
            ASSERT_GT(pos_vt.size(), 0u);
            pos_vt.set(0, storage_class, value);
        }
    }
    
    class ObjectTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {
            drop(file_name);
        }
    };
    
    TEST_F( ObjectTest , testObjectMeasure )
    {
        PosVT::Data data;
        data.m_types = std::vector<StorageClass> { StorageClass::INT64, StorageClass::POOLED_STRING };
        data.m_values = std::vector<Value> { Value(0), Value(0) };

        ASSERT_EQ ( 52u, o_object::measure(0, {0, 0}, 0, data, 0) );
    }
    
    TEST_F( ObjectTest , testObjectInitializerCanBeFoundIfAdded )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> class_mock = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(class_mock);
        ObjectInitializerManager cut;
        ASSERT_EQ(cut.findInitializer(*object_1), nullptr);
        cut.addInitializer(*object_1, class_mock);
        ASSERT_NE(cut.findInitializer(*object_1), nullptr);
        object_1->~Object();
        workspace.close();
    }
    
    TEST_F( ObjectTest , testObjectCanBeInstantiatedOnBaseWorkspace )
    {        
        BaseWorkspace workspace;
        auto memspace = workspace.getMemspace(prefix_name);
        PosVT::Data data(8);

        using Object = v_object<db0::object_model::o_object>;
        ASSERT_NO_THROW( Object(memspace, 0, std::make_pair(0u, 0u), 0, data, 0) );
        workspace.close();
    }
    
    TEST_F( ObjectTest , testNewObjectSpeed )
    {        
        BaseWorkspace workspace;
        auto memspace = workspace.getMemspace(prefix_name);
        using Object = v_object<db0::object_model::o_object>;
        PosVT::Data data(8);
        std::size_t size_of = db0::object_model::o_object::measure(0, std::make_pair(0u, 0u), 0, data, 0);

        // measure speed
        auto start = std::chrono::high_resolution_clock::now();
        std::size_t total_bytes = 0;
        std::size_t alloc_count = 100000;
        for (unsigned int i = 0; i < alloc_count; ++i) {
            Object(memspace, 0, std::make_pair(0u, 0u), 0, 8, 0);
            total_bytes += size_of;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "New object took: " << elapsed.count() << "ms" << std::endl;
        std::cout << "Total bytes: " << total_bytes << std::endl;
        std::cout << "MB / sec : " << (total_bytes / 1024.0 / 1024.0) * 1000.0 / elapsed.count() << std::endl;
        std::cout << "Allocs / sec : " << alloc_count * 1000.0 / elapsed.count() << std::endl;
        workspace.close();
    }
    
    TEST_F( ObjectTest , testNewObjectWithValues )
    {        
        BaseWorkspace workspace;
        auto memspace = workspace.getMemspace(prefix_name);
        using Object = v_object<db0::object_model::o_object>;

        PosVT::Data data;
        data.m_types = std::vector<StorageClass> { StorageClass::INT64, StorageClass::POOLED_STRING };
        data.m_values = std::vector<Value> { Value(0), Value(0) };
        
        unsigned int repeat = 5;
        for (unsigned int i = 0; i < repeat; ++i) {
            workspace.getCacheRecycler().clear();
            // cache utilization
            auto cache_size_0 = workspace.getCacheRecycler().size();
            std::vector<Object> objects;
            std::size_t alloc_count = 1000;
            for (unsigned int i = 0; i < alloc_count; ++i) {
                objects.emplace_back(memspace, 0, std::make_pair(0u, 0u), 0, data, 0);
            }
            
            workspace.getCacheRecycler().clear();
            auto cache_size_1 = workspace.getCacheRecycler().size();            
            ASSERT_TRUE(cache_size_1 > cache_size_0);
            objects.clear();

            workspace.getCacheRecycler().clear();
            auto cache_size_2 = workspace.getCacheRecycler().size();
            
            // make sure cache utlization is reduced after releasing some objects
            // note that utilization is still higher than the initial one which is due to
            // administrative data created by the allocators
            ASSERT_TRUE(cache_size_2 < cache_size_1);
        }
        workspace.close();
    }

    TEST_F( ObjectTest , testAtomicTupleValueRollbackReleasesAllocatorState )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto object_class = getTestClass(fixture);
            object_class->addField("value", 0);

            PosVT::Data data;
            data.m_types = { StorageClass::INT64 };
            data.m_values = { Value(0) };
            Object object(fixture, object_class, std::make_pair(0u, 0u), data, 0);
            object.incRef(false);
            auto object_address = object.getAddress();
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto tuple_address = makeTuple(fixture, {
                    { StorageClass::STRING_REF, Value(makeString(fixture, "atomic")) }
                });
                assignFirstPosValue(object, StorageClass::DB0_TUPLE, Value(tuple_address));
            }
            fixture->cancelAtomic(nullptr);

            Object reopened(fixture, object_address, object_class, Object::with_type_hint());
            ASSERT_EQ(reopened->pos_vt().types()[0], StorageClass::INT64);
            ASSERT_EQ(reopened->pos_vt().values()[0], Value(0));
            ASSERT_NO_THROW(fixture->commit());
        }
        workspace.close();
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsCommittedRawBinaryReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            {
                v_object<o_binary> initial(fixture->myPtr(initial_address));
                ASSERT_EQ(initial->size(), 7u);
                ASSERT_EQ(std::memcmp(initial->getBuffer(), "initial", 7), 0);
            }

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            // Narrowed raw repro: no Object instance or Python value conversion is
            // required. A committed raw member allocation becomes invalid after
            // atomic cancel when the same pre-commit transaction also updates
            // Class schema metadata after that allocation.
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsLaterCommittedRawBinaryReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto dummy_address = makeBinary(fixture, "dummy");
            auto initial_address = makeBinary(fixture, "initial");
            ASSERT_TRUE(!!dummy_address);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsKVIndexReferencedBinaryReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            KV_Index kv_index(*fixture, XValue(1, StorageClass::DB0_BYTES, Value(initial_address)));
            auto kv_address = kv_index.getAddress();
            auto kv_type = kv_index.getIndexType();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            KV_Index reopened(std::make_pair(fixture.get(), kv_address), kv_type);
            XValue value(1);
            ASSERT_TRUE(reopened.findOne(value));
            ASSERT_EQ(value.m_type, StorageClass::DB0_BYTES);
            ASSERT_EQ(value.m_value.asAddress(), initial_address);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(value.m_value.asAddress()));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsInsertedKVIndexBinaryReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            KV_Index kv_index(*fixture, XValue(0, StorageClass::INT64, Value(1)));
            auto initial_address = makeBinary(fixture, "initial");
            kv_index.insert(XValue(1, StorageClass::DB0_BYTES, Value(initial_address)));
            auto kv_address = kv_index.getAddress();
            auto kv_type = kv_index.getIndexType();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            KV_Index reopened(std::make_pair(fixture.get(), kv_address), kv_type);
            XValue value(1);
            ASSERT_TRUE(reopened.findOne(value));
            ASSERT_EQ(value.m_type, StorageClass::DB0_BYTES);
            ASSERT_EQ(value.m_value.asAddress(), initial_address);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(value.m_value.asAddress()));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterObjectRealmAllocationReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto object_realm_address = fixture->alloc(128, 0, Object::REALM_ID);
            auto initial_address = makeBinary(fixture, "initial");
            ASSERT_TRUE(!!object_realm_address);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterEarlierRawMutationReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            v_object<o_binary> holder(*fixture, 64);
            auto initial_address = makeBinary(fixture, "initial");
            std::memcpy(holder.modify().getBuffer(), &initial_address, sizeof(initial_address));
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsFixtureLockAllocatedBinaryReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            Address initial_address;
            {
                db0::FixtureLock lock(fixture);
                initial_address = makeBinary(fixture, "initial");
            }
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterClassSchemaUpdateReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto object_class = getTestClass(fixture);
            auto member_id = object_class->addField("value", 0);
            auto field_id = member_id.get(0);
            auto initial_address = makeBinary(fixture, "initial");
            object_class->addToSchema(field_id, 0, getSchemaTypeId(StorageClass::DB0_BYTES, Value(initial_address)));
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterStandaloneSchemaUpdateReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            Schema schema(*fixture, []() { return 1u; });
            auto initial_address = makeBinary(fixture, "initial");
            schema.add(FieldID::fromIndex(1), getSchemaTypeId(StorageClass::DB0_BYTES, Value(initial_address)));
            schema.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawSchemaMatrixSetReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::VLimitedMatrix<o_schema, lofi_store<2>::size()> matrix(*fixture);
            auto initial_address = makeBinary(fixture, "initial");
            std::vector<std::tuple<o_schema::FieldLoc, SchemaTypeId, int>> schema_items = {
                { {1, 0}, getSchemaTypeId(StorageClass::DB0_BYTES, Value(initial_address)), 1 }
            };
            matrix.set({1, 0}, o_schema(*fixture, schema_items.begin(), schema_items.end()));
            matrix.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawIntegerMatrixSetReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::VLimitedMatrix<std::uint64_t, lofi_store<2>::size()> matrix(*fixture);
            auto initial_address = makeBinary(fixture, "initial");
            matrix.set({1, 0}, 123);
            matrix.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawOptionalBVectorSetReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::v_bvector<db0::o_optional_item<std::uint64_t>> vector(*fixture);
            auto initial_address = makeBinary(fixture, "initial");
            vector.setItem(1, db0::o_optional_item<std::uint64_t>(123));
            vector.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawBVectorSetReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::v_bvector<std::uint64_t> vector(*fixture);
            auto initial_address = makeBinary(fixture, "initial");
            vector.setItem(1, 123);
            vector.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawBVectorFirstSetReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::v_bvector<std::uint64_t> vector(*fixture);
            auto initial_address = makeBinary(fixture, "initial");
            vector.setItem(0, 123);
            vector.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawBVectorConstructReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::v_bvector<std::uint64_t> vector(*fixture);
            auto initial_address = makeBinary(fixture, "initial");
            vector.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawBVectorGrowReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::v_bvector<std::uint64_t> vector(*fixture);
            auto initial_address = makeBinary(fixture, "initial");
            vector.growBy(1);
            vector.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawDataBlockReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            db0::v_object<db0::o_block_data<std::uint64_t, 0>> data_block(*fixture, fixture->getPageSize());
            data_block.modify().modifyItem(0) = 123;
            data_block.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawBVectorRootReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            db0::v_object<db0::o_bvector<db0::Address>> root(*fixture, fixture->getPageSize());
            root.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawBVectorRootSizedAllocReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            auto vector_root_size = db0::o_bvector<db0::Address>::measure(fixture->getPageSize());
            auto vector_root_address = fixture->alloc(vector_root_size);
            ASSERT_TRUE(!!vector_root_address);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterRawBVectorRootSizedMappedAllocReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            auto vector_root_size = db0::o_bvector<db0::Address>::measure(fixture->getPageSize());
            auto vector_root_address = fixture->alloc(vector_root_size);
            auto vector_root_lock = fixture->getPrefix().mapRange(
                vector_root_address.getOffset(), vector_root_size, { db0::AccessOptions::write }
            );
            vector_root_lock.modify();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterEarlierRawBVectorRootReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            db0::v_object<db0::o_bvector<db0::Address>> root(*fixture, fixture->getPageSize());
            auto initial_address = makeBinary(fixture, "initial");
            root.commit();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterEarlierRawBVectorRootSizedAllocReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto vector_root_size = db0::o_bvector<db0::Address>::measure(fixture->getPageSize());
            auto vector_root_address = fixture->alloc(vector_root_size);
            ASSERT_TRUE(!!vector_root_address);
            auto initial_address = makeBinary(fixture, "initial");
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryAfterEarlierRawBVectorRootSizedMappedAllocReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto vector_root_size = db0::o_bvector<db0::Address>::measure(fixture->getPageSize());
            auto vector_root_address = fixture->alloc(vector_root_size);
            auto vector_root_lock = fixture->getPrefix().mapRange(
                vector_root_address.getOffset(), vector_root_size, { db0::AccessOptions::write }
            );
            vector_root_lock.modify();
            auto initial_address = makeBinary(fixture, "initial");
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawAllocationKeepsEarlierRawAllocationReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto vector_root_size = db0::o_bvector<db0::Address>::measure(fixture->getPageSize());
            auto earlier_address = fixture->alloc(vector_root_size);
            ASSERT_TRUE(!!earlier_address);

            auto watched_size = db0::o_binary::measure(7);
            auto watched_address = fixture->alloc(watched_size);
            ASSERT_TRUE(!!watched_address);
            ASSERT_TRUE(fixture->isAddressValid(watched_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = fixture->alloc(db0::o_binary::measure(5));
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(watched_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(watched_address, 0));
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryBeforeLaterRawBinaryReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            auto later_address = makeBinary(fixture, "later");
            ASSERT_TRUE(!!later_address);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterRawBinaryAllocationKeepsBinaryBeforeLaterSimpleObjectReadable )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto initial_address = makeBinary(fixture, "initial");
            v_object<o_simple<std::uint64_t>> later(*fixture, 123);
            ASSERT_TRUE(!!later.getAddress());
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());

            fixture->beginAtomic(nullptr);
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);

            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            v_object<o_binary> restored(fixture->myPtr(initial_address));
            ASSERT_EQ(restored->size(), 7u);
            ASSERT_EQ(std::memcmp(restored->getBuffer(), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterStringAllocationKeepsObjectSetStringReadable )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto object_class = getTestClass(fixture);
            object_class->addField("value", 0);

            PosVT::Data object_data;
            object_data.m_types = { StorageClass::INT64 };
            object_data.m_values = { Value(1) };
            Object object(fixture, object_class, std::make_pair(0u, 0u), object_data, 0);
            object.incRef(false);
            {
                db0::FixtureLock lock(fixture);
                auto initial = Py_OWN(PyUnicode_FromString("initial"));
                ASSERT_TRUE(initial.get());
                object.set(lock, "value", db0::bindings::TypeId::STRING, initial.get());
            }
            auto initial_address = findMemberAddress(object, StorageClass::STRING_REF);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            auto committed = Py_OWN(object.get("value").steal());
            ASSERT_TRUE(committed.get());
            ASSERT_STREQ(PyUnicode_AsUTF8(committed.get()), "initial");
            fixture->detach();
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            auto committed_after_detach = Py_OWN(object.get("value").steal());
            ASSERT_TRUE(committed_after_detach.get());
            ASSERT_STREQ(PyUnicode_AsUTF8(committed_after_detach.get()), "initial");

            fixture->beginAtomic(nullptr);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            {
                auto atomic_address = makeString(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));

            // Object-level regression for the original atomic rollback failure:
            // canceling a later allocation must not hide the committed member
            // allocation behind an older cached prefix lock.
            auto restored = Py_OWN(object.get("value").steal());
            ASSERT_TRUE(restored.get());
            ASSERT_TRUE(PyUnicode_Check(restored.get()));
            ASSERT_STREQ(PyUnicode_AsUTF8(restored.get()), "initial");
        }
        ASSERT_NO_THROW(workspace.close());
    }

    TEST_F( ObjectTest , testAtomicCancelAfterBinaryAllocationKeepsObjectSetBytesReadable )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        {
            auto fixture = workspace.getFixture(prefix_name);
            auto object_class = getTestClass(fixture);
            object_class->addField("value", 0);

            PosVT::Data object_data;
            object_data.m_types = { StorageClass::INT64 };
            object_data.m_values = { Value(1) };
            Object object(fixture, object_class, std::make_pair(0u, 0u), object_data, 0);
            object.incRef(false);
            {
                db0::FixtureLock lock(fixture);
                auto initial = Py_OWN(PyBytes_FromStringAndSize("initial", 7));
                ASSERT_TRUE(initial.get());
                object.set(lock, "value", db0::bindings::TypeId::BYTES, initial.get());
            }
            auto initial_address = findMemberAddress(object, StorageClass::DB0_BYTES);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            ASSERT_TRUE(fixture->commit());
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            auto committed = Py_OWN(object.get("value").steal());
            ASSERT_TRUE(committed.get());
            ASSERT_TRUE(PyBytes_Check(committed.get()));
            ASSERT_EQ(PyBytes_GET_SIZE(committed.get()), 7);
            ASSERT_EQ(std::memcmp(PyBytes_AsString(committed.get()), "initial", 7), 0);

            fixture->beginAtomic(nullptr);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            {
                auto atomic_address = makeBinary(fixture, "outer");
                ASSERT_TRUE(!!atomic_address);
                ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));
            }
            fixture->cancelAtomic(nullptr);
            ASSERT_TRUE(fixture->isAddressValid(initial_address, 0));

            auto restored = Py_OWN(object.get("value").steal());
            ASSERT_TRUE(restored.get());
            ASSERT_TRUE(PyBytes_Check(restored.get()));
            ASSERT_EQ(PyBytes_GET_SIZE(restored.get()), 7);
            ASSERT_EQ(std::memcmp(PyBytes_AsString(restored.get()), "initial", 7), 0);
        }
        ASSERT_NO_THROW(workspace.close());
    }
        
}
