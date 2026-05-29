// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
        
}
