#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/vspace/v_object.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{
    
    class ObjectTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override {
            drop(file_name);
        }

        virtual void TearDown() override {            
            drop(file_name);
        }
    };

    TEST_F( ObjectTest , testObjectMeasure )
    {
        PosVT::Data data;
        data.m_types = std::vector<StorageClass> { StorageClass::INT64, StorageClass::POOLED_STRING };
        data.m_values = std::vector<Value> { Value(0), Value(0) };

        ASSERT_EQ ( 49u, o_object::measure(0, 0, data) );
    }
    
    TEST_F( ObjectTest , testObjectInitializerCanBeFoundIfAdded )
    {
        std::vector<char> data(sizeof(Object));
        auto object_1 = Object::makeNew(data.data(), nullptr);
        ObjectInitializerManager cut;
        ASSERT_EQ(cut.findInitializer(*object_1), nullptr);
        cut.addInitializer(*object_1, nullptr);
        ASSERT_NE(cut.findInitializer(*object_1), nullptr);
        object_1->~Object();
    }
    
    TEST_F( ObjectTest , testObjectCanBeInstantiatedOnBaseWorkspace )
    {
        BaseWorkspace workspace;
        auto memspace = workspace.getMemspace(prefix_name);
        PosVT::Data data(8);

        using Object = v_object<db0::object_model::o_object>;
        ASSERT_NO_THROW( Object(memspace, 0, 0, data) );
        workspace.close();
    }

    TEST_F( ObjectTest , testNewObjectSpeed )
    {
        BaseWorkspace workspace;
        auto memspace = workspace.getMemspace(prefix_name);
        using Object = v_object<db0::object_model::o_object>;
        PosVT::Data data(8);
        std::size_t size_of = db0::object_model::o_object::measure(0, 0, data);

        // measure speed
        auto start = std::chrono::high_resolution_clock::now();
        std::size_t total_bytes = 0;
        std::size_t alloc_count = 100000;
        for (unsigned int i = 0; i < alloc_count; ++i) {
            Object(memspace, 0, 0, 8);
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
                objects.emplace_back(memspace, 0, 0, data);
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

}