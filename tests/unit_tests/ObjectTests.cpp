#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/CommonBase.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/core/vspace/v_object.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{
    
    // this is to expose protected constructor for tests
    class SubClass: public Class
    {
    public:
        SubClass(db0::swine_ptr<Fixture> &fixture, const std::string &name, std::optional<std::string> module_name,
            const char *type_id, const char *prefix_name, const std::vector<std::string> &init_vars, ClassFlags flags,
            std::shared_ptr<Class> base_class)
            : Class(fixture, name, module_name, type_id, prefix_name, init_vars, flags, base_class)
        {
            // to prevent premature removal
            this->incRef(false);
            this->incRef(false);
        }
    };
    
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
        
        ASSERT_EQ ( 48u, o_object::measure(0, {0, 0}, 0, data) );
    }
    
    TEST_F( ObjectTest , testObjectInitializerCanBeFoundIfAdded )
    {
        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> null_class = Class::getNullClass();
        auto object_1 = new (data.data()) Object(null_class);
        ObjectInitializerManager cut;
        ASSERT_EQ(cut.findInitializer(*object_1), nullptr);
        cut.addInitializer(*object_1, null_class);
        ASSERT_NE(cut.findInitializer(*object_1), nullptr);
        object_1->~Object();
    }
    
    TEST_F( ObjectTest , testObjectCanBeInstantiatedOnBaseWorkspace )
    {        
        BaseWorkspace workspace;
        auto memspace = workspace.getMemspace(prefix_name);
        PosVT::Data data(8);

        using Object = v_object<db0::object_model::o_object>;
        ASSERT_NO_THROW( Object(memspace, 0, std::make_pair(0u, 0u), 0, data) );
        workspace.close();
    }
    
    TEST_F( ObjectTest , testNewObjectSpeed )
    {        
        BaseWorkspace workspace;
        auto memspace = workspace.getMemspace(prefix_name);
        using Object = v_object<db0::object_model::o_object>;
        PosVT::Data data(8);
        std::size_t size_of = db0::object_model::o_object::measure(0, std::make_pair(0u, 0u), 0, data);

        // measure speed
        auto start = std::chrono::high_resolution_clock::now();
        std::size_t total_bytes = 0;
        std::size_t alloc_count = 100000;
        for (unsigned int i = 0; i < alloc_count; ++i) {
            Object(memspace, 0, std::make_pair(0u, 0u), 0, 8);
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
                objects.emplace_back(memspace, 0, std::make_pair(0u, 0u), 0, data);
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
    
    TEST_F( ObjectTest , testObjectCanBeCastToCommonBase )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        
        PosVT::Data data(0);

        using Object = db0::object_model::Object;
        
        // mock type
        std::shared_ptr<Class> type = std::shared_ptr<Class>(new SubClass(
            fixture, "TestObject", std::nullopt, "test.object", "test_prefix", {}, ClassFlags(), nullptr)
        );
        {
            Object object(fixture, type, std::make_pair(0u, 0u), data);
            object.incRef(true);
            object.incRef(false);
            auto &cut = *reinterpret_cast<db0::object_model::CommonBase*>(&object);
            ASSERT_EQ(1u, cut->m_header.m_ref_counter.get().first);
            ASSERT_EQ(1u, cut->m_header.m_ref_counter.get().second);
            ASSERT_TRUE(cut.hasRefs());
            object.decRef(true);
            object.decRef(false);
            ASSERT_EQ(0u, cut->m_header.m_ref_counter.get().first);
            ASSERT_EQ(0u, cut->m_header.m_ref_counter.get().second);
            ASSERT_FALSE(cut.hasRefs());
            // NOTE: incRef preserves the object, otherwise the test would fail on cleanup
            object.incRef(true);
        }
        workspace.close();
    }
    
}