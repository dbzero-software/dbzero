#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/workspace/Fixture.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;

namespace tests

{
    
    class ObjectInitializerTest: public testing::Test
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

    TEST_F( ObjectInitializerTest, testIncompletePosVT )
    {    
        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> null_class = Class::getNullClass();
        auto object_1 = Object::makeNew(data.data(), null_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, null_class);
        auto &cut = *manager.findInitializer(*object_1);
        // fill rate = 3 / 4
        cut.set(0, StorageClass::INT64, Value(0));
        cut.set(1, StorageClass::POOLED_STRING, Value(0));
        cut.set(3, StorageClass::INT64, Value(0));

        PosVT::Data pos_vt_data;
        cut.getData(pos_vt_data);
        // NOTE: there should be 4 elements in pos-vt, but only 3 are filled
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        ASSERT_EQ(pos_vt_data.m_types[0], StorageClass::INT64);
        ASSERT_EQ(pos_vt_data.m_types[1], StorageClass::POOLED_STRING);
        ASSERT_EQ(pos_vt_data.m_types[2], StorageClass::UNDEFINED);
        ASSERT_EQ(pos_vt_data.m_types[3], StorageClass::INT64);
        
        object_1->~Object();
    }

    TEST_F( ObjectInitializerTest, testReducedPosVT )
    {    
        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> null_class = Class::getNullClass();
        auto object_1 = Object::makeNew(data.data(), null_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, null_class);
        auto &cut = *manager.findInitializer(*object_1);
        // NOTE: only the first 4 elements should be selected to pos-vt
        cut.set(0, StorageClass::INT64, Value(0));
        cut.set(13, StorageClass::INT64, Value(0));
        cut.set(1, StorageClass::POOLED_STRING, Value(0));
        cut.set(3, StorageClass::INT64, Value(0));
        cut.set(12, StorageClass::INT64, Value(0));

        PosVT::Data pos_vt_data;
        cut.getData(pos_vt_data);
        // NOTE: there should be 4 elements in pos-vt, but only 3 are filled
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        object_1->~Object();
    }
    
}