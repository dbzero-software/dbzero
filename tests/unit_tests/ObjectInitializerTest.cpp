#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <utils/SubClass.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

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
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(mock_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, mock_class);
        auto &cut = *manager.findInitializer(*object_1);
        // fill rate = 3 / 4
        cut.set({0, 0}, StorageClass::INT64, Value(0));
        cut.set({1, 0}, StorageClass::POOLED_STRING, Value(0));
        cut.set({3, 0}, StorageClass::INT64, Value(0));
        
        PosVT::Data pos_vt_data;
        unsigned int pos_vt_offset = 0;
        cut.getData(pos_vt_data, pos_vt_offset);
        ASSERT_EQ(pos_vt_offset, 0);
        // NOTE: there should be 4 elements in pos-vt, but only 3 are filled
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        ASSERT_EQ(pos_vt_data.m_types[0], StorageClass::INT64);
        ASSERT_EQ(pos_vt_data.m_types[1], StorageClass::POOLED_STRING);
        ASSERT_EQ(pos_vt_data.m_types[2], StorageClass::UNDEFINED);
        ASSERT_EQ(pos_vt_data.m_types[3], StorageClass::INT64);
        
        object_1->~Object();
        workspace.close();
    }
    
    TEST_F( ObjectInitializerTest, testReducedPosVT )
    {    
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(mock_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, mock_class);
        auto &cut = *manager.findInitializer(*object_1);
        // NOTE: only the first 4 elements should be selected to pos-vt
        cut.set({0, 0}, StorageClass::INT64, Value(0));
        cut.set({13, 0}, StorageClass::INT64, Value(0));
        cut.set({1, 0}, StorageClass::POOLED_STRING, Value(0));
        cut.set({3, 0}, StorageClass::INT64, Value(0));
        cut.set({12, 0}, StorageClass::INT64, Value(0));
        
        PosVT::Data pos_vt_data;
        unsigned int pos_vt_offset = 0;
        cut.getData(pos_vt_data, pos_vt_offset);
        // NOTE: there should be 4 elements in pos-vt, but only 3 are filled
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        object_1->~Object();
        workspace.close();
    }
    
}