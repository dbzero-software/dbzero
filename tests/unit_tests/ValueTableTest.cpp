// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/ValueTable.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{
    
    class ValueTableTest: public testing::Test
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
    
    TEST_F( ValueTableTest , testMicroArrayMeasure )
    {
        std::vector<StorageClass> values = { StorageClass::INT64, StorageClass::POOLED_STRING };
        ASSERT_EQ (0, db0::true_size_of<o_micro_array<StorageClass> >() );
        ASSERT_EQ ( 1u, sizeof(StorageClass) );
        ASSERT_EQ ( 3u, o_micro_array<StorageClass>::measure(values) );
    }
    
    TEST_F( ValueTableTest , testMicroArrayWithOffsetMeasure )
    {
        std::vector<StorageClass> values = { StorageClass::INT64, StorageClass::POOLED_STRING };
        ASSERT_EQ ( 4u, (o_micro_array<StorageClass, true>::measure(values)) );
    }
    
    TEST_F( ValueTableTest , testPosVTMeasure )
    {
        PosVT::Data data;
        data.m_types = std::vector<StorageClass> { StorageClass::INT64, StorageClass::POOLED_STRING };
        data.m_values = std::vector<Value> { Value(0), Value(0) };

        ASSERT_EQ ( 22u, PosVT::measure(data, 0) );
    }
    
    TEST_F( ValueTableTest , testMicroArrayCanBeCreatedWithValues )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<StorageClass> values = { StorageClass::INT64, StorageClass::POOLED_STRING };
        using UA_Object = v_object<o_micro_array<StorageClass> >;
        UA_Object cut(*fixture, values);
        EXPECT_EQ(2, cut->size());
        EXPECT_EQ(StorageClass::INT64, cut.const_ref()[0]);
        EXPECT_EQ(StorageClass::POOLED_STRING, cut.const_ref()[1]);
        workspace.close();
    }
    
    TEST_F( ValueTableTest , testPosVTCanBeCreatedWithValues )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        PosVT::Data data;
        data.m_types = std::vector<StorageClass> { StorageClass::INT64, StorageClass::POOLED_STRING };
        data.m_values = std::vector<Value> { Value(0), Value(0) };

        using PosVTObject = v_object<PosVT>;
        PosVTObject cut(*fixture, data, 0);
        EXPECT_EQ(2, cut->size());
        EXPECT_EQ(StorageClass::INT64, cut->types()[0]);
        EXPECT_EQ(StorageClass::POOLED_STRING, cut->types()[1]);
        workspace.close();
    }

    TEST_F( ValueTableTest , testPosVTItemsCanBeUpdatesPostCreate )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        PosVT::Data data;
        data.m_types = std::vector<StorageClass> { StorageClass::INT64, StorageClass::POOLED_STRING };
        data.m_values = std::vector<Value> { Value(0), Value(0) };

        using PosVTObject = v_object<PosVT>;
        PosVTObject cut(*fixture, data, 0);
        cut.modify().types()[0] = StorageClass::POOLED_STRING;
        cut.modify().values()[0] = Value(1);
        
        EXPECT_EQ(StorageClass::POOLED_STRING, cut->types()[0]);
        EXPECT_EQ(Value(1), cut->values()[0]);
        workspace.close();
    }

    TEST_F( ValueTableTest , testPosVTMemberOffsets )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        PosVT::Data data;
        data.m_types = std::vector<StorageClass> { StorageClass::INT64, StorageClass::POOLED_STRING };
        data.m_values = std::vector<Value> { Value(0), Value(0) };
        
        using PosVTObject = v_object<PosVT>;
        PosVTObject cut(*fixture, data, 0);
        auto size_of = cut->sizeOf();
        ASSERT_EQ(22u, size_of);
        ASSERT_EQ(2, (char*)&cut->types() - (char*)cut.getData());
        auto offset_values = (char*)&cut->values() - (char*)cut.getData();
        ASSERT_EQ((char*)&cut->values(), (char*)cut->values().begin());
        ASSERT_EQ(6u, offset_values);
        ASSERT_EQ(2u, cut->size());
        
        workspace.close();
    }
    
} 