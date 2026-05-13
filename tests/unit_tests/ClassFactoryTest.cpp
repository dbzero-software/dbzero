// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <mutex>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/class.hpp>
#include <utils/SubClass.hpp>

using namespace std;
using namespace db0;
using namespace db0::object_model;
using namespace db0::tests;
    
namespace tests

{
    
    class ClassFactoryTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override
        {            
            m_workspace.close();
            drop(file_name);
        }
        
    protected:
        Workspace m_workspace;        
    };
    
    TEST_F( ClassFactoryTest , testClassFactoryCanBeCreated )
    {        
        auto fixture = m_workspace.getFixture("my-test-prefix_1");
        ASSERT_NO_THROW( { auto cut = ClassFactory(fixture); } );
        m_workspace.close();
    }

    TEST_F( ClassFactoryTest , testProtectFieldsCreatesFieldSafe )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture("my-test-prefix_1");
        auto type = std::shared_ptr<SubClass>(new SubClass(
            fixture, "ProtectedObject", std::nullopt, "protected.object", "test_prefix", {}, ClassFlags(), nullptr)
        );

        ASSERT_FALSE(type->hasFieldSafe());
        type->setProtectFields();

        ASSERT_TRUE(type->isProtectFields());
        ASSERT_TRUE(type->hasFieldSafe());
        auto offset = type->getFieldSafe().getFieldIDMapper().assignFieldOffset("status");
        ASSERT_EQ(type->getFieldSafe().getFieldIDMapper().assignFieldOffset("status"), offset);
        workspace.close();
    }

    TEST_F( ClassFactoryTest , testProtectFieldsOnUnsupportedClassVersionRaises )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture("my-test-prefix_1");
        auto type = std::shared_ptr<SubClass>(new SubClass(
            fixture, "LegacyObject", std::nullopt, "legacy.object", "test_prefix", {}, ClassFlags(), nullptr)
        );
        type->forceObjVersionForTest(0);

        ASSERT_THROW(type->setProtectFields(), db0::InputException);
        ASSERT_FALSE(type->isProtectFields());
        ASSERT_FALSE(type->hasFieldSafe());
        workspace.close();
    }

}
