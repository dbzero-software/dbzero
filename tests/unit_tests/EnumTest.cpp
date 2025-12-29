// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/ObjectModel.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{

    class EnumTest: public testing::Test
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
    
    TEST_F( EnumTest , testEnumCanBeCreatedWithValues )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        {
            Enum cut(fixture, "test_enum", "", {"value1", "value2"});
            ASSERT_FALSE(cut.isNull());
        }
        workspace.close();
    }

    TEST_F( EnumTest , testEnumCanFindExistingValues )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        {
            Enum cut(fixture, "color", "", {"red", "green"});
            ASSERT_NO_THROW(cut.find("red"));
            ASSERT_NO_THROW(cut.find("green"));            
        }
        workspace.close();
    }

    TEST_F( EnumTest , testEnumThrowsOnAttemptToFindNonExistingValue )
    {        
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        {
            Enum cut(fixture, "color", "", {"red", "green"});
            ASSERT_ANY_THROW(cut.find("blue"));
        }
        workspace.close();
    }
    
}