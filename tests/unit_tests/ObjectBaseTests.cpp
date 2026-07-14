// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <utils/TestBase.hpp>
#include <mutex>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/workspace/Workspace.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;

namespace tests

{

    class ObjectBaseTest: public testing::Test
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
    
    TEST_F( ObjectBaseTest , testObjectBaseDerivedTypesMakeUniqueAlloc )
    {
        // List is derived from ObjectBase
        using List = db0::object_model::List;

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> buf(sizeof(List));
        new (buf.data()) List(fixture);
        auto &list = *reinterpret_cast<List*>(buf.data());
        // make sure the logical (unique) address differs from the physical one
        ASSERT_NE(list.getUniqueAddress().getValue(), list.getAddress().getValue());
        workspace.close();
    }
    
}