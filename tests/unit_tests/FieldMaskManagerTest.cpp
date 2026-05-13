// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/object_model/class/FieldMask.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <utils/utils.hpp>

namespace tests

{

    using namespace db0::object_model;

    class FieldMaskManagerTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "field-mask-manager-test";
        static constexpr const char *file_name = "field-mask-manager-test.db0";

        void SetUp() override {
            db0::tests::drop(file_name);
        }

        void TearDown() override {
            db0::tests::drop(file_name);
        }
    };

    TEST_F( FieldMaskManagerTest , testCreateFieldMaskCreatesAndRetrievesByAccountID )
    {
        db0::Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        FieldMaskManager manager(*fixture, fixture->getVObjectCache());

        auto mask = manager.createFieldMask(123);
        mask->setMask(2, { FieldMaskOptions::READ });

        auto result = manager.tryGetFieldMask(123);
        ASSERT_EQ(result, mask);
        ASSERT_TRUE((*result->getMask(2))[FieldMaskOptions::READ]);
        workspace.close();
    }

    TEST_F( FieldMaskManagerTest , testTryGetFieldMaskReturnsNullptrForMissingAccountID )
    {
        db0::Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        FieldMaskManager manager(*fixture, fixture->getVObjectCache());

        ASSERT_EQ(manager.tryGetFieldMask(999), nullptr);
        workspace.close();
    }

    TEST_F( FieldMaskManagerTest , testCreateFieldMaskReturnsExistingFieldMask )
    {
        db0::Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        FieldMaskManager manager(*fixture, fixture->getVObjectCache());

        auto first = manager.createFieldMask(123);
        auto second = manager.createFieldMask(123);

        ASSERT_EQ(second, first);
        ASSERT_EQ(manager.size(), 1u);
        workspace.close();
    }

    TEST_F( FieldMaskManagerTest , testFieldMaskCanBeRetrievedFromReopenedManager )
    {
        db0::Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        FieldMaskManager manager(*fixture, fixture->getVObjectCache());
        auto manager_address = manager.getAddress();

        auto mask = manager.createFieldMask(123);
        mask->setMask(4, { FieldMaskOptions::CREATE, FieldMaskOptions::UPDATE });

        FieldMaskManager reopened(fixture->myPtr(manager_address), fixture->getVObjectCache());
        auto result = reopened.tryGetFieldMask(123);
        ASSERT_TRUE(result != nullptr);
        ASSERT_TRUE((*result->getMask(4))[FieldMaskOptions::CREATE]);
        ASSERT_TRUE((*result->getMask(4))[FieldMaskOptions::UPDATE]);
        workspace.close();
    }

}
