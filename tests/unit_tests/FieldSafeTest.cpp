// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/object_model/class/FieldSafe.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <utils/utils.hpp>

namespace tests

{

    using namespace db0::object_model;

    class FieldSafeTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "field-safe-test";
        static constexpr const char *file_name = "field-safe-test.db0";

        void SetUp() override {
            db0::tests::drop(file_name);
        }

        void TearDown() override {
            db0::tests::drop(file_name);
        }
    };

    TEST_F( FieldSafeTest , testFieldOffsetsAndMasksShareDurableRoot )
    {
        db0::Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        FieldSafe field_safe(*fixture, fixture->getVObjectCache());
        auto field_safe_address = field_safe.getAddress();

        auto offset = field_safe.getFieldIDMapper().assignFieldOffset("status");
        auto mask = field_safe.getFieldMaskManager().createFieldMask(123);
        mask->setMask(offset, { FieldMaskOptions::READ });

        FieldSafe reopened(fixture->myPtr(field_safe_address), fixture->getVObjectCache());
        auto reopened_offset = reopened.getFieldIDMapper().assignFieldOffset("status");
        auto reopened_mask = reopened.getFieldMaskManager().tryGetFieldMask(123);

        ASSERT_EQ(reopened_offset, offset);
        ASSERT_TRUE(reopened_mask != nullptr);
        ASSERT_TRUE((*reopened_mask->getMask(reopened_offset))[FieldMaskOptions::READ]);
        workspace.close();
    }

    TEST_F( FieldSafeTest , testMaskAssignedByNameFollowsFieldIDAssignment )
    {
        db0::Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        FieldSafe field_safe(*fixture, fixture->getVObjectCache());
        auto field_id = FieldID::fromIndex(100, 3);

        auto name_offset = field_safe.getFieldIDMapper().assignFieldOffset("status");
        field_safe.getFieldMaskManager()
            .createFieldMask(123)
            ->setMask(name_offset, { FieldMaskOptions::CREATE, FieldMaskOptions::UPDATE });

        auto offset = field_safe.getFieldIDMapper().onFieldIDAssigned("status", field_id);
        auto field_offset = field_safe.getFieldIDMapper().assignFieldOffset(field_id);
        auto result = field_safe.getFieldMaskManager().tryGetFieldMask(123)->getMask(field_offset);

        ASSERT_EQ(field_offset, offset);
        ASSERT_TRUE((*result)[FieldMaskOptions::CREATE]);
        ASSERT_TRUE((*result)[FieldMaskOptions::UPDATE]);
        workspace.close();
    }

}
