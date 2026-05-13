// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/object_model/class/FieldMask.hpp>
#include <utils/TestBase.hpp>

namespace tests

{

    using namespace db0::object_model;

    class FieldMaskTest: public MemspaceTestBase
    {
    };

    TEST_F( FieldMaskTest , testMaskCanBeAssignedAndRetrievedByFieldOffset )
    {
        auto memspace = getMemspace();
        FieldMask mask(memspace);

        mask.setMask(42, { FieldMaskOptions::CREATE, FieldMaskOptions::READ });

        auto result = mask.getMask(42);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE((*result)[FieldMaskOptions::CREATE]);
        ASSERT_TRUE((*result)[FieldMaskOptions::READ]);
        ASSERT_FALSE((*result)[FieldMaskOptions::UPDATE]);
        ASSERT_FALSE((*result)[FieldMaskOptions::DELETE]);
    }

    TEST_F( FieldMaskTest , testMissingOffsetReturnsDefaultMask )
    {
        auto memspace = getMemspace();
        FieldMask mask(memspace);

        mask.setMask(7, { FieldMaskOptions::UPDATE });

        auto before = mask.getMask(6);
        auto after = mask.getMask(8);

        ASSERT_TRUE(before.has_value());
        ASSERT_TRUE(before->none());
        ASSERT_TRUE(after.has_value());
        ASSERT_TRUE(after->none());
    }

    TEST_F( FieldMaskTest , testAssignedEmptyMaskReturnsDefaultMask )
    {
        auto memspace = getMemspace();
        FieldMask mask(memspace);

        mask.setMask(3, {});

        auto result = mask.getMask(3);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result->none());
    }

    TEST_F( FieldMaskTest , testTwoMasksAreStoredInOneByteSlot )
    {
        auto memspace = getMemspace();
        FieldMask mask(memspace);

        mask.setMask(0, { FieldMaskOptions::CREATE });
        mask.setMask(1, { FieldMaskOptions::DELETE });

        ASSERT_EQ(mask.size(), 1u);
        ASSERT_EQ(mask.getItem(0), 0x81);
        ASSERT_TRUE((*mask.getMask(0))[FieldMaskOptions::CREATE]);
        ASSERT_TRUE((*mask.getMask(1))[FieldMaskOptions::DELETE]);
    }

    TEST_F( FieldMaskTest , testMaskCanBeOverwritten )
    {
        auto memspace = getMemspace();
        FieldMask mask(memspace);

        mask.setMask(5, { FieldMaskOptions::CREATE });
        mask.setMask(5, { FieldMaskOptions::DELETE });

        auto result = mask.getMask(5);
        ASSERT_TRUE(result.has_value());
        ASSERT_FALSE((*result)[FieldMaskOptions::CREATE]);
        ASSERT_TRUE((*result)[FieldMaskOptions::DELETE]);
    }

    TEST_F( FieldMaskTest , testMaskCanBeReadFromReopenedInstance )
    {
        auto memspace = getMemspace();
        FieldMask mask(memspace);
        auto address = mask.getAddress();

        mask.setMask(1024, { FieldMaskOptions::READ, FieldMaskOptions::UPDATE });

        FieldMask reopened(memspace.myPtr(address));
        auto result = reopened.getMask(1024);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE((*result)[FieldMaskOptions::READ]);
        ASSERT_TRUE((*result)[FieldMaskOptions::UPDATE]);
    }

}
