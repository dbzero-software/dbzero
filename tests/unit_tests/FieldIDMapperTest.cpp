// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/object_model/class/FieldIDMapper.hpp>
#include <utils/TestBase.hpp>

namespace tests

{

    using namespace db0;
    using namespace db0::object_model;

    class TestFieldIDMapper: public FieldIDMapper
    {
    public:
        using FieldIDMapper::FieldIDMapper;
        using FieldIDMapper::getFieldIDClusterMappingCount;
        using FieldIDMapper::getFieldIDExceptionMappingCount;
        using FieldIDMapper::getFieldIDMappingCount;
        using FieldIDMapper::getNameMappingCount;
        using FieldIDMapper::hasFieldIDClusterMappingCollection;
        using FieldIDMapper::hasFieldIDExceptionMappingCollection;
        using FieldIDMapper::hasNameMappingCollection;
    };

    class FieldIDMapperTest: public MemspaceTestBase
    {
    };

    TEST_F( FieldIDMapperTest , testClusterZeroFieldIDOffsetIsImplicit )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto field_id = FieldID::fromIndex(0, 3);

        ASSERT_FALSE(mapper.hasFieldIDClusterMappingCollection());
        ASSERT_FALSE(mapper.hasFieldIDExceptionMappingCollection());
        ASSERT_FALSE(mapper.hasNameMappingCollection());
        ASSERT_EQ(mapper.assignFieldOffset(field_id), field_id.getOffset());
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(field_id), field_id.getOffset());
        ASSERT_EQ(mapper.getFieldIDMappingCount(), 0u);
        ASSERT_EQ(mapper.getFieldIDClusterMappingCount(), 0u);
        ASSERT_EQ(mapper.getFieldIDExceptionMappingCount(), 0u);
        ASSERT_FALSE(mapper.hasFieldIDClusterMappingCollection());
        ASSERT_FALSE(mapper.hasFieldIDExceptionMappingCollection());
        ASSERT_FALSE(mapper.hasNameMappingCollection());
    }

    TEST_F( FieldIDMapperTest , testInitialFieldIDsInClusterZeroDoNotCreatePersistentCollections )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);

        for (std::uint32_t i = 0; i < FieldIDMapper::CLUSTER_SIZE; ++i) {
            auto field_id = FieldID::fromIndex(0, i);
            ASSERT_EQ(mapper.assignFieldOffset(field_id), i);
            ASSERT_EQ(mapper.tryGetAssignedFieldOffset(field_id), i);
        }

        ASSERT_EQ(mapper.getFieldIDMappingCount(), 0u);
        ASSERT_EQ(mapper.getNameMappingCount(), 0u);
        ASSERT_FALSE(mapper.hasFieldIDClusterMappingCollection());
        ASSERT_FALSE(mapper.hasFieldIDExceptionMappingCollection());
        ASSERT_FALSE(mapper.hasNameMappingCollection());
    }

    TEST_F( FieldIDMapperTest , testLargeFieldIDOffsetIsPersistedCompactly )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto field_id = FieldID::fromIndex(12, 3);
        auto sibling_field_id = FieldID::fromIndex(12, 4);

        auto offset = mapper.assignFieldOffset(field_id);

        ASSERT_NE(offset, field_id.getLongIndex());
        ASSERT_EQ(offset, FieldIDMapper::CLUSTER_SIZE + field_id.getOffset());
        ASSERT_EQ(mapper.assignFieldOffset(sibling_field_id), offset + 1);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(field_id), offset);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(sibling_field_id), offset + 1);
        ASSERT_EQ(mapper.getFieldIDMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDClusterMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDExceptionMappingCount(), 0u);
    }

    TEST_F( FieldIDMapperTest , testNamedFieldOffsetIsStable )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);

        auto offset = mapper.assignFieldOffset("status");

        ASSERT_EQ(mapper.assignFieldOffset("status"), offset);
        ASSERT_EQ(offset, FieldIDMapper::CLUSTER_SIZE);
        ASSERT_EQ(mapper.getNameMappingCount(), 1u);
    }

    TEST_F( FieldIDMapperTest , testRenameFieldMovesExistingNameMapping )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);

        auto offset = mapper.assignFieldOffset("status");

        ASSERT_TRUE(mapper.renameField("status", "state"));
        ASSERT_EQ(mapper.assignFieldOffset("state"), offset);
        ASSERT_EQ(mapper.assignFieldOffset("status"), offset + 1);
        ASSERT_EQ(mapper.getNameMappingCount(), 2u);
    }

    TEST_F( FieldIDMapperTest , testRenameFieldDoesNothingWhenOldNameIsMissing )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);

        ASSERT_FALSE(mapper.renameField("status", "state"));

        ASSERT_EQ(mapper.getNameMappingCount(), 0u);
        ASSERT_FALSE(mapper.hasNameMappingCollection());
    }

    TEST_F( FieldIDMapperTest , testRenameFieldDoesNotOverwriteExistingNameMapping )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);

        auto status_offset = mapper.assignFieldOffset("status");
        auto state_offset = mapper.assignFieldOffset("state");

        ASSERT_FALSE(mapper.renameField("status", "state"));
        ASSERT_EQ(mapper.assignFieldOffset("status"), status_offset);
        ASSERT_EQ(mapper.assignFieldOffset("state"), state_offset);
        ASSERT_EQ(mapper.getNameMappingCount(), 2u);
    }

    TEST_F( FieldIDMapperTest , testNameFirstAssignmentStillUsesImplicitClusterZero )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto name_offset = mapper.assignFieldOffset("status");
        auto small_field_id = FieldID::fromIndex(0, 3);

        auto field_offset = mapper.assignFieldOffset(small_field_id);

        ASSERT_EQ(name_offset, FieldIDMapper::CLUSTER_SIZE);
        ASSERT_EQ(field_offset, small_field_id.getOffset());
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(small_field_id), field_offset);
        ASSERT_EQ(mapper.getFieldIDClusterMappingCount(), 0u);
        ASSERT_EQ(mapper.getFieldIDExceptionMappingCount(), 0u);
    }

    TEST_F( FieldIDMapperTest , testImplicitClusterZeroFieldIDFirstAssignmentReservesClusterZeroRange )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto small_field_id = FieldID::fromIndex(0, 3);

        auto field_offset = mapper.assignFieldOffset(small_field_id);
        auto name_offset = mapper.assignFieldOffset("status");

        ASSERT_EQ(field_offset, small_field_id.getOffset());
        ASSERT_EQ(name_offset, FieldIDMapper::CLUSTER_SIZE);
        ASSERT_EQ(mapper.getFieldIDMappingCount(), 0u);
        ASSERT_EQ(mapper.getNameMappingCount(), 1u);
    }

    TEST_F( FieldIDMapperTest , testDirectFieldIDAssignmentAfterNameMappingIsPersistedForNonZeroCluster )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto name_offset = mapper.assignFieldOffset("status");
        auto field_id = FieldID::fromIndex(1, 0);

        auto field_offset = mapper.assignFieldOffset(field_id);

        ASSERT_NE(field_offset, name_offset);
        ASSERT_EQ(mapper.assignFieldOffset(field_id), field_offset);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(field_id), field_offset);
        ASSERT_EQ(mapper.getFieldIDMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDClusterMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDExceptionMappingCount(), 0u);
    }

    TEST_F( FieldIDMapperTest , testNamedMappingCanBeMigratedToFieldIDMapping )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto name_offset = mapper.assignFieldOffset("status");
        auto field_id = FieldID::fromIndex(100, 0);

        ASSERT_EQ(mapper.onFieldIDAssigned("status", field_id), name_offset);

        ASSERT_EQ(mapper.assignFieldOffset(field_id), name_offset);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(field_id), name_offset);
        ASSERT_EQ(mapper.getNameMappingCount(), 0u);
        ASSERT_EQ(mapper.getFieldIDMappingCount(), 1u);
    }

    TEST_F( FieldIDMapperTest , testNamedMappingMigrationCreatesExceptionMappingForSingleFieldID )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto other_name_offset = mapper.assignFieldOffset("type");
        auto name_offset = mapper.assignFieldOffset("status");
        auto field_id = FieldID::fromIndex(100, 3);
        auto sibling_field_id = FieldID::fromIndex(100, 4);

        ASSERT_EQ(mapper.onFieldIDAssigned("status", field_id), name_offset);

        ASSERT_EQ(mapper.assignFieldOffset(field_id), name_offset);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(field_id), name_offset);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(sibling_field_id), std::nullopt);
        ASSERT_EQ(mapper.getNameMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDClusterMappingCount(), 0u);
        ASSERT_EQ(mapper.getFieldIDExceptionMappingCount(), 1u);
        ASSERT_EQ(other_name_offset, FieldIDMapper::CLUSTER_SIZE);
    }

    TEST_F( FieldIDMapperTest , testDirectFieldIDAssignmentCreatesClusterMappingAfterExceptionMapping )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto name_offset = mapper.assignFieldOffset("status");
        auto exception_field_id = FieldID::fromIndex(100, 3);
        auto sibling_field_id = FieldID::fromIndex(100, 4);

        ASSERT_EQ(mapper.onFieldIDAssigned("status", exception_field_id), name_offset);
        auto sibling_offset = mapper.assignFieldOffset(sibling_field_id);

        ASSERT_EQ(mapper.assignFieldOffset(exception_field_id), name_offset);
        ASSERT_NE(sibling_offset, name_offset + 1);
        ASSERT_EQ(mapper.assignFieldOffset(sibling_field_id), sibling_offset);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(sibling_field_id), sibling_offset);
        ASSERT_EQ(mapper.getFieldIDMappingCount(), 2u);
        ASSERT_EQ(mapper.getFieldIDClusterMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDExceptionMappingCount(), 1u);
    }

    TEST_F( FieldIDMapperTest , testClusterAndNameAllocationsDoNotOverlap )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto first_cluster_field_id = FieldID::fromIndex(100, 3);
        auto first_cluster_sibling_field_id = FieldID::fromIndex(100, 4);
        auto second_cluster_field_id = FieldID::fromIndex(101, 0);

        auto first_cluster_offset = mapper.assignFieldOffset(first_cluster_field_id);
        auto name_offset = mapper.assignFieldOffset("status");
        auto second_cluster_offset = mapper.assignFieldOffset(second_cluster_field_id);

        ASSERT_EQ(mapper.assignFieldOffset(first_cluster_sibling_field_id), first_cluster_offset + 1);
        ASSERT_EQ(name_offset, FieldIDMapper::CLUSTER_SIZE * 2);
        ASSERT_EQ(second_cluster_offset, name_offset + FieldIDMapper::CLUSTER_SIZE);
        ASSERT_EQ(mapper.getNameMappingCount(), 1u);
        ASSERT_EQ(mapper.getFieldIDClusterMappingCount(), 2u);
        ASSERT_EQ(mapper.getFieldIDExceptionMappingCount(), 0u);
    }

    TEST_F( FieldIDMapperTest , testNameOffsetsAreConsecutiveUntilClusterBoundary )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);

        for (std::uint32_t i = 0; i < FieldIDMapper::CLUSTER_SIZE; ++i) {
            auto name = std::string("field_") + std::to_string(i);
            ASSERT_EQ(mapper.assignFieldOffset(name.c_str()), FieldIDMapper::CLUSTER_SIZE + i);
        }
    }

    TEST_F( FieldIDMapperTest , testNameOffsetsSkipReservedClusterRanges )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto field_id = FieldID::fromIndex(100, 0);

        ASSERT_EQ(mapper.assignFieldOffset(field_id), FieldIDMapper::CLUSTER_SIZE);
        ASSERT_EQ(mapper.assignFieldOffset("status"), FieldIDMapper::CLUSTER_SIZE * 2);
        ASSERT_EQ(mapper.assignFieldOffset("type"), FieldIDMapper::CLUSTER_SIZE * 2 + 1);
    }

    TEST_F( FieldIDMapperTest , testNameOffsetsRemainConsecutiveWhenClusterAllocatedBeforeBoundary )
    {
        auto memspace = getMemspace();
        TestFieldIDMapper mapper(memspace);
        auto field_id = FieldID::fromIndex(100, 0);

        ASSERT_EQ(mapper.assignFieldOffset("status"), FieldIDMapper::CLUSTER_SIZE);
        ASSERT_EQ(mapper.assignFieldOffset(field_id), FieldIDMapper::CLUSTER_SIZE * 2);
        ASSERT_EQ(mapper.assignFieldOffset("type"), FieldIDMapper::CLUSTER_SIZE + 1);
    }

    TEST_F( FieldIDMapperTest , testMapperPersistsMappings )
    {
        auto memspace = getMemspace();
        Address address;
        std::uint32_t name_offset;
        std::uint32_t field_offset;
        auto field_id = FieldID::fromIndex(100, 0);

        {
            TestFieldIDMapper mapper(memspace);
            name_offset = mapper.assignFieldOffset("status");
            field_offset = mapper.onFieldIDAssigned("status", field_id);
            address = mapper.getAddress();
        }

        TestFieldIDMapper mapper(memspace.myPtr(address));

        ASSERT_EQ(mapper.assignFieldOffset(field_id), field_offset);
        ASSERT_EQ(mapper.tryGetAssignedFieldOffset(field_id), field_offset);
        ASSERT_EQ(field_offset, name_offset);
    }

}
