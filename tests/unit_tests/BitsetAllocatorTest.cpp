// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/BitsetAllocator.hpp>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/memory/Address.hpp>

using namespace std;

namespace tests

{

    using Address = db0::Address;

    class BitsetAllocatorTests: public testing::Test
    {
    public:
        virtual void SetUp() override {
        }

        virtual void TearDown() override {

        }

    protected:
        db0::TestWorkspace m_workspace;
        static constexpr std::size_t PAGE_SIZE = 4096;

        using AllocatorT = db0::BitsetAllocator<db0::VFixedBitset<123> >;

        AllocatorT makeAllocator(Address base_addr, int direction = 1) {
            auto memspace = m_workspace.getMemspace("my-test-prefix_1");
            return AllocatorT(db0::VFixedBitset<123>(memspace), base_addr, PAGE_SIZE, direction);
        }

        void assertFindsAllocation(AllocatorT &allocator, Address query, Address expectedAddress) {
            auto result = allocator.findAllocation(query);
            ASSERT_TRUE(result);
            ASSERT_EQ(result->address, expectedAddress);
            ASSERT_EQ(result->size, PAGE_SIZE);
        }
    };

    TEST_F( BitsetAllocatorTests , testAllocAssignsValidAddresses )
    {
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        auto base_addr = Address::fromOffset(0);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, 1);
        auto ptr1 = cut.alloc(page_size);
        auto ptr2 = cut.alloc(page_size);
        ASSERT_NE(ptr1, ptr2);
    }

    TEST_F( BitsetAllocatorTests , testAllocThenFree )
    {
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        auto base_addr = Address::fromOffset(0);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, 1);
        auto ptr1 = cut.alloc(page_size);
        auto ptr2 = cut.alloc(page_size);
        cut.free(ptr2);
        // throws on double free
        ASSERT_ANY_THROW(cut.free(ptr2));
        // throws on get alloc size of the freed range
        ASSERT_ANY_THROW(cut.getAllocSize(ptr2));
        // allocates the same range again
        auto ptr3 = cut.alloc(page_size);
        ASSERT_EQ(ptr2, ptr3);
        // get alloc size works for allocated ranges
        ASSERT_EQ(page_size, cut.getAllocSize(ptr1));
        ASSERT_EQ(page_size, cut.getAllocSize(ptr3));
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorCanHandleSubAddresses )
    {
        // this functionality allows allocating some range and then requesting a subrange of it
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        auto base_addr = Address::fromOffset(0);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, 1);
        auto ptr1 = cut.alloc(page_size);
        std::size_t offset = 128;
        ASSERT_EQ(cut.getAllocSize(ptr1 + offset), page_size - offset);
        auto result = cut.findAllocation(ptr1 + offset);
        ASSERT_TRUE(result);
        ASSERT_EQ(result->address, ptr1);
        ASSERT_EQ(result->size, page_size);
        ASSERT_THROW(cut.findAllocation(ptr1 + page_size), db0::BadAddressException);
        ASSERT_THROW(cut.findAllocation(Address::fromOffset(page_size * 123)), db0::BadAddressException);
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorCanAllocateInNegativeDirection )
    {
        // this functionality allows allocating some range and then requesting a subrange of it
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        auto base_addr = Address::fromOffset(page_size * 1024);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, -1);
        auto ptr1 = cut.alloc(page_size);
        ASSERT_EQ(ptr1.getOffset(), page_size * 1024 - page_size);
        auto ptr2 = cut.alloc(page_size);
        ASSERT_TRUE(ptr2 < ptr1);
        auto result = cut.findAllocation(ptr1 + static_cast<Address::offset_t>(123));
        ASSERT_TRUE(result);
        ASSERT_EQ(result->address, ptr1);
        ASSERT_EQ(result->size, page_size);
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorFindAllocationExactAddress )
    {
        auto cut = makeAllocator(Address::fromOffset(0));
        auto address = cut.alloc(PAGE_SIZE);
        assertFindsAllocation(cut, address, address);
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorFindAllocationLastByte )
    {
        auto cut = makeAllocator(Address::fromOffset(0));
        auto address = cut.alloc(PAGE_SIZE);
        assertFindsAllocation(cut, address + static_cast<Address::offset_t>(PAGE_SIZE - 1), address);
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorFindAllocationRejectsFreedBlock )
    {
        auto cut = makeAllocator(Address::fromOffset(0));
        auto first = cut.alloc(PAGE_SIZE);
        auto second = cut.alloc(PAGE_SIZE);
        cut.free(second);
        ASSERT_THROW(cut.findAllocation(second), db0::BadAddressException);
        assertFindsAllocation(cut, first, first);
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorFindAllocationRejectsAddressBeyondBitset )
    {
        auto cut = makeAllocator(Address::fromOffset(0));
        cut.alloc(PAGE_SIZE);
        ASSERT_THROW(cut.findAllocation(Address::fromOffset(PAGE_SIZE * 123)), db0::BadAddressException);
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorFindAllocationRejectsAddressBeforeForwardBase )
    {
        auto cut = makeAllocator(Address::fromOffset(PAGE_SIZE));
        cut.alloc(PAGE_SIZE);
        ASSERT_THROW(cut.findAllocation(Address::fromOffset(0)), db0::BadAddressException);
    }

    TEST_F( BitsetAllocatorTests , testBitsetAllocatorFindAllocationRejectsReverseBaseBoundary )
    {
        auto base_addr = Address::fromOffset(PAGE_SIZE * 1024);
        auto cut = makeAllocator(base_addr, -1);
        auto address = cut.alloc(PAGE_SIZE);
        assertFindsAllocation(cut, address + static_cast<Address::offset_t>(PAGE_SIZE - 1), address);
        ASSERT_THROW(cut.findAllocation(base_addr), db0::BadAddressException);
    }

}
