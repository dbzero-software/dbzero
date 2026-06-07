// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <algorithm>
#include <random>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <utils/EmbeddedAllocator.hpp>
#include <dbzero/core/memory/SlabAllocator.hpp>
#include <dbzero/core/memory/SlotAllocator.hpp>
#include <dbzero/core/memory/OneShotAllocator.hpp>

using namespace std;

namespace tests

{

    using Address = db0::Address;

    class SlabAllocatorTests: public testing::Test
    {
    public:

        SlabAllocatorTests()
            : m_memspace(m_workspace.getMemspace("test_slab_allocator"))
        {
        }

        virtual void SetUp() override {
        }

        virtual void TearDown() override {
        }

    protected:
        db0::TestWorkspace m_workspace;
        db0::Memspace m_memspace;
        static constexpr std::size_t page_size = 4096;
        static constexpr std::size_t slab_size = 64 * 1024 * 1024;

        db0::SlabAllocator makeSlabAllocator(Address begin_addr, std::size_t size = slab_size) const
        {
            db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, size, page_size);
            return db0::SlabAllocator(m_memspace.getPrefixPtr(), begin_addr, size, page_size);
        }

        void assertFindsAllocation(db0::SlabAllocator &allocator, Address query, Address expected_address,
            std::size_t expected_size) const
        {
            auto result = allocator.findAllocation(query);
            ASSERT_EQ(result.address, expected_address);
            ASSERT_EQ(result.size, expected_size);
        }
    };

    TEST_F( SlabAllocatorTests , testNewlyFormattedSlabAllocatorCanBeOpened )
    {
        // initialize a new slab under the address = 0
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, slab_size, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, slab_size, page_size);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocationsAreTakenFromSlabFront )
    {
        // initialize a new slab under the address = 0
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, slab_size, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, slab_size, page_size);

        auto addr_1 = cut.alloc(100);
        auto addr_2 = cut.alloc(71);
        ASSERT_TRUE (addr_2 > addr_1);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorGetAllocSize )
    {
        // initialize a new slab under the address = 0
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, slab_size, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, slab_size, page_size);

        std::vector<std::size_t> sizes = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 31, 63, 64, 65, 66, 67, 68, 69, 70, 71 };
        std::vector<Address> addresses;

        for (auto size: sizes) {
            addresses.push_back(cut.alloc(size));
        }

        // validate sizes
        for (std::size_t i = 0; i < sizes.size(); ++i) {
            ASSERT_EQ(sizes[i], cut.getAllocSize(addresses[i]));
        }
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorCanFindAllocationByInnerAddress )
    {
        auto begin_addr = Address::fromOffset(0);
        auto cut = makeSlabAllocator(begin_addr);

        auto address = cut.alloc(71);
        assertFindsAllocation(cut, address + static_cast<Address::offset_t>(17), address, 71);
        ASSERT_THROW(cut.findAllocation(address + static_cast<Address::offset_t>(71)), db0::BadAddressException);
        ASSERT_THROW(cut.findAllocation(begin_addr + static_cast<Address::offset_t>(slab_size)), db0::BadAddressException);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorFindAllocationExactAddress )
    {
        auto cut = makeSlabAllocator(Address::fromOffset(0));
        auto address = cut.alloc(71);

        assertFindsAllocation(cut, address, address, 71);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorFindAllocationLastByte )
    {
        auto cut = makeSlabAllocator(Address::fromOffset(0));
        auto address = cut.alloc(71);

        assertFindsAllocation(cut, address + static_cast<Address::offset_t>(70), address, 71);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorFindAllocationRejectsAddressBeforeSlab )
    {
        auto begin_addr = Address::fromOffset(8 * 1024 * 1024);
        auto cut = makeSlabAllocator(begin_addr);

        cut.alloc(71);
        ASSERT_THROW(cut.findAllocation(begin_addr - static_cast<Address::offset_t>(1)), db0::BadAddressException);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorFindAllocationRejectsFreedAllocation )
    {
        auto cut = makeSlabAllocator(Address::fromOffset(0));
        auto address = cut.alloc(71);

        cut.free(address);
        ASSERT_THROW(cut.findAllocation(address + static_cast<Address::offset_t>(17)), db0::BadAddressException);
    }

    TEST_F( SlabAllocatorTests , testSlotAllocatorCanFindGeneralAndSlotAllocations )
    {
        auto main_allocator = std::make_shared<db0::EmbeddedAllocator>();
        db0::SlotAllocator cut(main_allocator);
        auto generalAddress = cut.alloc(64);

        auto slotBegin = Address::fromOffset(8 * 1024 * 1024);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), slotBegin, slab_size, page_size);
        auto slotAllocator = std::make_shared<db0::SlabAllocator>(m_memspace.getPrefixPtr(), slotBegin, slab_size, page_size);
        cut.setSlot(1, slotAllocator);
        auto slotAddress = cut.alloc(80, 1);

        auto general = cut.findAllocation(generalAddress + static_cast<Address::offset_t>(13));
        ASSERT_EQ(general.address, generalAddress);
        ASSERT_EQ(general.size, 64u);

        ASSERT_THROW(cut.findAllocation(slotAddress + static_cast<Address::offset_t>(19)), db0::BadAddressException);

        auto slot = cut.findAllocation(slotAddress + static_cast<Address::offset_t>(19),
            static_cast<db0::Allocator::SlotId>(1));
        ASSERT_EQ(slot.address, slotAddress);
        ASSERT_EQ(slot.size, 80u);
        ASSERT_THROW(cut.findAllocation(slotAddress + static_cast<Address::offset_t>(80),
            static_cast<db0::Allocator::SlotId>(1)), db0::BadAddressException);
        ASSERT_THROW(cut.findAllocation(Address::fromOffset(32 * 1024 * 1024)), db0::BadAddressException);
    }

    TEST_F( SlabAllocatorTests , testOneShotAllocatorCanFindAllocationByInnerAddress )
    {
        auto address = Address::fromOffset(4096);
        db0::OneShotAllocator cut(address, 256);
        ASSERT_EQ(cut.alloc(256), address);

        auto result = cut.findAllocation(address + static_cast<Address::offset_t>(255));
        ASSERT_EQ(result.address, address);
        ASSERT_EQ(result.size, 256u);
        ASSERT_THROW(cut.findAllocation(address + static_cast<Address::offset_t>(256)), db0::BadAddressException);

        cut.free(address);
        ASSERT_THROW(cut.findAllocation(address), db0::BadAddressException);
    }

    TEST_F( SlabAllocatorTests , testCalculateAdminSpaceSize )
    {
        auto calculated_size = db0::SlabAllocator::calculateAdminSpaceSize(page_size);
        // construct acutal SlabAllocator with identical parameters
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, page_size * 64, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, page_size * 64, page_size);

        ASSERT_EQ(cut.getAdminSpaceSize(false), calculated_size);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorCannotBeCreatedIfSizeTooSmall )
    {
        // throws because size of the administrative space exceeds the size of the slab
        auto begin_addr = Address::fromOffset(0);
        ASSERT_ANY_THROW(
            db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, page_size * 3, page_size);
        );
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorCanDynamicallyManageAvailableSize )
    {
        // this test if to check if the available data space is adjusted according to changing administrative space
        // create allocator over the 4-pages slab
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, page_size * 64, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, page_size * 64, page_size);

        std::vector<Address> addresses;
        // make allocations until the entire slab is occupied
        std::uint64_t max_addr = 0;
        int size = 1;
        int max_size = 0;
        std::size_t total_size = 0;
        while (true) {
            auto addr = cut.tryAlloc(size);
            if (!addr)
                break;
            addresses.push_back(*addr);
            if ((*addr).getOffset() > max_addr) {
                max_addr = *addr;
                max_size = size;
            }
            total_size += size;
            ++size;
        }

        // make sure the max address is not conflicting with the admin space
        ASSERT_TRUE(max_addr + max_size + cut.getAdminSpaceSize(false) <= cut.getSlabSize());

        size = 1;
        for (auto &addr: addresses) {
            ASSERT_EQ(size, cut.getAllocSize(addr));
            ++size;
        }
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorAdminOverhead )
    {
        srand(123622u);
        // 4MB slab
        auto size_ = 4 * 1024 * 1024;
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);

        // perform random allocations until full
        for (;;) {
            // random size alloc
            auto addr = cut.tryAlloc(rand() % 1024 + 1);
            if (!addr)
                break;
        }

        // measure the administrative overhead
        auto admin_overhead = cut.getAdminSpaceSize(true) / (double)size_;
        ASSERT_TRUE(admin_overhead < 0.1);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorCanFillAvailableCapacity )
    {
        // 256kb slab
        auto size_ = 256 * 1024;
        auto begin_addr = Address::fromOffset(0);
        auto init_capacity = db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, size_, page_size, init_capacity);

        std::size_t total_allocated = 0;
        // perform 100b allocations until full
        for (;;) {
            auto addr = cut.tryAlloc(100);
            if (!addr)
                break;

            total_allocated += 100;
        }

        ASSERT_TRUE(cut.getRemainingCapacity() < 100);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorEmptyMethod )
    {
        srand(123622u);
        // 4MB slab
        auto size_ = 4 * 1024 * 1024;
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);

        ASSERT_TRUE(cut.empty());
        // perform random allocations until full
        std::vector<Address> addresses;
        for (int i = 0;i < 100; ++i) {
            // random size alloc
            addresses.push_back(cut.alloc(rand() % 1024 + 1));
        }

        ASSERT_FALSE(cut.empty());
        // free all allocations
        for (auto addr: addresses) {
            cut.free(addr);
        }
        ASSERT_TRUE(cut.empty());
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorAllocSpeed )
    {
        auto size_ = 64 * 1024 * 1024;
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);

        // measure speed
        auto start = std::chrono::high_resolution_clock::now();
        std::size_t total_bytes = 0;
        for (int i = 0; i < 100000; ++i) {
            cut.alloc(100);
            total_bytes += 100;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "SlabAllocator alloc speed: " << elapsed.count() << "ms" << std::endl;
        std::cout << "Total bytes: " << total_bytes << std::endl;
        std::cout << "MB / sec : " << (total_bytes / 1024.0 / 1024.0) * 1000.0 / elapsed.count() << std::endl;
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorCanMakeAddressUnique )
    {
        auto size_ = 64 * 1024 * 1024;
        auto begin_addr = Address::fromOffset(0);
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);

        auto addr = cut.alloc(100);
        auto addr1 = cut.tryMakeAddressUnique(addr);
        ASSERT_TRUE(addr1.isValid());
        auto addr2 = cut.tryMakeAddressUnique(addr);
        ASSERT_TRUE(addr2.isValid());
        ASSERT_NE(addr1, addr2);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorDeallocAfterReachingCapacity )
    {
        auto size_ = 64 << 20;
        auto begin_addr = Address::fromOffset(0);
        auto init_capacity = db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), begin_addr, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), begin_addr, size_, page_size, init_capacity, 0);

        std::vector<Address> addresses;
        // perform allocations until full
        for (;;) {
            auto alloc_size = rand() % 512 + 1;
            auto addr = cut.tryAlloc(alloc_size);
            if (!addr)
                break;

            addresses.push_back(*addr);
        }
        std::random_device rd;
        std::mt19937 g(rd());   // random number generator (Mersenne Twister)
        // release in random order
        std::shuffle(addresses.begin(), addresses.end(), g);
        for (auto &addr: addresses) {
            cut.free(addr);
        }

        ASSERT_TRUE(cut.getRemainingCapacity() + cut.getLostCapacity() >= init_capacity);
    }

}
