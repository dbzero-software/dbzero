// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/memory/Recycler.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <utils/TestWorkspace.hpp>

using namespace std;

namespace tests

{

    using namespace db0;
    using SlabRecycler = db0::Recycler<db0::SlabItem>;
    
    // a proxy class to expose protected members for testing
    class MetaAllocatorProxy: public MetaAllocator
    {
    public:
        template <typename... Args>
        MetaAllocatorProxy(Args&&... args)
            : MetaAllocator(std::forward<Args>(args)...)
        {
        }

        std::uint32_t getSlabId(Address address) const {
            return MetaAllocator::getSlabId(address);
        }
    };

    class MetaAllocatorTests: public testing::Test
    {
    public:
        MetaAllocatorTests()
            : m_recycler(2u << 30, m_dirty_meter)
        {
        }
        
        void SetUp() override
        {
            using StorageT = db0::Storage0;
            m_prefix = std::shared_ptr<PrefixImpl>(new PrefixImpl(
                "", m_dirty_meter, m_recycler, std::make_shared<StorageT>(PAGE_SIZE)));
            m_dirty_meter = 0;
            m_recycler.clear();
        }
        
        void TearDown() override 
        {
            m_prefix->close();
            m_prefix = nullptr;
            m_dirty_meter = 0;
            m_recycler.clear();
        }
        
    protected:
        // in bytes
        static constexpr std::size_t PAGE_SIZE = 4096;
        static constexpr std::size_t SLAB_SIZE = 4u << 20;
        static constexpr std::size_t SMALL_SLAB_SIZE = 64 * 4096;
        
        std::atomic<std::size_t> m_dirty_meter = 0;
        CacheRecycler m_recycler;
        std::shared_ptr<Prefix> m_prefix;
    };
    
    TEST_F( MetaAllocatorTests , testAddressPoolFunction )
    {
        auto page_size = 4096;
        auto slab_size = 16 * 4096;
        auto slab_count = MetaAllocator::getSlabCount(page_size, slab_size);
        auto f = MetaAllocator::getAddressPool(38, page_size, slab_size);
        auto num_realms = MetaAllocator::NUM_REALMS;
        std::uint64_t addr = 0;
        std::vector<std::uint64_t> expected_addresses;
        for (unsigned int j = 0; j < 2; ++j) {
            for (unsigned int i = 0; i < num_realms; ++i) {
                addr += page_size;
                expected_addresses.push_back(addr);
                addr += page_size;
                expected_addresses.push_back(addr);            
            }
            addr += slab_size * slab_count;
        }

        for (unsigned int i = 0; i < expected_addresses.size(); ++i) {
            ASSERT_EQ(Address::fromOffset(expected_addresses[i]), f(i));
        }        
    }

    TEST_F( MetaAllocatorTests , testReverseAddressPoolFunction )
    {
        auto page_size = 4096;
        auto slab_size = 16 * 4096;
        auto slab_count = MetaAllocator::getSlabCount(page_size, slab_size);
        auto rf = MetaAllocator::getReverseAddressPool(38, page_size, slab_size);
        auto num_realms = MetaAllocator::NUM_REALMS;
        std::uint64_t addr = 0;
        std::vector<std::uint64_t> expected_addresses;
        for (unsigned int j = 0; j < 2; ++j) {
            for (unsigned int i = 0; i < num_realms; ++i) {
                addr += page_size;
                expected_addresses.push_back(addr);
                addr += page_size;
                expected_addresses.push_back(addr);            
            }
            addr += slab_size * slab_count;
        }
        
        for (unsigned int i = 0; i < expected_addresses.size(); ++i) {
            ASSERT_EQ(rf(Address::fromOffset(expected_addresses[i])), i);
        }        
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorCanBeInitialized )
    { 
        // prepare prefix before first use
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SLAB_SIZE);
        MetaAllocator cut(m_prefix);        
    }

    TEST_F( MetaAllocatorTests , testMetaAllocatorCanAllocateFromNewSlab )
    { 
        // prepare prefix before first use
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SLAB_SIZE);
        MetaAllocator cut(m_prefix);
        
        std::vector<std::size_t> alloc_sizes = { 100, 200, 300, 400, 500, 600, 700, 800, 900 };
        std::uint64_t last_address = 0;
        for (auto alloc_size: alloc_sizes) {
            auto ptr = cut.alloc(alloc_size);            
            ASSERT_TRUE(ptr.getOffset() > last_address);
            last_address = ptr;
        }
        cut.close();
    }

    TEST_F( MetaAllocatorTests , testMetaAllocatorCanAllocateFromExistingSlab )
    {
        // prepare prefix before first use
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SLAB_SIZE);
        
        {
            // first assign from a new slab
            MetaAllocatorProxy cut(m_prefix);
            std::vector<std::size_t> alloc_sizes = { 100, 200, 300, 400, 500, 600, 700, 800, 900 };                
            for (auto alloc_size: alloc_sizes) {
                auto ptr = cut.alloc(alloc_size);
                ASSERT_EQ(cut.getSlabId(ptr), 0);
            }
            cut.close();
        }
        
        // open again and try to allocate
        MetaAllocatorProxy cut(m_prefix);
        auto ptr = cut.alloc(100);
        // the allocation should be in the same slab
        ASSERT_EQ(cut.getSlabId(ptr), 0);    
        cut.close();    
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorCanAllocateFromMultipleExistingSlabs )
    {
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);

        {
            // make allocations until the 2 slabs are occupied
            MetaAllocator cut(m_prefix);
            while (cut.getSlabCount() < 2) {
                cut.alloc(100);
            }
            cut.close();
        }

        // open again and try to allocate
        MetaAllocatorProxy cut(m_prefix);
        auto ptr = cut.alloc(100);
        // the allocation should be from the other slab since the 1st is full
        ASSERT_TRUE(cut.getSlabId(ptr) > 0);
        cut.close();
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorRemainingCapacityIsTrackedPerSlab )
    {
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        {
            // make allocations until the 2 slabs are occupied
            MetaAllocatorProxy cut(m_prefix, &recycler);
            std::size_t total_allocated = 0;
            std::vector<unsigned int> slab_ids;
            while (cut.getSlabCount() < 2) {
                auto ptr = cut.alloc(100);
                auto slab_id = cut.getSlabId(ptr);
                if (std::find(slab_ids.begin(), slab_ids.end(), slab_id) == slab_ids.end()) {
                    slab_ids.push_back(slab_id);
                }
                total_allocated += 100;
            }

            ASSERT_EQ(2u, slab_ids.size());
            ASSERT_TRUE(cut.getRemainingCapacity(slab_ids[0]) < 100);
            ASSERT_TRUE(cut.getRemainingCapacity(slab_ids[1]) > 100);
            cut.close();
        }
    }

    TEST_F( MetaAllocatorTests , testMetaAllocatorResourcesAreReleasedFromRecyclerOnClose )
    {
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        {
            // make allocations until the 2 slabs are occupied
            MetaAllocator cut(m_prefix, &recycler);
            while (cut.getSlabCount() < 2) {
                cut.alloc(100);            
            }
            cut.close();
        }
        ASSERT_EQ(recycler.size(), 0);
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorRemainingCapacityIsPersistedOnClose )
    {
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        std::vector<unsigned int> slab_ids;
        {
            // make allocations until the 2 slabs are occupied
            MetaAllocatorProxy cut(m_prefix, &recycler);
            while (cut.getSlabCount() < 2) {
                auto ptr = cut.alloc(100);
                auto slab_id = cut.getSlabId(ptr);
                if (std::find(slab_ids.begin(), slab_ids.end(), slab_id) == slab_ids.end()) {
                    slab_ids.push_back(slab_id);
                }
            }
            cut.close();
        }
        
        MetaAllocator cut(m_prefix, &recycler);
        ASSERT_TRUE(cut.getRemainingCapacity(slab_ids[0]) < 100);
        ASSERT_TRUE(cut.getRemainingCapacity(slab_ids[1]) > 100);
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorCanGetAllocSize )
    {
        srand(191231u);
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        std::vector<std::size_t> alloc_sizes;
        std::vector<Address> addresses;
        auto count = 10000;
        {
            MetaAllocator cut(m_prefix, &recycler);
            // make random allocations
            for (int i = 0; i < count; ++i) {
                auto alloc_size = rand() % 1000 + 1;
                alloc_sizes.push_back(alloc_size);
                addresses.push_back(cut.alloc(alloc_size));
            }
            cut.close();
        }

        MetaAllocator cut(m_prefix, &recycler);
        // validate addresses
        for (int i = 0; i < count; ++i) {
            ASSERT_EQ(cut.getAllocSize(addresses[i]), alloc_sizes[i]);
        }
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorCanFree )
    {
        srand(191231u);
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        std::vector<std::size_t> alloc_sizes;
        std::vector<Address> addresses;
        auto count = 10000;
        {
            MetaAllocator cut(m_prefix, &recycler);
            // make random allocations
            for (int i = 0; i < count; ++i) {
                auto alloc_size = rand() % 1000 + 1;
                alloc_sizes.push_back(alloc_size);
                addresses.push_back(cut.alloc(alloc_size));
            }
            cut.close();
        }

        MetaAllocator cut(m_prefix, &recycler);
        // free addresses in random order
        for (int i = 0; i < count / 5; ++i) {
            auto index = rand() % addresses.size();
            if (alloc_sizes[index] > 0) {
                cut.free(addresses[index]);
                alloc_sizes[index] = 0;
            }
        }       
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorCanReleaseEmptySlab )
    {
        srand(191231u);
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        std::map<int, std::vector<Address> > addr_map;
        auto count = 10000;
        {
            // deferred free is disabled
            MetaAllocatorProxy cut(m_prefix, &recycler, false);
            // make random allocations
            for (int i = 0; i < count; ++i) {
                auto alloc_size = rand() % 1000 + 1;
                auto ptr = cut.alloc(alloc_size);
                addr_map[cut.getSlabId(ptr)].push_back(ptr);
            }
            ASSERT_EQ(cut.getSlabCount(), addr_map.size());
            cut.close();
        }
        
        // Remove from the highest slabs first
        auto slab_count = addr_map.size();
        MetaAllocator cut(m_prefix, &recycler, false);
        for (auto it = addr_map.rbegin(), end = addr_map.rend(); it != end; ++it) {
            for (auto ptr: it->second) {
                cut.free(ptr);
            }
            --slab_count;
            ASSERT_EQ(cut.getSlabCount(), slab_count);
        }
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorAllocSpeed )
    {
        /*
        On my machine the reported speed was 5.62M allocs / sec
        for comparison the regular malloc operation did 13.7 allocs / sec
        */
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, 64 * 1024 * 1024);
        SlabRecycler recycler;        
        MetaAllocator cut(m_prefix, &recycler);
        // measure speed
        auto start = std::chrono::high_resolution_clock::now();
        std::size_t total_bytes = 0;
        std::size_t alloc_count = 1000000;
        for (unsigned int i = 0; i < alloc_count; ++i) {
            cut.alloc(100);
            total_bytes += 100;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "MetaAllocator alloc speed: " << elapsed.count() << "ms" << std::endl;
        std::cout << "Total bytes: " << total_bytes << std::endl;
        std::cout << "MB / sec : " << (total_bytes / 1024.0 / 1024.0) * 1000.0 / elapsed.count() << std::endl;
        std::cout << "Allocs / sec : " << alloc_count * 1000.0 / elapsed.count() << std::endl;
        cut.close();
    }

    TEST_F( MetaAllocatorTests , testReservedPrivateSlab )
    {
        using offset_t = typename Address::offset_t;

        srand(191231u);
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        MetaAllocator cut(m_prefix, &recycler);
        auto private_slab = cut.reserveNewSlab();        
        std::pair<std::uint64_t, std::uint64_t> range {
            private_slab->getAddress(), private_slab->getAddress() + static_cast<offset_t>(private_slab->size())
        };

        auto in_range = [&](std::uint64_t address) {
            return address >= range.first && address < range.second;
        };

        // make random allocations, make sure the allocated addresses are
        // not falling into the private range
        for (int i = 0; i < 1000; ++i) {
            auto alloc_size = rand() % 1000 + 1;
            auto ptr = cut.alloc(alloc_size);
            ASSERT_FALSE(in_range(ptr));
        }
        cut.close();
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorFirstAllocatedAddress )
    {        
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        MetaAllocator cut(m_prefix, &recycler);
        ASSERT_EQ(cut.alloc(8), cut.getFirstAddress());
        cut.close();
    }
    
    TEST_F( MetaAllocatorTests , testMetaAllocatorLocalityAwareAllocation )
    {        
        MetaAllocator::formatPrefix(m_prefix, PAGE_SIZE, SMALL_SLAB_SIZE);
        SlabRecycler recycler;
        MetaAllocatorProxy cut(m_prefix, &recycler);
        // locality = 0 (default)
        auto addr_0 = cut.alloc(8, 0, false, 0);
        auto addr_1 = cut.alloc(8, 0, false, 0, 0);
        // locality = 1
        auto addr_2 = cut.alloc(8, 0, false, 0, 1);

        // same slab
        ASSERT_EQ(cut.getSlabId(addr_0), cut.getSlabId(addr_1));
        // different slabs
        ASSERT_NE(cut.getSlabId(addr_0), cut.getSlabId(addr_2));
        cut.close();
    }
    
}