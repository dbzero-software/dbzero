// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <dbzero/core/memory/BitSpace.hpp>
#include <dbzero/core/crdt/CRDT_Allocator.hpp>
#include <utils/TestWorkspace.hpp>

using namespace std;

namespace tests

{

    using Address = db0::Address;

    class CRDT_AllocatorTests: public testing::Test
    {
    public:
        CRDT_AllocatorTests()
            : m_memspace(m_workspace.getMemspace("my-test-prefix_1"))
            // configure bitspace to use the entire 4kb page - i.e. 0x8000 bits
            , m_bitspace(m_memspace.getPrefixPtr(), Address::fromOffset(0), page_size)
        {
        }

        virtual void SetUp() override {
            init(MAX_ADDRESS);
        }

        virtual void TearDown() override
        {
            m_allocs.reset();
            m_blanks.reset();
            m_aligned_blanks.reset();
            m_stripes.reset();
            m_bitspace.clear();    
        }

    protected:
        using Blank = db0::CRDT_Allocator::Blank;
        using AllocSetT = db0::CRDT_Allocator::AllocSetT;
        using BlankSetT = db0::CRDT_Allocator::BlankSetT;
        using AlignedBlankSetT = db0::CRDT_Allocator::AlignedBlankSetT;
        using StripeSetT = db0::CRDT_Allocator::StripeSetT;

        static constexpr unsigned int MAX_ADDRESS = 2000;
        db0::TestWorkspace m_workspace;
        static constexpr std::size_t page_size = 4096;
        db0::Memspace m_memspace;
        db0::BitSpace<0x8000> m_bitspace;
        std::unique_ptr<AllocSetT> m_allocs;
        std::unique_ptr<BlankSetT> m_blanks;
        std::unique_ptr<AlignedBlankSetT> m_aligned_blanks;
        std::unique_ptr<StripeSetT> m_stripes;

        void init(std::size_t max_addr, const std::vector<std::pair<std::uint32_t, std::uint32_t>> &blanks = {}, 
            std::optional<std::uint32_t> min_aligned_alloc_size = {})
        {
            using CompT = typename AlignedBlankSetT::CompT;

            // put allocs and stripes on the same bitspace
            m_allocs = std::make_unique<AllocSetT>(m_bitspace, page_size);
            m_blanks = std::make_unique<BlankSetT>(m_bitspace, page_size);
            m_aligned_blanks = std::make_unique<AlignedBlankSetT>(m_bitspace, page_size, CompT(page_size), page_size);
            m_stripes = std::make_unique<StripeSetT>(m_bitspace, page_size);
            // by default, initialize containers by registering a single blank starting at 0x0
            if (blanks.empty()) {
                db0::CRDT_Allocator::insertBlank(*m_blanks, *m_aligned_blanks,
                    { static_cast<std::uint32_t>(max_addr), 0 }, page_size, min_aligned_alloc_size
                );
            } else {
                for (auto &blank : blanks) {
                    db0::CRDT_Allocator::insertBlank(
                        *m_blanks, *m_aligned_blanks, { blank.first, blank.second }, page_size, min_aligned_alloc_size
                    );
                }
            }
        }
    };
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorCanAllocFromBlanks )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        cut.alloc(8);

        ASSERT_EQ(m_allocs->size(), 1);
        ASSERT_EQ(m_blanks->size(), 1);        
        ASSERT_EQ(m_stripes->size(), 1);
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorCanAllocFromStripes )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        cut.alloc(8);
        cut.alloc(8);
        // the 3rd allocation should be taken from the 'stripes'
        cut.alloc(8);

        ASSERT_EQ(m_allocs->size(), 2);
        ASSERT_EQ(m_blanks->size(), 1);        
        ASSERT_EQ(m_stripes->size(), 1);
    }

    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorCanAlloc )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);

        // allocate 10 items of identical size
        std::vector<std::uint32_t> m_addresses;
        for (std::uint64_t i = 0; i < 10; ++i) {
            m_addresses.push_back(cut.alloc(8));
        }

        // validate all addressess are unique
        std::sort(m_addresses.begin(), m_addresses.end());
        auto last = std::unique(m_addresses.begin(), m_addresses.end());
        ASSERT_EQ(last, m_addresses.end());
    }

    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorCanAllocFromMultipleStripes )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        cut.alloc(8);
        cut.alloc(8);
        cut.alloc(11);
        cut.alloc(11);

        ASSERT_EQ(m_allocs->size(), 4);
        ASSERT_EQ(m_blanks->size(), 1);        
        ASSERT_EQ(m_stripes->size(), 2);

        // the subsequent allocations done from existing stripes
        cut.alloc(8);
        cut.alloc(11);
        ASSERT_EQ(m_allocs->size(), 4);
        ASSERT_EQ(m_blanks->size(), 1);
        ASSERT_EQ(m_stripes->size(), 2);
    }

    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorFillMapAll )
    {
        auto size = db0::crdt::SIZE_MAP[0];
        db0::CRDT_Allocator::FillMap fill_map(size, false);
        ASSERT_EQ(fill_map.size(), size);
        for (unsigned int i = 0;i < size;++i) {
            fill_map.allocUnit();
        }
        ASSERT_TRUE(fill_map.all());
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorGetAllocSize )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        std::vector<std::size_t> sizes = { 1, 2, 4, 19, 33, 2, 4, 33, 129 };
        std::vector<std::uint64_t> addresses;

        for (auto size : sizes) {
            addresses.push_back(cut.alloc(size));
        }

        // validate alloc sizes
        for (std::size_t i = 0; i < sizes.size(); ++i) {
            ASSERT_EQ(cut.getAllocSize(addresses[i]), sizes[i]);
        }
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorFreeFromAllocs )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, 64 * page_size, page_size);
        std::vector<std::size_t> sizes = { 16, 16, 16, 1, 2, 4 };
        std::vector<std::uint64_t> addresses;
        
        for (auto size : sizes) {
            addresses.push_back(cut.alloc(size));
        }
        
        ASSERT_EQ(m_blanks->size(), 1);
        ASSERT_EQ(m_stripes->size(), 4);
        
        cut.free(addresses[0]);
        cut.free(addresses[1]);
        cut.free(addresses[2]);
        // after removing the 16-byte stripe, the new blank should be created
        ASSERT_EQ(m_blanks->size(), 2);
        ASSERT_EQ(m_stripes->size(), 3);

        cut.free(addresses[4]);
        ASSERT_EQ(m_stripes->size(), 2);
        ASSERT_EQ(m_blanks->size(), 3);
        
        cut.free(addresses[3]);
        ASSERT_EQ(m_stripes->size(), 1);
        ASSERT_EQ(m_blanks->size(), 2);

        cut.free(addresses[5]);
        ASSERT_EQ(m_stripes->size(), 0);
        ASSERT_EQ(m_blanks->size(), 1);
    }

    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorSubsequentlyAllocatedStripesGrowInSize )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        std::vector<int> stripe_sizes;
        std::optional<unsigned int> last_alloc_count;
        int current_stripe_size = 0;
        while (true) {
            cut.alloc(4);
            auto alloc_count = m_allocs->size();
            if (last_alloc_count && alloc_count > *last_alloc_count) {
                if (!stripe_sizes.empty() && stripe_sizes.back() == current_stripe_size) {
                    break;
                }                
                stripe_sizes.push_back(current_stripe_size);
                current_stripe_size = 0;
            }
            ++current_stripe_size;
            last_alloc_count = alloc_count;            
        }
        // validate stripe sizes
        ASSERT_EQ(stripe_sizes.size(), 4);
        for (unsigned int i = 1; i < stripe_sizes.size(); ++i) {
            ASSERT_TRUE(stripe_sizes[i] > stripe_sizes[i - 1]);
        }
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorCanReclaimSpaceFromStripes )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        std::vector<std::uint64_t> addresses;
        std::vector<std::uint32_t> stripe_ids;
        std::optional<unsigned int> last_alloc_count;
        unsigned int stripe_id = 0;
        // allocate entire available test space
        while (true) {
            auto result = cut.tryAlloc(8);
            if (!result) {
                break;
            }
            auto alloc_count = m_allocs->size();
            if (last_alloc_count && alloc_count > *last_alloc_count) {
                ++stripe_id;
            }
            last_alloc_count = alloc_count;
            addresses.push_back(*result);
            stripe_ids.push_back(stripe_id);
        }

        // leave 1 unit allocated in each stripe
        for (unsigned int i = 1; i < stripe_ids.size(); ++i) {
            if (stripe_ids[i] == stripe_ids[i - 1]) {
                cut.free(addresses[i]);
            }
        }
        ASSERT_EQ(m_allocs->size(), *last_alloc_count);
        
        // now try a different size alloc, the space should be reclaimed from stripes
        ASSERT_NO_THROW(cut.alloc(11));
        ASSERT_NO_THROW(cut.alloc(15));
        ASSERT_NO_THROW(cut.alloc(31));
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorFromStripeCanBeConstrainedWithDynamicBounds )
    {
        std::uint32_t admin_span = 200;
        std::uint32_t dynamic_bound = MAX_ADDRESS;
        auto bounds_fn = [admin_span, &dynamic_bound]() -> std::tuple<std::uint32_t, std::uint32_t, std::uint32_t> {
            std::uint32_t b2 = dynamic_bound;
            std::uint32_t b1 = (b2 >= admin_span) ? b2 - admin_span : 0;
            std::uint32_t b0 = (b1 >= admin_span) ? b1 - admin_span : 0;
            return { b0, b1, b2 };
        };
        
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        cut.setDynamicBound(bounds_fn);

        std::uint64_t max_addr = 0;
        for (int i = 0; i < 100; ++i) {
            max_addr = std::max(max_addr, cut.alloc(4));
        }

        // update dynamic bound to the last assigned address (plus admin margin)
        dynamic_bound = max_addr + 2 * admin_span;
        std::optional<std::uint64_t> alloc;
        // all subsequent allocations must be within the updated bounds
        while ((alloc = cut.tryAlloc(4))) {
            ASSERT_TRUE(*alloc + 4 <= dynamic_bound);
        }
    }

    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorAllocThenFree )
    {
        srand(5916412u);
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        
        std::vector<std::size_t> alloc_sizes;
        std::vector<std::uint64_t> addresses;
        auto count = 100;
        {            
            // make random allocations
            for (int i = 0; i < count; ++i) {
                auto alloc_size = rand() % 50 + 1;
                auto ptr = cut.tryAlloc(alloc_size);
                if (ptr) {
                    alloc_sizes.push_back(alloc_size);
                    addresses.push_back(*ptr);
                }
            }          
        }
        
        // free selected addresses in random order
        for (int i = 0; i < count / 5; ++i) {
            auto index = rand() % addresses.size();
            if (alloc_sizes[index] > 0) {
                cut.free(addresses[index]);
                alloc_sizes[index] = 0;
            }
        }           
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorAllocSpeed )
    {
        auto max_addr = 64 * 1024 * 1024;
        init(max_addr);
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, max_addr, page_size);
        // measure speed
        auto start = std::chrono::high_resolution_clock::now();
        std::size_t total_bytes = 0;
        for (int i = 0; i < 100000; ++i) {
            cut.alloc(100);
            total_bytes += 100;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "CRDT_Allocator alloc speed: " << elapsed.count() << "ms" << std::endl;
        std::cout << "Total bytes: " << total_bytes << std::endl;
        std::cout << "MB / sec : " << (total_bytes / 1024.0 / 1024.0) * 1000.0 / elapsed.count() << std::endl;
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AllocatorFirstAllocatedAddress )
    {
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, MAX_ADDRESS, page_size);
        ASSERT_EQ(cut.alloc(8), db0::CRDT_Allocator::getFirstAddress());
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_PageAlignedAllocs )
    {
        auto capacity = page_size * 3 * 100;
        auto min_aligned_alloc_size = 16;
        init(capacity);
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes,
            capacity, page_size, min_aligned_alloc_size);
        // first allocation may not be able to page-align
        cut.alloc(8);
        for (int i = 0; i < 100; ++i) {
            auto addr = cut.alloc(rand() % page_size * 2 + min_aligned_alloc_size, true);
            // make sure is page aligned
            ASSERT_EQ(addr % page_size, 0);
        }
    }
    
    TEST_F( CRDT_AllocatorTests , testCRDT_AlignedAllocsFromSmallBlanks )
    {
        auto capacity = page_size * 256;
        auto min_aligned_alloc_size = 4;
        // initialize with small blanks (size / address)
        init(capacity, { { 100, 0 }, { 125, page_size - 25 }, { 2033, page_size * 2 - 33 }, { 10242, page_size * 10 },
            { 14, page_size * 3 - 10 } }, min_aligned_alloc_size);
        // Use custom configuration of min_aligned_alloc_size = 4
        db0::CRDT_Allocator cut(*m_allocs, *m_blanks, *m_aligned_blanks, *m_stripes, capacity, page_size, min_aligned_alloc_size);
        // first allocation may not be able to page-aligned
        auto addr = cut.alloc(100);
        ASSERT_EQ(addr, 0);
        for (auto alloc_size: { 8192, 2000, 100, 4 }) {
            auto addr = cut.alloc(alloc_size, true);
            // make sure is page aligned
            ASSERT_EQ(addr % page_size, 0);            
        }
    }

    TEST_F( CRDT_AllocatorTests , testBlanksInsertEraseIssue_1 )
    {
        auto erase = [&](Blank blank) {
            auto it = m_blanks->find_equal(blank);
            ASSERT_TRUE(it.first);
            m_blanks->erase(it);
        };

        m_blanks->insert({ 5617, 4143631 });
        m_blanks->insert({ 8064, 2414479 });
        erase({ 8064, 2414479 });
        m_blanks->insert({ 5760, 2416783 });
        erase({ 5760, 2416783 });
        m_blanks->insert({ 3456, 2419087 });
        erase({ 3456, 2419087 });
        m_blanks->insert({ 1152, 2421391 });

        auto blank_ptr = m_blanks->upper_equal_bound(Blank(2304, 0));
        ASSERT_EQ((*blank_ptr.first).m_size, 5617);
    }
    
}
