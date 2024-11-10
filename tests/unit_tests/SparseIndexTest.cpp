#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/SparseIndex.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <utils/utils.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class SparseIndexTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";
        SparseIndexTest() = default;

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {        
            drop(file_name);
        }
    };
    
    TEST_F( SparseIndexTest , testSparseIndexCanBeInstantiated ) {
        SparseIndex cut(16 * 1024);
    }

    TEST_F( SparseIndexTest , testSparseIndexCanAppendPageDescriptors )
    {
        SparseIndex cut(16 * 1024);
        cut.emplace(0, 0, 0, SparseIndex::PageType::FIXED);
    }

    void testSparseIndexLookupPageDescriptors(std::size_t node_size)
    {
        SparseIndex cut(node_size);
        std::vector<SparseIndex::Item> items {
            // page number, state number, physical page number, page type
            { 0, 1, 0, SparseIndex::PageType::FIXED },
            { 1, 1, 1, SparseIndex::PageType::FIXED },
            { 2, 1, 2, SparseIndex::PageType::FIXED },
            { 3, 2, 3, SparseIndex::PageType::FIXED },
            { 0, 2, 4, SparseIndex::PageType::FIXED },
            { 2, 3, 5, SparseIndex::PageType::FIXED },
            { 4, 4, 6, SparseIndex::PageType::FIXED }
        };

        // page number, state number
        std::vector<std::pair<std::uint64_t, std::uint32_t> > queries {
            { 0, 1 }, { 0, 2 }, { 0, 3 },
            { 1, 1 }, { 1, 2 }, { 1, 3 }, { 1, 4 }, 
            { 2, 1 }, { 2, 2 }, { 2, 3 }, { 2, 4 }, 
            { 3, 1 }, { 3, 2 }, { 3, 3 }, { 3, 4 }, { 3, 5 },
            { 4, 1 }, { 4, 2 }, { 4, 3 }, { 4, 4 }, { 4, 5 }
        };
        
        // storage page number
        std::vector<std::optional<std::uint64_t> > m_expected_results {
            0, 4, 4,
            1, 1, 1, 1, 
            2, 2, 5, 5, 
            std::nullopt, 3, 3, 3, 3,
            std::nullopt, std::nullopt, std::nullopt, 6, 6
        };

        for (auto &item: items) {
            cut.insert(item);
        }
        unsigned int i = 0;
        for (auto &query: queries) {
            auto pd = cut.lookup(query);
            if (pd) {
                ASSERT_EQ(pd.m_storage_page_num, *m_expected_results[i]);
            } else {
                ASSERT_FALSE(m_expected_results[i].has_value());
            }
            ++i;
        }
    }

    TEST_F( SparseIndexTest , testSparseIndexLookupPageDescriptors )
    { 
        // also test with non-standard node size
        testSparseIndexLookupPageDescriptors(16 * 1024 - 256);
        testSparseIndexLookupPageDescriptors(16 * 1024);
    }

    TEST_F( SparseIndexTest , testSparseIndexCanTrackMaxStoragePageNum )
    {
        SparseIndex cut(16 * 1024);
        std::vector<SparseIndex::Item> items {
            // page number, state number, physical page number, page type
            { 0, 0, 0, SparseIndex::PageType::FIXED },
            { 1, 0, 1, SparseIndex::PageType::FIXED },
            { 2, 0, 2, SparseIndex::PageType::FIXED },
            { 3, 1, 3, SparseIndex::PageType::FIXED },
            { 0, 1, 4, SparseIndex::PageType::FIXED },
            { 2, 2, 5, SparseIndex::PageType::FIXED },
            { 4, 3, 6, SparseIndex::PageType::FIXED }
        };
        for (auto &item: items) {
            cut.insert(item);
        }
        ASSERT_EQ(cut.getNextStoragePageNum(), 7);
    }
    
    TEST_F( SparseIndexTest , testSparseIndexCanTrackMaxStateNum )
    {
        SparseIndex cut(16 * 1024);
        std::vector<SparseIndex::Item> items {
            // page number, state number, physical page number, page type
            { 0, 0, 0, SparseIndex::PageType::FIXED },
            { 1, 0, 1, SparseIndex::PageType::FIXED },
            { 2, 0, 2, SparseIndex::PageType::FIXED },
            { 3, 1, 3, SparseIndex::PageType::FIXED },
            { 0, 1, 4, SparseIndex::PageType::FIXED },
            { 2, 2, 5, SparseIndex::PageType::FIXED },
            { 4, 3, 6, SparseIndex::PageType::FIXED }
        };
        for (auto &item: items) {
            cut.insert(item);
        }
        ASSERT_EQ(cut.getMaxStateNum(), 3);
    }

    TEST_F( SparseIndexTest , testSparseIndexCanBeUpdatedByDRAMSpaceSwap )
    {   
        std::size_t node_size = 16 * 1024;     
        SparseIndex sparse_index(node_size);
        DRAM_Pair dram_pair;
        auto dram_space = DRAMSpace::create(node_size, [&](DRAM_Pair dp) {
            dram_pair = dp;
        });

        SparseIndex cut(dram_pair, AccessType::READ_WRITE);
        std::vector<SparseIndex::Item> items_1 {
            // page number, state number, physical page number, page type
            { 0, 0, 0, SparseIndex::PageType::FIXED },
            { 1, 0, 1, SparseIndex::PageType::FIXED }
        };

        for (auto &item: items_1) {
            sparse_index.insert(item);
        }
        // copy DRAM binary contents between the instances
        *(dram_pair.first) = sparse_index.getDRAMPrefix();

        // make sure the contents is in-sync
        ASSERT_EQ(cut.lookup(0, 0), sparse_index.lookup(0, 0));
        ASSERT_EQ(cut.lookup(1, 0), sparse_index.lookup(1, 0));

        std::vector<SparseIndex::Item> items_2 {
            // page number, state number, physical page number, page type
            { 2, 0, 2, SparseIndex::PageType::FIXED },
            { 3, 1, 3, SparseIndex::PageType::FIXED },
            { 0, 1, 4, SparseIndex::PageType::FIXED },
            { 2, 2, 5, SparseIndex::PageType::FIXED },
            { 4, 3, 6, SparseIndex::PageType::FIXED }
        };

        for (auto &item: items_2) {
            sparse_index.insert(item);        
        }

        (*dram_pair.first) = sparse_index.getDRAMPrefix();
        // make sure the contents is in-sync
        for (unsigned int i = 0; i < 5; ++i) {
            auto state_num = sparse_index.getMaxStateNum();
            ASSERT_EQ(cut.lookup(i, state_num), sparse_index.lookup(i, state_num));
        }
    }

    TEST_F( SparseIndexTest , testSparseIndexMaxStateNumUpdatedAfterRefresh )
    {   
        std::size_t node_size = 16 * 1024;     
        SparseIndex sparse_index(node_size);
        DRAM_Pair dram_pair;
        auto dram_space = DRAMSpace::create(node_size, [&](DRAM_Pair dp) {
            dram_pair = dp;
        });

        SparseIndex cut(dram_pair, AccessType::READ_WRITE);
        std::vector<SparseIndex::Item> items_1 {
            // page number, state number, physical page number, page type
            { 0, 0, 0, SparseIndex::PageType::FIXED },
            { 1, 1, 1, SparseIndex::PageType::FIXED }
        };

        for (auto &item: items_1) {
            sparse_index.insert(item);
        }
        // copy DRAM binary contents between the instances
        *(dram_pair.first) = sparse_index.getDRAMPrefix();
        
        // make sure max-state-number reported correctly after refresh
        cut.refresh();
        ASSERT_EQ(cut.getMaxStateNum(), 1);

        std::vector<SparseIndex::Item> items_2 {
            // page number, state number, physical page number, page type
            { 2, 0, 2, SparseIndex::PageType::FIXED },
            { 3, 1, 3, SparseIndex::PageType::FIXED },
            { 0, 1, 4, SparseIndex::PageType::FIXED },
            { 2, 2, 5, SparseIndex::PageType::FIXED },
            { 4, 3, 6, SparseIndex::PageType::FIXED }
        };

        for (auto &item: items_2) {
            sparse_index.insert(item);
        }

        (*dram_pair.first) = sparse_index.getDRAMPrefix();
        cut.refresh();
        ASSERT_EQ(cut.getMaxStateNum(), 3);
    }

    TEST_F( SparseIndexTest , testSparseIndexCollectsChangeLogOfAddedItems )
    {   
        std::size_t node_size = 16 * 1024;
        SparseIndex sparse_index(node_size);
        DRAM_Pair dram_pair;
        auto dram_space = DRAMSpace::create(node_size, [&](DRAM_Pair dp) {
            dram_pair = dp;
        });

        SparseIndex cut(dram_pair, AccessType::READ_WRITE);
        std::vector<SparseIndex::Item> items_1 {
            // page number, state number, physical page number, page type
            { 1, 1, 1, SparseIndex::PageType::FIXED },
            { 0, 1, 0, SparseIndex::PageType::FIXED }
        };

        for (auto &item: items_1) {
            sparse_index.insert(item);
        }

        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&]() {
            return file.size();
        };
        
        {
            ChangeLogIOStream io(file, 0, 4096, tail_function);
            auto &change_log = sparse_index.extractChangeLog(io);
            std::vector<std::uint64_t> data;
            for (auto value: change_log) {
                data.push_back(value);
            }
            io.close();
            // first element of the change log is the state number
            ASSERT_EQ(data, (std::vector<std::uint64_t> { 1, 0, 1 }));
        }

        std::vector<SparseIndex::Item> items_2 {
            // page number, state number, physical page number, page type
            { 2, 1, 2, SparseIndex::PageType::FIXED },
            { 3, 2, 3, SparseIndex::PageType::FIXED },
            { 0, 3, 4, SparseIndex::PageType::FIXED },
            { 2, 4, 5, SparseIndex::PageType::FIXED },
            { 4, 5, 6, SparseIndex::PageType::FIXED }
        };

        for (auto &item: items_2) {
            sparse_index.insert(item);
        }
        
        {
            ChangeLogIOStream io(file, 0, 4096, tail_function);
            while (io.readChangeLogChunk());
            auto &change_log = sparse_index.extractChangeLog(io);
            // first element of the change log is the state number
            std::vector<std::uint64_t> expected_data { 1, 0, 2, 3, 4 };
            std::vector<std::uint64_t> data;
            for (auto value: change_log) {
                data.push_back(value);
            }
            io.close();
            ASSERT_EQ(data, expected_data);
        }
    }
    
    TEST_F( SparseIndexTest , testSparseIndexInsertFailingCase )
    {
        SparseIndex cut(16 * 1024);
        std::vector<SparseIndex::Item> items {
            // page number, state number, physical page number, page type
            { 0, 1, 0, SparseIndex::PageType::FIXED }
        };
        for (auto &item: items) {
            cut.insert(item);
        }

        std::vector<std::uint64_t> page_num;
        cut.forAll([&](const SparseIndex::Item &item) {
            page_num.push_back(item.m_page_num);        
        });
        ASSERT_EQ(page_num, std::vector<std::uint64_t> { 0 });
    }
    
    TEST_F( SparseIndexTest , testSparseIndexInsertLookupFailingCase )
    {
        SparseIndex cut(16 * 1024);
        std::vector<SparseIndex::Item> items {
            // page number, state number, physical page number, page type
            { 0, 1, 0, SparseIndex::PageType::FIXED }
        };
        for (auto &item: items) {
            cut.insert(item);
        }

        ASSERT_TRUE(cut.lookup(0, 1));
    }
    
    TEST_F( SparseIndexTest , testSparseIndexBadWriteIssue )
    {
        // note non-standard page size (used in production)
        std::size_t dp_size = 16356;
        auto prefix = std::make_shared<db0::DRAM_Prefix>(dp_size);
        auto allocator = std::make_shared<db0::DRAM_Allocator>(dp_size);
        
        CFile::create(file_name, {});
        db0::CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&]() {
            return file.size();
        };     

        int count = 10;
        for (int i = 0; i < count; ++i) {
            SparseIndex cut({ prefix, allocator}, AccessType::READ_WRITE);
            for (unsigned int page_num = 0; page_num < 1000; ++page_num) {
                cut.emplace(page_num, i, 999, SparseIndex::PageType::FIXED);
            }
            
            // simulate change log extraction
            db0::ChangeLogIOStream io(file, 0, 16 << 10, tail_function, AccessType::READ_WRITE);
            while (io.readChangeLogChunk());
            cut.extractChangeLog(io);
            io.close();

            // refresh updates local cached variables with DRAM prefix
            cut.refresh();
            ASSERT_EQ(cut.getMaxStateNum(), i);
        }
    }
    
}
