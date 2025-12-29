// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <utils/TestWorkspace.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/memory/BitSpace.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class BDevStorageTest: public testing::Test {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override {            
            drop(file_name);
        }

        virtual void TearDown() override {            
            drop(file_name);
        }
    };
    
    // Wrapper class for testing
    class BDevStorageWrapper: public BDevStorage
    {
    public:
        /**
         * Opens BDevStorage over an existing file
        */
        BDevStorageWrapper(const std::string &file_name, AccessType = AccessType::READ_WRITE)
            : BDevStorage(file_name, AccessType::READ_WRITE)
        {
        }
        
        SparseIndex &getSparseIndex() {
            return m_sparse_index;
        }

        const DRAM_IOStream &getDRAM_IOStream() const {
            return m_dram_io;
        }

        void readMetered(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
            unsigned int &chain_len) const
        {
            _read(address, state_num, size, buffer, { AccessOptions::read }, &chain_len);
        }
    };

    TEST_F( BDevStorageTest , testCanCreateEmptyDB0FileWithDefaultConfiguration )
    {         
        BDevStorage::create(file_name);
        ASSERT_TRUE(file_exists(file_name));
    }

    TEST_F( BDevStorageTest , testCanWriteThenReadFullPagesFromOneState )
    { 
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        auto page_size = cut.getPageSize();
        // a valid state number must be > 0
        auto state_num = 1;
        std::unordered_map<std::uint64_t, std::vector<char>> pages;
        for (int i = 0; i < 100; ++i) {
            auto page_num = rand() % 10000;
            if (pages.find(page_num) != pages.end()) {
                continue;
            }
            auto &page = pages.insert({page_num, randomPage(page_size)}).first->second;
            cut.write(page_num * page_size, state_num, page.size(), page.data());
        }

        // read pages & validate contents
        for (auto &page: pages) {
            std::vector<char> read_buffer(page_size);
            cut.read(page.first * page_size, state_num, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(page.second, read_buffer));
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testCanReadPagesFromDifferentStates )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        std::deque<std::vector<char> > pages;
        for (int i = 0;i < 10;++i) {
            auto state_num = 1 + i * 5;
            pages.push_back(randomPage(cut.getPageSize()));
            // write page under state "i * 5"
            cut.write(0, state_num, pages.back().size(), pages.back().data());
        }
        // pairs of query / expected states
        std::vector<std::pair<int, int> > states = {
            {0 + 1, 0 + 1}, {3 + 1, 0 + 1}, {11 + 1, 10 + 1}, {33 + 1, 30 + 1}, {34 + 1, 30 + 1}, 
            {51 + 1, 45 + 1}, {99 + 1, 45 + 1}, {12 + 1, 10 + 1}
        };
        for (auto &p: states) {
            std::vector<char> read_buffer(cut.getPageSize());
            cut.read(0, p.first, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages[p.second / 5], read_buffer));
        }
        cut.close();
    }
    
    TEST_F( BDevStorageTest , testSparseIndexIsSerializedOnClose )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        std::deque<std::vector<char> > pages;
        {
            BDevStorage cut(file_name);
            for (int i = 0;i < 10;++i) {
                pages.push_back(randomPage(cut.getPageSize()));
                auto state_num = 1 + i * 5;
                // write page under state "i * 5"
                cut.write(0, state_num, pages.back().size(), pages.back().data());
            }
            cut.close();
        }
        // open storage again
        BDevStorage cut(file_name);
        // pairs of query / expected states
        std::vector<std::pair<int, int> > states = {
            {0 + 1, 0 + 1}, {3 + 1, 0 + 1}, {11 + 1, 10 + 1}, {33 + 1, 30 + 1}, {34 + 1, 30 + 1}, 
            {51 + 1, 45 + 1}, {99 + 1, 45 + 1}, {12 + 1, 10 + 1}
        };
        for (auto &p: states) {
            std::vector<char> read_buffer(cut.getPageSize());
            cut.read(0, p.first, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages[p.second / 5], read_buffer));
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testBDevStorageThrowsIfReadingFromUninitializedSpace )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name, AccessType::READ_ONLY);
        std::vector<char> buffer(cut.getPageSize());
        ASSERT_ANY_THROW(cut.read(0, 1, cut.getPageSize(), buffer.data(), { AccessOptions::read }));
    }
    
    TEST_F( BDevStorageTest , testBDevStorageZeroInitializeNewPagesIfAccessedForWriteOnly )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        std::vector<char> buffer(cut.getPageSize());
        std::vector<char> zero_buffer(cut.getPageSize());
        memset(zero_buffer.data(), 0, zero_buffer.size());
        
        cut.read(0, 1, cut.getPageSize(), buffer.data(), { AccessOptions::write });
        ASSERT_TRUE(equal(zero_buffer, buffer));
        cut.close();
    }
    
    TEST_F( BDevStorageTest , testCanFindMutation )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        auto page_size = cut.getPageSize();
        std::deque<std::vector<char> > pages;
        for (int i = 0;i < 10;++i) {            
            pages.push_back(randomPage(page_size));
            // a valid state num must be > 0
            auto state_num = 1 + i * 5;
            // write page under state "i * 5", address = page num * page_size
            cut.write(i * page_size, state_num, page_size, pages.back().data());
        }
        ASSERT_EQ(cut.findMutation(0, 1 + 3), 1);
        // unable to read page #1 (not yet available in state = 1)
        StateNumType mutation_id;
        ASSERT_FALSE(cut.tryFindMutation(1, 1, mutation_id));
        cut.close();
    }

    TEST_F( BDevStorageTest , testSparseIndexIsProperlySerializedAfterUpdates )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        std::deque<std::vector<char> > pages_v1;
        {
            BDevStorage cut(file_name);
            for (int i = 0;i < 10;++i) {
                pages_v1.push_back(randomPage(cut.getPageSize()));
                // write pages under state "1"
                cut.write(i * cut.getPageSize(), 1, pages_v1.back().size(), pages_v1.back().data());
            }
            cut.close();
        }
        
        std::deque<std::vector<char> > pages_v2;
        {
            BDevStorage cut(file_name);
            for (int i = 0;i < 10;++i) {
                pages_v2.push_back(randomPage(cut.getPageSize()));
                // write pages under state "2"
                cut.write(i * cut.getPageSize(), 2, pages_v2.back().size(), pages_v2.back().data());
            }
            cut.close();
        }

        // open storage and try retrieving both versions
        BDevStorage cut(file_name);
        for (int i = 0;i < 10;++i) {
            std::vector<char> read_buffer(cut.getPageSize());
            cut.read(i * cut.getPageSize(), 1, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages_v1[i], read_buffer));
            cut.read(i * cut.getPageSize(), 2, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages_v2[i], read_buffer));
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testStateWiseWriteThenRead )
    {   
        // In this test scenario we simply perform a sequence of writes
        // and then read and validate contents
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };

        // Writer, eaach write performed under a different state number
        {
            BDevStorage cut(file_name, AccessType::READ_WRITE);
            StateNumType state_num = 1;
            for (auto &op: write_ops) {
                std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
                cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
                // flush after each write for additional validation
                cut.flush();
                ++state_num;
            }
            cut.close();
        }
        
        // Reader, validate contents
        {
            BDevStorage cut(file_name, AccessType::READ_ONLY);
            StateNumType state_num = 1;
            for (auto &op: write_ops) {
                std::vector<char> buffer(std::get<1>(op) * page_size);
                cut.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                // validate contents
                for (std::size_t i = 0;i < buffer.size();++i) {
                    ASSERT_EQ(buffer[i], std::get<2>(op));
                }                
                ++state_num;
            }
            cut.close();
        }
    }

    TEST_F( BDevStorageTest , testReadAfterFlushButWithoutClose )
    {   
        // In this test scenario we perform sequence of write/flush
        // and try reading before closing the output stream        
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };

        // Writer, eaach write performed under a different state number    
        BDevStorage cut(file_name, AccessType::READ_WRITE);
        StateNumType state_num = 1;
        for (auto &op: write_ops) {
            std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
            cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
            // flush after each write for additional validation
            cut.flush();

            // Attempt reading before close
            {
                BDevStorage reader(file_name, AccessType::READ_ONLY);
                std::vector<char> buffer(std::get<1>(op) * page_size);
                reader.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                // validate contents
                for (std::size_t i = 0;i < buffer.size();++i) {
                    ASSERT_EQ(buffer[i], std::get<2>(op));
                }
                reader.close();
            }

            ++state_num;
        }
        cut.close();        
    }

    TEST_F( BDevStorageTest , testConcurrentStorageWriterAndReaderWithClose )
    {
        // This is to test the scenario when file is flushed and the modifications
        // should be accessible to a newly opened read-only instance, no refresh called     
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };
        
        // Start reader from a separate thread
        std::thread reader([&]()
        {
            StateNumType state_num = 1;
            for (auto &op: write_ops) {
                bool success = false;
                while (!success) {
                    BDevStorage storage(file_name, AccessType::READ_ONLY);
                    // only attempt reading when the state is available
                    if (storage.getMaxStateNum() >= state_num) {
                        std::vector<char> buffer(std::get<1>(op) * page_size);
                        storage.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                        // validate contents
                        for (std::size_t i = 0;i < buffer.size();++i) {
                            ASSERT_EQ(buffer[i], std::get<2>(op));
                        }
                        success = true;
                    }
                    storage.close();
                    // sleep before making another attempt                    
                    if (!success) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                }
                ++state_num;
            }
        });
        
        BDevStorage cut(file_name, AccessType::READ_WRITE);
        StateNumType state_num = 1;
        for (auto &op: write_ops) {
            std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
            cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
            // flush data after each write
            cut.flush();
            ++state_num;
            // sleep 25ms
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        cut.close();
        reader.join();
    }
    
    TEST_F( BDevStorageTest , testConcurrentWriterAndReaderUsingRefresh )
    {
        // In this test case the reader is not closing the storage but using 'refresh' to 
        // sync to the latest changes
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };
        
        // Start reader from a separate thread
        std::thread reader([&]()
        {
            StateNumType state_num = 1;
            BDevStorage storage(file_name, AccessType::READ_ONLY);
            for (auto &op: write_ops) {
                bool success = false;
                while (!success) {
                    // refresh before making an attempt
                    storage.refresh();
                    // only attempt reading when the state is available
                    if (storage.getMaxStateNum() >= state_num) {
                        std::vector<char> buffer(std::get<1>(op) * page_size);
                        storage.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                        // validate contents
                        for (std::size_t i = 0;i < buffer.size();++i) {
                            ASSERT_EQ(buffer[i], std::get<2>(op));
                        }
                        success = true;
                    }

                    // sleep before making another attempt
                    if (!success) {                        
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                }
                ++state_num;
            }
            storage.close();
        });
        
        BDevStorage cut(file_name, AccessType::READ_WRITE);
        StateNumType state_num = 1;
        for (auto &op: write_ops) {
            std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
            cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
            // flush data after each write
            cut.flush();
            ++state_num;
            // sleep 25ms
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        cut.close();
        reader.join();
    }
    
    TEST_F( BDevStorageTest , testSparseIndexDurability )
    {   
        // In this test scenario we perform sequence of write/flush
        // and try reading before closing the output stream        
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        auto count = 10;
        std::optional<int> last_state_num;
        for (int i = 0; i < count; ++i) {
            BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
                        
            if (last_state_num) {
                ASSERT_EQ(cut.getMaxStateNum(), *last_state_num);
            }
            auto &sparse_index = cut.getSparseIndex();
            for (unsigned int page_num = 0; page_num < 1000; ++page_num) {
                sparse_index.emplace(page_num, i, 999);

                cut.getSparseIndex().refresh();
                ASSERT_EQ(cut.getMaxStateNum(), (std::uint32_t)i);
            }
            
            cut.getSparseIndex().refresh();
            ASSERT_EQ(cut.getMaxStateNum(), (std::uint32_t)i);
            cut.close();
            last_state_num = i;
        }
    }
    
    TEST_F( BDevStorageTest , testDiffsChainIsLimited )
    {   
        // in this test we perform as sequence of writes usind the diff-storage
        // the expected outcome is that the diff-chain is limited to the specified length
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);        
        std::optional<int> last_state_num;
        BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);

        std::vector<std::byte> dp_0(page_size, std::byte{0});
        std::vector<std::byte> dp_1(page_size, std::byte{0});
        unsigned int max_len = 16;
        for (int i = 1; i < 100; ++i) {
            std::vector<std::uint16_t> diffs;
            dp_1[150] = (std::byte)(i + 1);
            ASSERT_TRUE(db0::getDiffs(dp_0.data(), dp_1.data(), dp_1.size(), diffs));            
            if (!cut.tryWriteDiffs(0, i + 1, page_size, dp_1.data(), diffs, max_len)) {
                cut.write(0, i + 1, page_size, dp_1.data());
            }
            cut.flush();
            std::memcpy(dp_0.data(), dp_1.data(), dp_1.size());
        }
        
        // now, reading the data from past transactions verify that then chain length is limited
        unsigned int last_chain_len = 0;
        for (int i = 1; i < 100; ++i) {
            std::vector<char> buffer(page_size);
            unsigned int chain_len = 0;
            cut.readMetered(0, i + 1, buffer.size(), buffer.data(), chain_len);
            ASSERT_EQ(buffer[150], i + 1);
            ASSERT_TRUE(chain_len <= max_len);
            ASSERT_TRUE(chain_len >= last_chain_len || chain_len <= 1);
            last_chain_len = chain_len;
        }
        cut.close();
    }
    
    TEST_F( BDevStorageTest , testBDevStorageFetchChangeLogs )
    {
        using DP_ChangeLogT = db0::BaseStorage::DP_ChangeLogT;

        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name, AccessType::READ_WRITE, {}, 16);

        std::vector<std::vector<std::uint64_t> > updates {
            { 0, 1, 2, 3 },
            { 14, 13, 10, 11, 12 },
            { 21, 20 },
            { 35, 30, 31, 32, 33, 34 },
            { 43, 40, 44, 41, 42 },
            { 50, 51, 52, 53 },
            { 60, 61, 62, 63, 64 },
            { 73, 72, 70, 71 },
            { 80, 81, 82, 83 },
            { 91, 90 }
        };

        StateNumType state_num = 1;
        for (auto &page_nums: updates) {
            std::vector<char> page(cut.getPageSize());
            for (auto &page_num: page_nums) {
                std::memset(page.data(), page_num, page.size());
                cut.write(page_num * cut.getPageSize(), state_num, page.size(), page.data());
            }
            cut.flush();
            ++state_num;
        }

        std::vector<std::pair<StateNumType, std::optional<StateNumType> > > state_ranges {
            { 9, std::nullopt },
            { 4, 10 },
            { 8, 12 },
            { 3, 7 }
        };

        std::vector<std::vector<StateNumType> > expected_state_nums {
            { 9, 10 },
            { 4, 5, 6, 7, 8, 9 },
            { 8, 9, 10 },
            { 3, 4, 5, 6 }
        };
        
        unsigned int range_id = 0;
        for (auto range: state_ranges) {
            // collect and validate change-logs
            std::vector<StateNumType> state_nums;
            cut.fetchDP_ChangeLogs(range.first, range.second, [&](const DP_ChangeLogT &cl) {
                state_nums.push_back(cl.m_state_num);
                std::vector<std::uint64_t> page_nums;
                for (auto page_num: cl) {
                    page_nums.push_back(page_num);
                }
                auto sorted_updates = updates[cl.m_state_num - 1];
                std::sort(sorted_updates.begin(), sorted_updates.end());
                ASSERT_EQ(page_nums, sorted_updates);
            });
            
            ASSERT_EQ(state_nums, expected_state_nums[range_id]);
            ++range_id;
        }
        
        cut.close();
    }

}
