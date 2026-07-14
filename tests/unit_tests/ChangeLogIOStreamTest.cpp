// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class ChangeLogIOStreamTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override {            
            drop(file_name);
        }

        virtual void TearDown() override {            
            drop(file_name);
        }
    };
    
    TEST_F( ChangeLogIOStreamTest , testAppendStoresTheLastChangeLogChunk )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);        
        ChangeLogIOStream cut(file, 0, 4096);        
        // chunk 1 (RLE)
        {
            std::vector<std::uint64_t> change_log = { 1, 2, 3, 4, 5 };
            ChangeLogData data(std::move(change_log), true, false, false);
            cut.appendChangeLog(std::move(data));
        }
        // chunk 2 (uncompressed)
        {
            std::vector<std::uint64_t> change_log = { 3, 4, 8 };
            ChangeLogData data(std::move(change_log), false, false, false);
            cut.appendChangeLog(std::move(data));
        }
        std::vector<std::uint64_t> data;
        std::vector<std::uint64_t> expected_data { 3, 4, 8 };
        for (auto value: *cut.getLastChangeLogChunk()) {
            data.push_back(value);
        }
        cut.close();
        ASSERT_EQ(expected_data, data);
    }
    
    TEST_F( ChangeLogIOStreamTest , testReadStoresTheLastChangeLogChunkInternally )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);
                
        // write some data
        {
            ChangeLogIOStream cut(file, 0, 4096);
            // chunk 1 (uncompressed)
            {
                std::vector<std::uint64_t> change_log = { 3, 4, 8 };
                ChangeLogData data(std::move(change_log), false, false, false);
                cut.appendChangeLog(std::move(data));
            }
            // chunk 2 (RLE)
            {
                std::vector<std::uint64_t> change_log = { 1, 2, 3, 4, 5 };
                ChangeLogData data(std::move(change_log), true, false, false);
                cut.appendChangeLog(std::move(data));
            }
            cut.close();
        }

        ChangeLogIOStream cut(file, 0, 4096);        
        // read all chunks
        while (cut.readChangeLogChunk());

        // validate last chunk's contents
        std::vector<std::uint64_t> data;
        std::vector<std::uint64_t> expected_data { 1, 2, 3, 4, 5 };
        for (auto value: *cut.getLastChangeLogChunk()) {
            data.push_back(value);
        }
        cut.close();
        ASSERT_EQ(expected_data, data);
    }

    TEST_F( ChangeLogIOStreamTest , testChangeLogWriterAndReader )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        
        // some example write operations
        std::vector<std::vector<std::uint64_t> > write_ops {
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 }
        };
        
        // Start reader first
        std::thread reader([&]()
        {            
            CFile file(file_name, AccessType::READ_ONLY);
            ChangeLogIOStream stream(file, 0, 4096, {}, AccessType::READ_ONLY);
            for (auto &op: write_ops) {
                bool success = false;
                while (!success) {
                    // refresh before making an attempt
                    if (stream.eos()) {
                        stream.refresh();
                    }
                    auto changelog_ptr = stream.readChangeLogChunk();
                    if (changelog_ptr) {
                        std::vector<std::uint64_t> data;
                        for (auto value: *changelog_ptr) {
                            data.push_back(value);
                        }
                        ASSERT_EQ(op, data);
                        success = true;
                    }

                    // sleep before making another attempt                    
                    if (!success) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                }
            }
            stream.close();
        });

        // Write some data
        CFile file(file_name, AccessType::READ_WRITE);
        ChangeLogIOStream stream(file, 0, 4096);        
        for (auto op: write_ops) {
            ChangeLogData data(std::move(op), false, false, false);
            stream.appendChangeLog(std::move(data));
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        } 
        stream.close();
        reader.join();
    }

    TEST_F( ChangeLogIOStreamTest , testRefreshReturnsFalseIfNothingChanged )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        // write some data
        {
            ChangeLogIOStream cut(file, 0, 4096);
            // chunk 1 (uncompressed)
            {
                std::vector<std::uint64_t> change_log = { 3, 4, 8 };
                ChangeLogData data(std::move(change_log), false, false, false);
                cut.appendChangeLog(std::move(data));
            }
            // chunk 2 (RLE)
            {
                std::vector<std::uint64_t> change_log = { 1, 2, 3, 4, 5 };
                ChangeLogData data(std::move(change_log), true, false, false);
                cut.appendChangeLog(std::move(data));
            }
            cut.close();
        }
        
        ChangeLogIOStream cut(file, 0, 4096, {}, AccessType::READ_ONLY);        
        // read all chunks
        while (cut.readChangeLogChunk());
        bool refresh_result = cut.refresh();        
        cut.close();
        ASSERT_FALSE(refresh_result);
    }

    TEST_F( ChangeLogIOStreamTest , testCanAppendWithRLECompression )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        ChangeLogIOStream cut(file, 0, 4096);
        // append with RLE compression
        ChangeLogData cl_data(std::vector<std::uint64_t> { 0, 1}, true, false, false);
        auto &change_log = cut.appendChangeLog(std::move(cl_data));
        std::vector<std::uint64_t> data;
        for (auto value: change_log) {
            data.push_back(value);
        }
        ASSERT_EQ(data, (std::vector<std::uint64_t> { 0, 1 }));
        cut.close();
    }
    
    TEST_F( ChangeLogIOStreamTest , testChangeLogIOSetStreamPos )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);

        std::vector<std::vector<std::uint64_t> > write_ops {
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 },
            { 1, 2, 3, 4, 5 },
            { 3, 4, 8 }
        };

        // Write some data
        CFile file(file_name, AccessType::READ_WRITE);        
        // saved stream positions
        std::vector<std::pair<std::uint64_t, std::uint64_t> > pos_buf;
        // corresponding change logs
        std::vector<std::vector<char> > change_logs;
        
        ChangeLogIOStream cut(file, 0, 4096);        
        int count = 0;
        for (int i = 0; i < 500; ++i) {
            for (auto op: write_ops) {
                if (count % 10 == 0 || count == 17 || count == 7) {
                    // position before write
                    pos_buf.push_back(cut.getStreamPos());
                }
                
                ChangeLogData data(op, false, false, false);
                const auto &change_log = cut.appendChangeLog(std::move(data));
                change_logs.emplace_back(change_log.sizeOf());
                std::memcpy(change_logs.back().data(), &change_log, change_log.sizeOf());
            }
            ++count;
        }
        // flush before we can read back
        cut.flush();
        
        // seek and compare results
        for (int i = 0; i < (int)pos_buf.size(); ++i) {
            cut.setStreamPos(pos_buf[i].first, pos_buf[i].second);
            auto changelog_ptr = cut.readChangeLogChunk();
            ASSERT_TRUE(changelog_ptr != nullptr);
            ASSERT_EQ(changelog_ptr->sizeOf(), change_logs[i].size());
            // binary compare
            ASSERT_TRUE(equal(change_logs[i].data(), change_logs[i].data() + change_logs[i].size(), (char*)changelog_ptr));
        }

        cut.close();
    }
    
}
