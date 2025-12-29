// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/storage/MetaIOStream.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class MetaIOStreamTest: public testing::Test
    {
    public:
        static constexpr const char *file_name_1 = "test_meta.stream";
        static constexpr const char *file_name_2 = "test_block_1.stream";

        virtual void SetUp() override 
        { 
            drop(file_name_1);
            drop(file_name_2);
        }

        virtual void TearDown() override 
        { 
            drop(file_name_1);
            drop(file_name_2);
        }
    };
    
    TEST_F( MetaIOStreamTest , testMetaIOStreamCanBeCreated )
    {
        std::vector<char> no_data;
        CFile::create(file_name_1, no_data);
        CFile file(file_name_1, AccessType::READ_WRITE);        
        MetaIOStream cut(file, {}, 0, 4096);
        ASSERT_TRUE(cut.eos());
        cut.close();        
    }

    TEST_F( MetaIOStreamTest , testMetaIOStreamNoWriteBeforeStepLimitReached )
    {
        std::vector<char> no_data;
        CFile::create(file_name_1, no_data);
        CFile::create(file_name_2, no_data);

        CFile meta_file(file_name_1, AccessType::READ_WRITE);        
        CFile block_file(file_name_2, AccessType::READ_WRITE);
        
        BlockIOStream block_stream(block_file, 0, 4096, {}, AccessType::READ_WRITE);
        std::vector<BlockIOStream*> managed_streams = { &block_stream };

        MetaIOStream cut(meta_file, managed_streams, 0, 4096);
        auto tell_0 = cut.tell();
        // no append because the managed stream was not written to
        cut.checkAndAppend(0);
        ASSERT_EQ(cut.tell(), tell_0);
        cut.close();
        block_stream.close();
    }
    
    TEST_F( MetaIOStreamTest , testMetaIOLowerBound )
    {
        std::vector<char> no_data;
        CFile::create(file_name_1, no_data);
        CFile::create(file_name_2, no_data);

        CFile meta_file(file_name_1, AccessType::READ_WRITE);
        CFile block_file(file_name_2, AccessType::READ_WRITE);
        
        BlockIOStream block_stream(block_file, 0, 4096, {}, AccessType::READ_WRITE);
        std::vector<BlockIOStream*> managed_streams = { &block_stream };
        
        MetaIOStream cut(meta_file, managed_streams, 0, 4096, {}, AccessType::READ_WRITE, false, 128);
        for (int i = 0; i < 100; i += 2) {
            // write to the managed stream
            std::vector<char> data = randomPage(250);
            block_stream.addChunk(data.size());
            block_stream.appendToChunk(data.data(), data.size());
            cut.checkAndAppend(i);
        }

        cut.flush();
        std::vector<char> buffer;
        auto meta_log_ptr = cut.lowerBound(7, buffer);
        ASSERT_EQ(meta_log_ptr->m_state_num, 6);

        meta_log_ptr = cut.lowerBound(98, buffer);
        ASSERT_EQ(meta_log_ptr->m_state_num, 98);

        meta_log_ptr = cut.lowerBound(99, buffer);
        ASSERT_EQ(meta_log_ptr->m_state_num, 98);

        meta_log_ptr = cut.lowerBound(150, buffer);
        ASSERT_EQ(meta_log_ptr->m_state_num, 98);
        
        cut.close();
        block_stream.close();
    }
    
}