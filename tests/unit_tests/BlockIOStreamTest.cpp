// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <utils/TestWorkspace.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/memory/BitSpace.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class BlockIOStreamTest: public testing::Test 
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

    void readAll(BlockIOStream &io) {
        std::vector<char> buffer;        
        for (;;) {
            if (!io.readChunk(buffer)) {
                break;
            }
        }
    }

    TEST_F( BlockIOStreamTest , testCanAppendDataToBlockIOStream )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        {
            BlockIOStream cut(file, 0, 4096);
            cut.addChunk(11);
            cut.appendToChunk("hello world", 11);
            cut.close();
        }
        
        // read the chunk
        BlockIOStream cut(file, 0, 4096, {}, AccessType::READ_ONLY);
        std::vector<char> buffer(11);
        auto size = cut.readChunk(buffer);
        ASSERT_EQ(size, 11);
        ASSERT_EQ(std::string(buffer.data(), buffer.size()), "hello world");
    }

    TEST_F( BlockIOStreamTest, testThrowsOnExpectedChunkSizeMismatch )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        {
            BlockIOStream cut(file, 0, 4096);
            cut.addChunk(11);
            cut.appendToChunk("hello world", 11);
            cut.close();
        }
        
        // read the chunk
        BlockIOStream cut(file, 0, 4096, {}, AccessType::READ_ONLY);
        std::vector<char> buffer;
        ASSERT_ANY_THROW(cut.readChunk(buffer, 10));
    }
    
    TEST_F( BlockIOStreamTest, testThrowsIfAppendingToUnprocessedStream )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        {
            BlockIOStream cut(file, 0, 4096);
            cut.addChunk(11);
            cut.appendToChunk("hello world", 11);
            cut.close();
        }
        
        BlockIOStream cut(file, 0, 4096);
        // should throw because not at the end of stream
        ASSERT_ANY_THROW(cut.addChunk(11));
        cut.close();
    }

    TEST_F( BlockIOStreamTest, testCanAppendToExistingStream )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        {
            BlockIOStream cut(file, 0, 4096);
            cut.addChunk(11);
            cut.appendToChunk("hello world", 11);
            cut.close();
        }
        
        // append chunk to existing stream
        {
            BlockIOStream cut(file, 0, 4096);
            readAll(cut);
            cut.addChunk(11);
            cut.appendToChunk("hello world", 11);
            cut.close();
        }

        // read the chunk
        BlockIOStream cut(file, 0, 4096, {}, AccessType::READ_ONLY);
        std::vector<char> buffer;
        std::size_t size = 0;
        int count = 0;
        while ((size = cut.readChunk(buffer)) > 0) {
            ASSERT_EQ(size, 11);
            ASSERT_EQ(std::string(buffer.data(), buffer.size()), "hello world");
            ++count;
        }
        ASSERT_EQ(count, 2);
    }

    TEST_F( BlockIOStreamTest, testCanAppendChunksLargerThanBlock )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);
        auto page = randomPage(5099);
        {
            BlockIOStream cut(file, 0, 4096);            
            cut.addChunk(page.size());
            cut.appendToChunk(page.data(), page.size());
            cut.close();
        }

        BlockIOStream cut(file, 0, 4096, {}, AccessType::READ_ONLY);
        std::vector<char> buffer;
        cut.readChunk(buffer, page.size());
        ASSERT_TRUE(equal(buffer, page));
    }

    TEST_F( BlockIOStreamTest, testMultipleStreamsCanBeWrittenToOneFile )
    {
        srand(9347914u);
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);
        BlockIOStream *str_1 = nullptr, *str_2 = nullptr;
        auto tail_function = [&]() {
            return std::max(str_1->tail(), str_2->tail());
        };

        std::vector<std::vector<char> > pages;
        auto block_size = 4096;
        auto begin_0 = 0;
        auto begin_1 = block_size;
        {
            BlockIOStream io_1(file, begin_0, block_size, tail_function);
            str_1 = &io_1;
            BlockIOStream io_2(file, begin_1, block_size, tail_function);
            str_2 = &io_2;
            for (int i = 0; i < 100; ++i) {
                auto size = rand() % 1000 + 1;
                auto io = ((i % 2 == 0)?str_1:str_2);
                pages.push_back(randomPage(size));
                io->addChunk(size);
                io->appendToChunk(pages.back().data(), size);
            }
            io_1.close();
            io_2.close();
        }

        // open stream #2 and validate
        BlockIOStream cut(file, begin_1, block_size, tail_function, AccessType::READ_ONLY);
        std::vector<char> buffer;
        int i = 1;
        std::size_t chunk_size = 0;
        while ((chunk_size = cut.readChunk(buffer)) > 0) {
            ASSERT_TRUE(equal(buffer.data(), buffer.data() + chunk_size, pages[i]));
            i += 2;
        }
    }

    TEST_F( BlockIOStreamTest, testReaderCanAccessChunksWrittenByOtherInstanceWithoutClosing )
    {    
        // list of chunks to be created
        std::vector<std::pair<std::size_t, char> > chunk_data {
            { 10, 'a' },
            { 20, 'b' },
            { 30, 'c' },
            { 40, 'd' },
            { 50, 'e' },
            { 60, 'f' },
            { 70, 'g' },
            { 80, 'h' },
            { 90, 'i' },
            { 100, 'j' },
        };
        
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        // output stream
        BlockIOStream out(file, 0, 4096);
        std::vector<std::pair<std::size_t, char> > in_chunks;
        bool data_valid = true;
        std::thread reader([&]() {
            // open as read-only
            CFile file(file_name, AccessType::READ_ONLY);
            // tail function not required in read-only mode
            BlockIOStream in(file, 0, 4096, {}, AccessType::READ_ONLY);

            std::vector<char> buffer(4096);
            unsigned int chunk_count = 0;
            // try until all expected chunks are retrieved
            while (chunk_count < chunk_data.size()) {                
                auto chunk_size = in.readChunk(buffer, 0);
                if (!chunk_size) {
                    // try again after sleep & refresh
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    in.refresh();
                    continue;
                }
                
                ++chunk_count;
                in_chunks.emplace_back(chunk_size, buffer.data()[0]);
                // validate all bytes in the chunk are same (this is what the other instance has written)
                for (auto i = 1u; i < chunk_size; ++i) {
                    if (buffer.data()[i] != buffer.data()[0]) {
                        data_valid = false;                            
                    }
                }
            }
        });
        
        // sleep before first write (so that the reader can start from empty stream)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // append chunks according to their definitions
        int chunk_index = 0;
        for (auto &chunk : chunk_data) {
            out.addChunk(chunk.first);
            out.appendToChunk(std::vector<char>(chunk.first, chunk.second).data(), chunk.first);
            if (chunk_index % 2 == 0) {
                // flush every second chunk
                out.flush();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            ++chunk_index;
        }

        out.close();
        reader.join();
    }
    
    void testReaderCanAccessChunksWrittenInMultipleCycles(BlockIOStreamTest &self, bool checksums)
    {
        // list of chunks to be created
        std::vector<std::pair<std::size_t, char> > chunk_data {
            { 10, 'a' },
            { 20, 'b' },
            { 30, 'c' },
            { 40, 'd' },
            { 50, 'e' },
            { 60, 'f' },
            { 70, 'g' },
            { 80, 'h' },
            { 90, 'i' },
            { 100, 'j' },
        };
        
        std::vector<char> no_data;
        CFile::create(self.file_name, no_data);
        CFile file(self.file_name, AccessType::READ_WRITE);

        // Create empty file first
        {
            BlockIOStream out(file, 0, 4096, {}, AccessType::READ_WRITE, checksums);
            out.flush();
            out.close();
        }
        
        std::vector<std::pair<std::size_t, char> > in_chunks;
        bool data_valid = true;
        std::thread reader([&]() {
            // open as read-only
            CFile file(self.file_name, AccessType::READ_ONLY);
            // tail function not required in read-only mode
            BlockIOStream in(file, 0, 4096, {}, AccessType::READ_ONLY, checksums);

            std::vector<char> buffer(4096);
            unsigned int chunk_count = 0;
            // try until all expected chunks are retrieved
            while (chunk_count < chunk_data.size()) {                
                auto chunk_size = in.readChunk(buffer, 0);
                if (!chunk_size) {
                    // try again after sleep & refresh
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    in.refresh();
                    continue;
                }
                
                ++chunk_count;
                in_chunks.emplace_back(chunk_size, buffer.data()[0]);
                // validate all bytes in the chunk are same (this is what the other instance has written)
                for (auto i = 1u; i < chunk_size; ++i) {
                    if (buffer.data()[i] != buffer.data()[0]) {
                        data_valid = false;                            
                    }
                }
            }
        });
        
        // append chunks in multiple open/close cycles
        for (auto &chunk : chunk_data) {
            BlockIOStream out(file, 0, 4096, {}, AccessType::READ_WRITE, checksums);
            // read until end of stream           
            readAll(out);
            out.addChunk(chunk.first);
            out.appendToChunk(std::vector<char>(chunk.first, chunk.second).data(), chunk.first);            
            out.flush();
            out.close();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        reader.join();
    }
    
    TEST_F( BlockIOStreamTest, testReaderCanAccessChunksWrittenInMultipleCycles )
    {
        testReaderCanAccessChunksWrittenInMultipleCycles(*this, false);
    }
    
    TEST_F( BlockIOStreamTest, testReaderCanAccessChunksWrittenInMultipleCyclesWithChecksums )
    {
        testReaderCanAccessChunksWrittenInMultipleCycles(*this, true);
    }

    TEST_F( BlockIOStreamTest, testCanSaveAndThenRestoreStateWhenAppending )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        BlockIOStream cut(file, 0, 4096);
        std::pair<std::uint64_t, std::uint64_t> stream_pos;
        // append some chunks, and remmber stream pos at chunk #8
        for (int i = 0; i < 10; ++i) {
            if (i == 7) {
                stream_pos = cut.getStreamPos();
                cut.addChunk(11);
                cut.appendToChunk("hello world", 11);
            } else {
                auto page = randomPage(3189); 
                cut.addChunk(page.size());
                cut.appendToChunk(page.data(), page.size());
            }                         
        }
        
        BlockIOStream::State state;
        cut.flush();
        cut.saveState(state);
        
        // try reading chunk at the stored position
        cut.setStreamPos(stream_pos.first, stream_pos.second);
        std::vector<char> buffer;
        auto size = cut.readChunk(buffer);
        ASSERT_EQ(size, 11);
        ASSERT_EQ(std::string(buffer.data(), buffer.size()), "hello world");

        // restore state and continue appending
        cut.restoreState(state);
        ASSERT_TRUE(cut.eos());
        for (int i = 0; i < 3; ++i) {
            auto page = randomPage(3189); 
            cut.addChunk(page.size());
            cut.appendToChunk(page.data(), page.size());
        }
        cut.close();
    }
    
    TEST_F( BlockIOStreamTest, testCanSetStreamPosHead )
    {
        std::vector<char> no_data;
        CFile::create(file_name, no_data);
        CFile file(file_name, AccessType::READ_WRITE);

        BlockIOStream cut(file, 0, 4096);
        cut.addChunk(11);
        cut.appendToChunk("hello world", 11);

        for (int i = 0; i < 10; ++i) {
            auto page = randomPage(3189); 
            cut.addChunk(page.size());
            cut.appendToChunk(page.data(), page.size());            
        }

        // position at head and try reading the 1st chunk
        cut.setStreamPosHead();
        std::vector<char> buffer;
        auto size = cut.readChunk(buffer);
        ASSERT_EQ(size, 11);
        ASSERT_EQ(std::string(buffer.data(), buffer.size()), "hello world");
        cut.close();
    }

}