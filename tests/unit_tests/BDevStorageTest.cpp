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
    
    TEST_F( BDevStorageTest , testCanCreateEmptyDB0FileWithDefaultConfiguration )
    {         
        BDevStorage::create(file_name);
        ASSERT_TRUE(file_exists(file_name));
    }

    TEST_F( BDevStorageTest , testCanOpenEmptyDB0FileCreatedWithDefaultConfiguration )
    {
        BDevStorage::create(file_name);
        std::unique_ptr<BDevStorage> cut;
        ASSERT_NO_THROW(cut = std::make_unique<BDevStorage>(file_name, AccessType::READ_ONLY));
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
        std::uint64_t mutation_id;
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
            std::uint64_t state_num = 1;
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
            std::uint64_t state_num = 1;
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
        std::uint64_t state_num = 1;
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
            std::uint64_t state_num = 1;
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
        std::uint64_t state_num = 1;
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
            std::uint64_t state_num = 1;
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
        std::uint64_t state_num = 1;
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

}
