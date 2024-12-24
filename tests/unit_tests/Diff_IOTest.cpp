#include <gtest/gtest.h>
#include <sys/stat.h>
#include <utils/TestWorkspace.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class Diff_IOTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-file.io";
        static constexpr std::size_t page_size = 4096;

        virtual void SetUp() override 
        {
            m_dp_0 = std::vector<std::byte>(page_size, std::byte(0));
            m_dp_1 = m_dp_0;
            std::memset(m_dp_1.data() + 18, 1, 32);
            std::memset(m_dp_1.data() + 94, 2, 11);
            std::memset(m_dp_1.data() + 1200, 3, 120);

            drop(file_name);
        }

        virtual void TearDown() override {            
            drop(file_name);
        }

    protected:
        std::vector<std::byte> m_dp_0;
        std::vector<std::byte> m_dp_1;
    };
    
    TEST_F( Diff_IOTest , testDiff_IOSimpleAppend )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Diff_IO cut(0, file, page_size, page_size * 16, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);

        auto page_num = cut.append(m_dp_1.data(), {0, 0}, diff_buf);
        ASSERT_EQ(0, page_num);
    }
    
    TEST_F( Diff_IOTest , testDiff_IOAppendMultiplePages )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Diff_IO cut(0, file, page_size, page_size * 16, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);

        std::uint64_t last_page_num = 0;
        for (unsigned int i = 0; i < 100; ++i) {
            last_page_num = cut.append(m_dp_1.data(), {i, i}, diff_buf);
        }
        ASSERT_TRUE(last_page_num > 0);
    }

    TEST_F( Diff_IOTest , testDiff_IOAppendMultipleBlocks )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        // block size set to "2" so that it overflows quickly
        Diff_IO cut(0, file, page_size, page_size * 2, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);

        std::uint64_t last_page_num = 0;
        for (unsigned int i = 0; i < 100; ++i) {
            last_page_num = cut.append(m_dp_1.data(), {i, i}, diff_buf);
        }
        ASSERT_TRUE(last_page_num > 0);
    }

    TEST_F( Diff_IOTest , testDiff_IOApply )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        // block size set to "2" so that it overflows quickly
        Diff_IO cut(0, file, page_size, page_size * 2, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);
        
        std::map<std::uint64_t, std::uint64_t> diff_map;
        for (unsigned int i = 0; i < 100; ++i) {            
            auto page_num = cut.append(m_dp_1.data(), {i, i}, diff_buf);
            diff_map[i] = page_num;
        }
        // must flush before performing any reads
        cut.flush();

        for (auto &item : diff_map) {
            // use old version as a base
            auto dp = m_dp_0;
            cut.read(item.second, dp.data(), {item.first, item.first});
            // make sure the diffs were applied correctly
            ASSERT_EQ(std::memcmp(m_dp_1.data(), dp.data(), page_size), 0);
        }
    }

}