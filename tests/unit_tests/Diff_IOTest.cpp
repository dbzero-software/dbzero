// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
    
    class Diff_IOProxy: public Diff_IO
    {
    public:
        Diff_IOProxy(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address,
            std::uint32_t page_count, std::function<std::uint64_t()> tail_function)
            : Diff_IO(header_size, file, page_size, block_size, address, page_count, 1u, tail_function)
        {
        }

        std::pair<std::uint64_t, std::uint32_t> getNextPageNum() {
            return Page_IO::getNextPageNum();
        }
    };

    class Diff_IOTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-file.io";
        static constexpr std::size_t page_size = 4096;

        virtual void SetUp() override 
        {
            m_dp_0 = std::vector<std::byte>(page_size, std::byte(0));
            m_dp_1 = m_dp_0;
            m_dp_2 = m_dp_0;
            std::memset(m_dp_1.data() + 18, 1, 32);
            std::memset(m_dp_1.data() + 94, 2, 11);
            std::memset(m_dp_1.data() + 1200, 3, 120);
            
            // buffer position + size (100 values)
            std::vector<std::pair<unsigned int, unsigned int>> positions = {
                {0, 10}, {20, 5}, {30, 15}, {50, 8}, {60, 12},
                {75, 7}, {85, 9}, {95, 6}, {105, 11}, {120, 13},
                {135, 14}, {150, 10}, {165, 5}, {175, 15}, {190, 8},
                {200, 12}, {215, 7}, {225, 9}, {235, 6}, {245, 11},
                {260, 13}, {275, 14}, {290, 10}, {305, 5}, {315, 15},
                {330, 8}, {340, 12}, {355, 7}, {365, 9}, {375, 6},
                {385, 11}, {400, 13}, {415, 14}, {430, 10}, {445, 5},
                {455, 15}, {470, 8}, {480, 12}, {495, 7}, {505, 9},
                {515, 6}, {525, 11}, {540, 13}, {555, 14}, {570, 10},
                {585, 5}, {595, 15}, {610, 8}, {620, 12}, {635, 7},
                {645, 9}, {655, 6}, {665, 11}, {680, 13}, {695, 14},
                {710, 10}, {725, 5}, {735, 15}, {750, 8}, {760, 12},
                {775, 7}, {785, 9}, {795, 6}, {805, 11}, {820, 13},
                {835, 14}, {850, 10}, {865, 5}, {875, 15}, {890, 8},
                {900, 12}, {915, 7}, {925, 9}, {935, 6}, {945, 11},
                {960, 13}, {975, 14}, {990, 10}, {1005, 5}, {1015, 15},
                {1030, 8}, {1040, 12}, {1055, 7}, {1065, 9}, {1075, 6},
                {1085, 11}, {1100, 13}, {1115, 14}, {1130, 10}, {1145, 5},
                {1155, 15}, {1170, 8}, {1180, 12}, {1195, 7}, {1205, 9},
                {1215, 6}, {1225, 11}, {1240, 13}, {1255, 14}, {1270, 10}
            };
            
            unsigned int c = 1;
            for (auto &item: positions) {
                std::memset(m_dp_2.data() + item.first, c, item.second);
                ++c;
            }

            drop(file_name);
        }

        virtual void TearDown() override {    
            drop(file_name);
        }

    protected:
        std::vector<std::byte> m_dp_0;
        std::vector<std::byte> m_dp_1;
        std::vector<std::byte> m_dp_2;
    };
    
    TEST_F( Diff_IOTest , testDiff_IOSimpleAppendDiff )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Diff_IOProxy cut(0, file, page_size, page_size * 16, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);

        auto page_num = cut.appendDiff(m_dp_1.data(), {0, 0}, diff_buf).first;
        ASSERT_EQ(0, page_num);
    }
    
    TEST_F( Diff_IOTest , testDiff_IOAppendMultiplePageDiff )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Diff_IOProxy cut(0, file, page_size, page_size * 16, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);

        std::uint64_t last_page_num = 0;
        for (unsigned int i = 0; i < 100; ++i) {
            last_page_num = cut.appendDiff(m_dp_1.data(), {i, i}, diff_buf).first;
        }
        ASSERT_TRUE(last_page_num > 0);
    }

    TEST_F( Diff_IOTest , testDiff_IOAppendMultipleBlocksDiff )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        // block size set to "2" so that it overflows quickly
        Diff_IOProxy cut(0, file, page_size, page_size * 2, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);

        std::uint64_t last_page_num = 0;
        for (unsigned int i = 0; i < 100; ++i) {
            last_page_num = cut.appendDiff(m_dp_1.data(), {i, i}, diff_buf).first;
        }
        ASSERT_TRUE(last_page_num > 0);
    }

    TEST_F( Diff_IOTest , testDiff_IOApplyFrom )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        // block size set to "2" so that it overflows quickly
        Diff_IOProxy cut(0, file, page_size, page_size * 2, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_0.data(), m_dp_1.data(), page_size, diff_buf);
        
        std::map<std::uint64_t, std::uint64_t> diff_map;
        for (unsigned int i = 0; i < 100; ++i) {            
            auto page_num = cut.appendDiff(m_dp_1.data(), {i, i}, diff_buf).first;
            diff_map[i] = page_num;
        }
        // must flush before performing any reads
        cut.flush();

        for (auto &item : diff_map) {
            // use old version as a base
            auto dp = m_dp_0;
            cut.applyFrom(item.second, dp.data(), {item.first, item.first});
            // make sure the diffs were applied correctly
            ASSERT_EQ(std::memcmp(m_dp_1.data(), dp.data(), page_size), 0);
        }
    }
    
    TEST_F( Diff_IOTest , testDiff_IOAppendDiffWithOverflow )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        // block size set to "2" so that it overflows quickly
        Diff_IOProxy cut(0, file, page_size, page_size * 2, 0, 0, tail_function);        
        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(m_dp_1.data(), m_dp_2.data(), page_size, diff_buf);
        
        for (unsigned int i = 0; i < 250; ++i) {
            auto [page_num, overflow] = cut.appendDiff(m_dp_2.data(), {i, i}, diff_buf);
            // appendDiff must return the first page written to and the number of pages
            ASSERT_EQ(page_num + (overflow ? 1 : 0), cut.getNextPageNum().first);
        }
        cut.flush();
    }
    
}