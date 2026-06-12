// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <utils/TestWorkspace.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/storage/Page_IO.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class Page_IOTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-file.io";
        static constexpr std::size_t page_size = 4096;

        virtual void SetUp() override {
            drop(file_name);
        }

        virtual void TearDown() override {    
            drop(file_name);
        }
    };
    
    TEST_F( Page_IOTest, testPage_IOAppendMultiple )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        auto header_size = 128;
        auto block_size = page_size * 2;
        auto address = header_size;
        auto page_count = 0;
        auto block_num = 0;
        // 4 blocks in a single step
        auto step_size = 4;
        
        db0::Page_IO cut(header_size, file, page_size, block_size, address, page_count,
            step_size, tail_function, block_num
        );
        
        std::vector<char> buf(16 * page_size);
        memset(buf.data(), 0, buf.size());
        
        ASSERT_EQ(cut.getNextPageNum().first, 0);
        ASSERT_EQ(cut.getCurrentStepRemainingPages(), 8);
        cut.append(buf.data(), 3);
        ASSERT_EQ(cut.getCurrentStepRemainingPages(), 5);
        ASSERT_THROW(cut.append(buf.data(), 6), db0::InternalException);
        ASSERT_EQ(cut.getCurrentStepRemainingPages(), 5);
        cut.append(buf.data(), 5);
        ASSERT_EQ(cut.getNextPageNum().first, 8);
    }

    TEST_F( Page_IOTest, testPage_IOReserveWithinSingleBlockStep )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        auto block_size = page_size * 8;
        db0::Page_IO cut(0, file, page_size, block_size, 0, 0, 1u, tail_function, 0);

        ASSERT_EQ(0u, cut.reserve(4));
        ASSERT_EQ(4u, cut.reserve(4));
        ASSERT_EQ(8u, cut.reserve(4));
    }

    TEST_F( Page_IOTest, testReservePoolTracksContiguousStrides )
    {
        db0::ReservePool cut;

        cut.add(10, 2);
        cut.add(12, 3);
        cut.add(20, 1);

        ASSERT_FALSE(cut.empty());
        ASSERT_EQ((std::make_pair<std::uint64_t, std::uint32_t>(10, 5)), cut.next());
        ASSERT_EQ(10u, cut.tryPop(3).value());
        ASSERT_EQ((std::make_pair<std::uint64_t, std::uint32_t>(13, 2)), cut.next());
        ASSERT_FALSE(cut.tryPop(3).has_value());
        ASSERT_EQ(13u, cut.tryPop(2).value());
        ASSERT_EQ((std::make_pair<std::uint64_t, std::uint32_t>(20, 1)), cut.next());
        ASSERT_EQ(20u, cut.pop());
        ASSERT_TRUE(cut.empty());
    }

    TEST_F( Page_IOTest, testPage_IOReserveSkippedPagesAreReusedByAppend )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        auto block_size = page_size * 2;
        db0::Page_IO cut(0, file, page_size, block_size, 0, 0, 2u, tail_function, 0);

        ASSERT_EQ(0u, cut.reserve(1));
        ASSERT_EQ(4u, cut.reserve(4));

        std::vector<char> write_buf(page_size * 2, 'x');
        ASSERT_EQ(1u, cut.append(write_buf.data(), 2));

        std::vector<char> read_buf(page_size * 2, 0);
        cut.read(1, read_buf.data(), 2);
        ASSERT_EQ(write_buf, read_buf);
        ASSERT_EQ(3u, cut.append(write_buf.data()));
        ASSERT_EQ(8u, cut.append(write_buf.data()));
    }

    TEST_F( Page_IOTest, testPage_IOAppendMultipleDoesNotSplitReservePool )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        auto block_size = page_size * 2;
        db0::Page_IO cut(0, file, page_size, block_size, 0, 0, 2u, tail_function, 0);

        ASSERT_EQ(0u, cut.reserve(1));
        ASSERT_EQ(4u, cut.reserve(4));

        std::vector<char> write_buf(page_size * 4, 'x');
        ASSERT_EQ(8u, cut.append(write_buf.data(), 4));
        ASSERT_EQ(1u, cut.getNextPageNum().first);
    }

    TEST_F( Page_IOTest, testPage_IOPreservesFirstPageFlag )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        auto block_size = page_size * 2;
        db0::Page_IO cut(0, file, page_size, block_size, 0, 0, 2u, tail_function, 0);

        bool is_first_page = false;
        ASSERT_EQ(0u, cut.getNextPageNum(&is_first_page).first);
        ASSERT_TRUE(is_first_page);

        std::vector<char> write_buf(page_size, 'x');
        ASSERT_EQ(0u, cut.reserve(4));

        is_first_page = false;
        ASSERT_EQ(4u, cut.append(write_buf.data(), &is_first_page));
        ASSERT_TRUE(is_first_page);
    }

    TEST_F( Page_IOTest, testPage_IOReserveSkippedPagesAreForgottenAfterReopen )
    {
        CFile::create(file_name, {});
        std::uint64_t end_page_num = 0;
        {
            CFile file(file_name, AccessType::READ_WRITE);
            auto tail_function = [&file]() -> std::uint64_t {
                return file.size();
            };

            auto block_size = page_size * 2;
            db0::Page_IO cut(0, file, page_size, block_size, 0, 0, 2u, tail_function, 0);

            ASSERT_EQ(0u, cut.reserve(3));
            ASSERT_EQ(4u, cut.reserve(2));
            end_page_num = cut.getEndPageNum();
        }

        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        auto block_size = page_size * 2;
        db0::Page_IO reopened(0, file, page_size, block_size, 4 * page_size, 2, 2u, tail_function, 0);
        ASSERT_EQ(6u, end_page_num);

        std::vector<char> write_buf(page_size, 'x');
        ASSERT_EQ(6u, reopened.append(write_buf.data()));
    }

}
