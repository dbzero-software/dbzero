// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestWorkspace.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/storage/PageStream.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class PageStreamTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "page-stream-test.io";
        static constexpr std::size_t page_size = 4096;

        virtual void SetUp() override
        {
            drop(file_name);
        }

        virtual void TearDown() override
        {
            drop(file_name);
        }

        static std::vector<std::byte> makePage(std::byte value)
        {
            return std::vector<std::byte>(page_size, value);
        }
    };

    TEST_F( PageStreamTest, testPageStreamAppendAndRead )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Page_IO page_io(0, file, page_size, page_size * 4, 0, 0, 2u, tail_function, 0);
        PageStream cut(page_io, 4);

        auto src = makePage(std::byte(17));
        bool is_first_page = false;
        auto page_num = cut.appendPage(src.data(), &is_first_page);
        ASSERT_EQ(0u, page_num);
        ASSERT_TRUE(is_first_page);
        cut.flush();

        auto read_buf = makePage(std::byte(0));
        page_io.read(page_num, read_buf.data());
        ASSERT_EQ(std::memcmp(src.data(), read_buf.data(), page_size), 0);

        auto reader = cut.getReader();
        std::uint64_t reader_page_num = 0;
        std::memset(read_buf.data(), 0, read_buf.size());
        ASSERT_TRUE(reader.readNext(read_buf.data(), &reader_page_num));
        ASSERT_EQ(page_num, reader_page_num);
        ASSERT_EQ(std::memcmp(src.data(), read_buf.data(), page_size), 0);
        ASSERT_FALSE(reader.readNext(read_buf.data()));
    }

    TEST_F( PageStreamTest, testPageStreamUsesSentinelControlPageWithoutHeader )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Page_IO page_io(0, file, page_size, page_size * 4, 0, 0, 2u, tail_function, 0);
        PageStream cut(page_io, 4);

        auto page = makePage(std::byte(7));
        ASSERT_EQ(0u, cut.appendPage(page.data()));
        ASSERT_EQ(1u, cut.appendPage(page.data()));
        cut.flush();

        std::vector<std::byte> read_buf(page_size);
        page_io.read(0, read_buf.data());
        ASSERT_EQ(std::memcmp(page.data(), read_buf.data(), page_size), 0);
        page_io.read(1, read_buf.data());
        ASSERT_EQ(std::memcmp(page.data(), read_buf.data(), page_size), 0);
    }

    TEST_F( PageStreamTest, testPageStreamClearReusesPreviousPages )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Page_IO page_io(0, file, page_size, page_size * 4, page_size * 4, 0, 2u, tail_function, 0);
        PageStream cut(page_io, 4);

        auto first = makePage(std::byte(1));
        auto second = makePage(std::byte(2));
        auto replacement = makePage(std::byte(3));

        ASSERT_EQ(4u, cut.appendPage(first.data()));
        ASSERT_EQ(5u, cut.appendPage(second.data()));
        cut.flush();
        auto size_before_clear = file.size();

        cut.clear();

        ASSERT_EQ(4u, cut.appendPage(replacement.data()));
        cut.flush();
        ASSERT_EQ(size_before_clear, file.size());

        auto read_buf = makePage(std::byte(0));
        page_io.read(4, read_buf.data());
        ASSERT_EQ(std::memcmp(replacement.data(), read_buf.data(), page_size), 0);

        auto reader = cut.getReader();
        ASSERT_TRUE(reader.readNext(read_buf.data()));
        ASSERT_EQ(std::memcmp(replacement.data(), read_buf.data(), page_size), 0);
        ASSERT_FALSE(reader.readNext(read_buf.data()));
    }

    TEST_F( PageStreamTest, testPageStreamExtendsAfterReusedTail )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        Page_IO page_io(0, file, page_size, page_size * 4, page_size * 4, 0, 2u, tail_function, 0);
        PageStream cut(page_io, 4);

        auto page = makePage(std::byte(1));
        ASSERT_EQ(4u, cut.appendPage(page.data()));
        ASSERT_EQ(5u, cut.appendPage(page.data()));
        ASSERT_EQ(6u, cut.appendPage(page.data()));
        ASSERT_EQ(8u, cut.appendPage(page.data()));
        cut.flush();
        cut.clear();

        ASSERT_EQ(4u, cut.appendPage(page.data()));
        ASSERT_EQ(5u, cut.appendPage(page.data()));
        ASSERT_EQ(6u, cut.appendPage(page.data()));
        ASSERT_EQ(8u, cut.appendPage(page.data()));
        cut.flush();

        auto reader = cut.getReader();
        auto read_buf = makePage(std::byte(0));
        std::uint64_t page_num = 0;
        std::vector<std::uint64_t> page_nums;
        while (reader.readNext(read_buf.data(), &page_num)) {
            page_nums.push_back(page_num);
        }
        ASSERT_EQ((std::vector<std::uint64_t> { 4, 5, 6, 8 }), page_nums);
    }

}
