// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <utils/TestWorkspace.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/memory/diff_utils.hpp>
#include <dbzero/core/storage/Page_IO.hpp>
#include <dbzero/core/storage/RandomIO_Stream.hpp>

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

        static std::vector<std::byte> makePage(std::size_t size, std::byte value)
        {
            return std::vector<std::byte>(size, value);
        }
    };

    class RandomIO_StreamDiffIO: public Diff_IO
    {
    public:
        RandomIO_StreamDiffIO(CFile &file, std::uint32_t page_size, std::uint32_t block_size,
            std::function<std::uint64_t()> tail_function)
            : Diff_IO(0, file, page_size, block_size, 0, 0, 1u, tail_function, 0)
        {
        }
    };

    class TestRandomIO_Stream: public RandomIO_Stream
    {
    public:
        using RandomIO_Stream::RandomIO_Stream;
        using RandomIO_Stream::getPageNum;
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

    TEST_F( Page_IOTest, testRandomIO_StreamAppendsLargePagesOverSmallPageIO )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(5), page_size * 2);

        auto first = makePage(cut.getPageSize(), std::byte(1));
        auto second = makePage(cut.getPageSize(), std::byte(2));
        auto third = makePage(cut.getPageSize(), std::byte(3));

        bool is_first_page = false;
        ASSERT_EQ(0u, cut.append(first.data(), &is_first_page));
        ASSERT_TRUE(is_first_page);
        ASSERT_EQ(2u, cut.append(second.data()));
        ASSERT_EQ(5u, cut.append(third.data()));
        cut.flush();

        std::vector<std::byte> read_buf(cut.getPageSize());
        cut.readRandom(0, read_buf.data());
        ASSERT_EQ(first, read_buf);
        cut.readRandom(2, read_buf.data());
        ASSERT_EQ(second, read_buf);
        cut.readRandom(5, read_buf.data());
        ASSERT_EQ(third, read_buf);

    }

    TEST_F( Page_IOTest, testRandomIO_StreamClearReusesLargePageBlocks )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(5), page_size * 2);

        auto first = makePage(cut.getPageSize(), std::byte(1));
        auto second = makePage(cut.getPageSize(), std::byte(2));
        auto replacement = makePage(cut.getPageSize(), std::byte(9));

        ASSERT_EQ(0u, cut.append(first.data()));
        ASSERT_EQ(2u, cut.append(second.data()));
        cut.flush();
        auto size_before_clear = file.size();

        cut.clear();
        ASSERT_EQ(0u, cut.append(replacement.data()));
        cut.flush();
        ASSERT_EQ(size_before_clear, file.size());

        std::vector<std::byte> read_buf(cut.getPageSize());
        cut.readRandom(0, read_buf.data());
        ASSERT_EQ(replacement, read_buf);

        cut.readRandom(0, read_buf.data());
        ASSERT_EQ(replacement, read_buf);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamForwardsRandomAccessWithPageSizeTranslation )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(5), page_size * 2);

        auto page = makePage(cut.getPageSize(), std::byte(4));
        auto page_num = cut.append(page.data());
        cut.flush();

        auto replacement = makePage(cut.getPageSize(), std::byte(8));
        replacement[page_size - 1] = std::byte(0xaa);
        replacement[page_size] = std::byte(0xbb);
        cut.writeRandom(page_num, replacement.data());
        
        std::vector<std::byte> read_buf(cut.getPageSize());
        cut.readRandom(page_num, read_buf.data());
        ASSERT_EQ(replacement, read_buf);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamRandomAccessIsIndependentOfClear )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(3));

        auto first = makePage(page_size, std::byte(1));
        auto random_replacement = makePage(page_size, std::byte(7));
        auto stream_replacement = makePage(page_size, std::byte(9));

        ASSERT_EQ(0u, cut.append(first.data()));
        cut.flush();

        auto random_page_num = page_io.reserve(1);
        cut.writeRandom(random_page_num, random_replacement.data());

        cut.clear();

        std::vector<std::byte> read_buf(page_size);
        cut.readRandom(random_page_num, read_buf.data());
        ASSERT_EQ(random_replacement, read_buf);

        ASSERT_EQ(0u, cut.append(stream_replacement.data()));
        cut.flush();

        cut.readRandom(0, read_buf.data());
        ASSERT_EQ(stream_replacement, read_buf);

        cut.readRandom(random_page_num, read_buf.data());
        ASSERT_EQ(random_replacement, read_buf);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamAppendRandomDoesNotAffectManagedStream )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(3));

        auto stream_first = makePage(page_size, std::byte(1));
        auto random_page = makePage(page_size, std::byte(7));
        auto stream_second = makePage(page_size, std::byte(2));

        ASSERT_EQ(0u, cut.append(stream_first.data()));
        cut.flush();

        auto random_page_num = cut.appendRandom(random_page.data());
        ASSERT_EQ(3u, random_page_num);

        std::vector<std::byte> read_buf(page_size);
        cut.readRandom(random_page_num, read_buf.data());
        ASSERT_EQ(random_page, read_buf);

        cut.readRandom(0, read_buf.data());
        ASSERT_EQ(stream_first, read_buf);

        ASSERT_EQ(1u, cut.append(stream_second.data()));
        cut.flush();

        cut.readRandom(0, read_buf.data());
        ASSERT_EQ(stream_first, read_buf);
        cut.readRandom(1, read_buf.data());
        ASSERT_EQ(stream_second, read_buf);

        cut.readRandom(random_page_num, read_buf.data());
        ASSERT_EQ(random_page, read_buf);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamReadRandomCanAccessStreamAppends )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(13), page_size * 4);

        auto full_page = makePage(cut.getPageSize(), std::byte(4));
        auto full_page_num = cut.append(full_page.data());
        cut.flush();

        std::vector<std::byte> read_buf(cut.getPageSize());
        cut.readRandom(full_page_num, read_buf.data());
        ASSERT_EQ(full_page, read_buf);

        auto base_page = makePage(cut.getPageSize(), std::byte(0));
        auto changed_page = base_page;
        std::memset(changed_page.data() + 17, 0x11, 120);
        std::memset(changed_page.data() + page_size * 2 + 31, 0x22, 300);

        std::vector<std::uint16_t> diff_buf;
        ASSERT_TRUE(db0::getDiffs(base_page.data(), changed_page.data(), cut.getPageSize(), diff_buf));

        auto [diff_page_num, overflow] = cut.appendDiff(changed_page.data(), {11, 7}, diff_buf);
        ASSERT_FALSE(overflow);
        cut.flush();

        cut.readRandom(diff_page_num, read_buf.data());
        ASSERT_NE(base_page, read_buf);
        auto result = base_page;
        cut.applyFrom(diff_page_num, result.data(), {11, 7});
        ASSERT_EQ(changed_page, result);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamAppendDiffApplies16KBPages )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(13), page_size * 4);

        auto base_page = makePage(cut.getPageSize(), std::byte(0));
        auto changed_page = base_page;
        std::memset(changed_page.data() + 123, 0x11, 120);
        std::memset(changed_page.data() + page_size + 31, 0x22, 300);
        std::memset(changed_page.data() + page_size * 3 + 17, 0x33, 80);

        std::vector<std::uint16_t> diff_buf;
        ASSERT_TRUE(db0::getDiffs(base_page.data(), changed_page.data(), cut.getPageSize(), diff_buf));

        bool is_first_page = false;
        auto [page_num, overflow] = cut.appendDiff(changed_page.data(), {7, 3}, diff_buf, &is_first_page);
        ASSERT_EQ(0u, page_num);
        ASSERT_FALSE(overflow);
        ASSERT_TRUE(is_first_page);

        auto result = base_page;
        cut.applyFrom(page_num, result.data(), {7, 3});
        ASSERT_EQ(changed_page, result);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamAppendDiffWithOverflowApplies16KBPages )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        RandomIO_Stream cut(page_io, std::uint32_t(9), page_size * 4);

        auto base_page = makePage(cut.getPageSize(), std::byte(0));
        auto changed_page = base_page;
        for (std::size_t i = 0; i < changed_page.size(); i += 2) {
            changed_page[i] = std::byte(0x7f);
        }

        std::vector<std::uint16_t> diff_buf;
        ASSERT_TRUE(db0::getDiffs(base_page.data(), changed_page.data(), cut.getPageSize(), diff_buf,
            cut.getPageSize() * 2));

        auto [page_num, overflow] = cut.appendDiff(changed_page.data(), {19, 5}, diff_buf);
        ASSERT_EQ(0u, page_num);
        ASSERT_TRUE(overflow);

        auto result = base_page;
        cut.applyFrom(page_num, result.data(), {19, 5});
        ASSERT_EQ(changed_page, result);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamOpenReadWritePositionsForAppend )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        auto first = makePage(page_size, std::byte(1));
        auto second = makePage(page_size, std::byte(2));
        auto third = makePage(page_size, std::byte(3));
        auto fourth = makePage(page_size, std::byte(4));

        TestRandomIO_Stream created(page_io, std::uint32_t(3));
        ASSERT_EQ(0u, created.append(first.data()));
        ASSERT_EQ(1u, created.append(second.data()));
        ASSERT_EQ(3u, created.append(third.data()));
        created.flush();

        auto stream_page_num = created.getPageNum();
        RandomIO_Stream opened(page_io, stream_page_num, 3, AccessType::READ_WRITE);
        ASSERT_EQ(4u, opened.append(fourth.data()));
        opened.flush();

        std::vector<std::byte> read_buf(opened.getPageSize());
        opened.readRandom(0, read_buf.data());
        ASSERT_EQ(first, read_buf);
        opened.readRandom(1, read_buf.data());
        ASSERT_EQ(second, read_buf);
        opened.readRandom(3, read_buf.data());
        ASSERT_EQ(third, read_buf);
        opened.readRandom(4, read_buf.data());
        ASSERT_EQ(fourth, read_buf);
    }

    TEST_F( Page_IOTest, testRandomIO_StreamMaintainsIndependentStreamsOverSharedDiffIO )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&file]() -> std::uint64_t {
            return file.size();
        };

        RandomIO_StreamDiffIO page_io(file, page_size, page_size * 16, tail_function);
        auto a1 = makePage(page_size, std::byte(0xa1));
        auto a2 = makePage(page_size, std::byte(0xa2));
        auto a3 = makePage(page_size, std::byte(0xa3));
        auto b1 = makePage(page_size, std::byte(0xb1));
        auto b2 = makePage(page_size, std::byte(0xb2));
        auto b3 = makePage(page_size, std::byte(0xb3));

        TestRandomIO_Stream stream_a(page_io, std::uint32_t(3));
        ASSERT_EQ(0u, stream_a.append(a1.data()));
        ASSERT_EQ(1u, stream_a.append(a2.data()));
        stream_a.flush();

        TestRandomIO_Stream stream_b(page_io, std::uint32_t(3));
        ASSERT_EQ(3u, stream_b.append(b1.data()));
        ASSERT_EQ(4u, stream_b.append(b2.data()));
        stream_b.flush();

        auto stream_a_page_num = stream_a.getPageNum();
        auto stream_b_page_num = stream_b.getPageNum();

        RandomIO_Stream opened_a(page_io, stream_a_page_num, 3, AccessType::READ_WRITE);
        ASSERT_EQ(6u, opened_a.append(a3.data()));
        opened_a.flush();

        RandomIO_Stream opened_b(page_io, stream_b_page_num, 3, AccessType::READ_WRITE);
        ASSERT_EQ(9u, opened_b.append(b3.data()));
        opened_b.flush();

        std::vector<std::byte> read_buf(page_size);
        opened_a.readRandom(0, read_buf.data());
        ASSERT_EQ(a1, read_buf);
        opened_a.readRandom(1, read_buf.data());
        ASSERT_EQ(a2, read_buf);
        opened_a.readRandom(6, read_buf.data());
        ASSERT_EQ(a3, read_buf);
        opened_b.readRandom(3, read_buf.data());
        ASSERT_EQ(b1, read_buf);
        opened_b.readRandom(4, read_buf.data());
        ASSERT_EQ(b2, read_buf);
        opened_b.readRandom(9, read_buf.data());
        ASSERT_EQ(b3, read_buf);
    }

}
