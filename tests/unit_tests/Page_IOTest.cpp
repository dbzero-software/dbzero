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
        cut.append(buf.data(), 8);
        ASSERT_EQ(cut.getCurrentStepRemainingPages(), 5);
        ASSERT_EQ(cut.getNextPageNum().first, 11);
    }

}