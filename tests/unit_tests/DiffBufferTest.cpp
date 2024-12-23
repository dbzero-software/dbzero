#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <cstring>
#include <dbzero/core/storage/diff_buffer.hpp>
#include <dbzero/core/memory/ResourceLock.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    class DiffBufferTest: public testing::Test
    {
    };
    
    TEST_F( DiffBufferTest , testDiffBufferCreate )
    {
        std::size_t page_size = 4096;
        std::vector<std::byte> dp_0(page_size, std::byte(0));
        std::vector<std::byte> dp_1(page_size, std::byte(0));
        std::memset(dp_1.data() + 18, 1, 32);
        std::memset(dp_1.data() + 94, 2, 11);
        std::memset(dp_1.data() + 1200, 3, 120);

        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(dp_0.data(), dp_1.data(), page_size, diff_buf);

        std::vector<std::byte> temp(page_size);
        auto &cut = o_diff_buffer::__new(temp.data(), dp_0.data(), diff_buf);
        ASSERT_EQ(cut.m_size, o_diff_buffer::measure(dp_0.data(), diff_buf));
        // diff data + administrative overhead
        ASSERT_TRUE((cut.sizeOf() < 32 + 11 + 120 + 2 + 4 * 4));
    }

    TEST_F( DiffBufferTest , testDiffBufferApply )
    {
        std::size_t page_size = 4096;
        std::vector<std::byte> dp_0(page_size, std::byte(0));
        std::vector<std::byte> dp_1(page_size, std::byte(0));
        std::memset(dp_1.data() + 18, 1, 32);
        std::memset(dp_1.data() + 94, 2, 11);
        std::memset(dp_1.data() + 1200, 3, 120);

        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(dp_0.data(), dp_1.data(), page_size, diff_buf);

        std::vector<std::byte> temp(page_size);
        auto &cut = o_diff_buffer::__new(temp.data(), dp_1.data(), diff_buf);
        
        std::vector<std::byte> result(page_size);
        // apply diffs
        cut.apply(dp_0.data(), page_size, result.data());
        ASSERT_EQ(0, std::memcmp(dp_1.data(), result.data(), page_size));
    }

}   