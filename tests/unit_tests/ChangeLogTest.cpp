// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <dbzero/core/storage/ChangeLogTypes.hpp>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class ChangeLogTest: public testing::Test
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
    
    TEST_F( ChangeLogTest , testChangeLogMeasureAndSizeOf )
    {
        std::vector<std::byte> buf;
        // create default change log (i.e. null header)
        using ChangeLogT = o_change_log<>;

        // Test empty
        {            
            auto measured_size = ChangeLogT::measure(ChangeLogData());
            buf.resize(measured_size);
            ChangeLogT::__new(buf.data(), ChangeLogData());
            auto safe_size = ChangeLogT::safeSizeOf(buf.data());
            ASSERT_EQ(measured_size, safe_size);
        }
        
        // Test RLE compressed
        {
            std::vector<std::uint64_t> change_log = { 1, 2, 3, 4, 5 };
            ChangeLogData data(std::move(change_log), true, false, false);
            auto measured_size = ChangeLogT::measure(data);
            buf.resize(measured_size);
            ChangeLogT::__new(buf.data(), data);
            auto safe_size = ChangeLogT::safeSizeOf(buf.data());         
            ASSERT_EQ(measured_size, safe_size);
        }

        // Test uncompressed
        {
            std::vector<std::uint64_t> change_log = { 3, 4, 8 };
            ChangeLogData data(std::move(change_log), false, false, false);
            auto measured_size = ChangeLogT::measure(data);
            buf.resize(measured_size);
            ChangeLogT::__new(buf.data(), data);
            auto safe_size = ChangeLogT::safeSizeOf(buf.data());
            ASSERT_EQ(measured_size, safe_size);
        }                
    }

    TEST_F( ChangeLogTest , testChangeLogNullHeaderHasNoOverhead )
    {
        std::vector<std::byte> buf;
        // create default change log (i.e. null header)
        using ChangeLogT = o_change_log<db0::o_fixed_null>;
                
        std::vector<std::uint64_t> change_log = { 1, 2, 3, 4, 5 };
        ChangeLogData data(std::move(change_log), true, false, false);
        auto measured_size = ChangeLogT::measure(data);
        buf.resize(measured_size);
        auto &cut = ChangeLogT::__new(buf.data(), data);
        ASSERT_TRUE(cut.isRLECompressed());

        auto diff = (std::byte*)&cut.rleCompressed() - (std::byte*)&cut;
        ASSERT_EQ(0, diff);
        
        unsigned int count = 0;
        for (auto addr: cut) {
            ASSERT_EQ(addr, count + 1);
            ++count;
        }
        ASSERT_EQ(count, 5u);
    }
    
    TEST_F( ChangeLogTest , testChangeLogWithHeader )
    {
        std::vector<std::byte> buf1, buf2;
        // create default change log (i.e. null header)
        using ChangeLogT1 = o_change_log<db0::o_fixed_null>;
        using ChangeLogT2 = o_change_log<db0::o_dp_changelog_header>;
        
        // create without header first
        std::vector<std::uint64_t> change_log = { 1, 2, 3, 4, 5 };
        ChangeLogData data(std::move(change_log), true, false, false);
        auto measured_size = ChangeLogT1::measure(data);
        buf1.resize(measured_size);
        auto &cut1 = ChangeLogT1::__new(buf1.data(), data);
        
        // create with header next (same data)
        measured_size = ChangeLogT2::measure(data);
        buf2.resize(measured_size);
        auto &cut2 = ChangeLogT2::__new(buf2.data(), data, 123, 456);
        ASSERT_EQ(cut1.sizeOf() + o_dp_changelog_header::sizeOf(), cut2.sizeOf());

        // compare contents of both change logs
        auto it1 = cut1.begin();
        auto it2 = cut2.begin();
        while (it1 != cut1.end() && it2 != cut2.end()) {
            ASSERT_EQ(*it1, *it2);
            ++it1;
            ++it2;
        }
        ASSERT_FALSE(it1 != cut1.end());
        ASSERT_FALSE(it2 != cut2.end());
        
        // access header fields
        ASSERT_EQ(123u, cut2.m_state_num);
        ASSERT_EQ(456u, cut2.m_end_storage_page_num);
    }
    
}
