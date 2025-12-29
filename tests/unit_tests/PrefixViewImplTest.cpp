// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/PrefixViewImpl.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class PrefixViewImplTest: public testing::Test
    {
    public:
        PrefixViewImplTest()
            : m_cache_recycler(16 * 1024 * 1024, m_dirty_meter)
        {
        }

        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override 
        {
            drop(file_name);
            m_dirty_meter = 0;
            m_cache_recycler.clear();
        }

        virtual void TearDown() override 
        {
            drop(file_name);          
            m_dirty_meter = 0;
            m_cache_recycler.clear();
        }

    protected:
        std::atomic<std::size_t> m_dirty_meter = 0;
        CacheRecycler m_cache_recycler;
    };
    
    TEST_F( PrefixViewImplTest , testPrefixViewFreezePageFromHeadTransaction )
    {
        BDevStorage::create(file_name);
        auto storage = std::make_shared<BDevStorage>(file_name);
        PrefixImpl px(file_name, m_dirty_meter, &m_cache_recycler, storage);
        auto page_size = px.getPageSize();        
        {
            auto w1 = px.mapRange(0, page_size, { AccessOptions::write });
            memcpy(w1.modify(), "12345678", 8);
        }
        px.commit();
        auto state_num = px.getStateNum(true);
        // create the head commit's view
        PrefixViewImpl cut(px.getName(), storage, px.getCache(), state_num);
        // map page but don't hold the reference to it
        cut.mapRange(0, 8, { AccessOptions::read });

        // overwrite the page with new data
        {
            auto w2 = px.mapRange(0, page_size, { AccessOptions::write });
            memcpy(w2.modify(), "abcdefgh", 8);
        }

        // read from r1
        auto r1 = cut.mapRange(0, 8, { AccessOptions::read });
        ASSERT_EQ(std::string((char *)r1.m_buffer, 8), "12345678");
        cut.close();
        px.close();
    }

}
