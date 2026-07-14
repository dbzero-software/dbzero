// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/MetaAllocator.hpp>
#include <dbzero/core/memory/SlabItem.hpp>
#include <dbzero/core/memory/Recycler.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <utils/TestWorkspace.hpp>
#include <utils/cp_data_1.hpp>

using namespace std;

namespace tests

{

    using namespace db0;

    class CapacityTreeTests: public testing::Test 
    {
    public:
        CapacityTreeTests()
            : m_memspace(m_workspace.getMemspace("my-test-prefix_1"))
            // configure bitspace to use the entire 4kb page - i.e. 0x8000 bits
            , m_bitspace(m_memspace.getPrefixPtr(), Address::fromOffset(0), page_size)
        {
        }
        
        virtual void SetUp() override {
            m_bitspace.clear();
        }

        virtual void TearDown() override {
            m_bitspace.clear();
        }

    protected:
        db0::TestWorkspace m_workspace;
        static constexpr std::size_t page_size = 4096;
        db0::Memspace m_memspace;
        db0::BitSpace<0x8000> m_bitspace;
    };

    TEST_F( CapacityTreeTests , testCreateEmptyCapacityTree )
    {
        using CapacityTreeT = typename db0::MetaAllocator::CapacityTreeT;
        CapacityTreeT cut(m_bitspace, page_size);
        ASSERT_TRUE(cut.getAddress() != 0);
    }
    
    TEST_F( CapacityTreeTests , testCapacityTreeInsertEraseIssue1 )
    {
        using CapacityTreeT = typename db0::MetaAllocator::CapacityTreeT;        
        
        std::vector<CapacityTreeT> realms;
        realms.emplace_back(m_bitspace, page_size);
        realms.emplace_back(m_bitspace, page_size);
        auto data = db0::tests::getCPData();
        for (const auto &item: data) {
            // process line
            unsigned int op_code = std::get<0>(item);
            unsigned int realm_id = std::get<1>(item);
            unsigned int capacity = std::get<2>(item);
            unsigned int slab = std::get<3>(item);
            auto &cut = realms[realm_id];
            if (op_code == 0) {
                // insert
                cut.insert(CapacityItem { capacity, 0, slab });
            } else if (op_code == 1) {
                // erase
                CapacityItem item { capacity, 0, slab };
                auto it = cut.find_equal(item);
                ASSERT_FALSE(it.isEnd());
                cut.erase(it);
            } else if (op_code == 2) {
                // emplace                
                cut.emplace(capacity, 0, slab);                
            }
        }
    }

}