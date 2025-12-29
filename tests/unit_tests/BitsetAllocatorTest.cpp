// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/BitsetAllocator.hpp>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/memory/Address.hpp>

using namespace std;

namespace tests

{

    using Address = db0::Address;

    class BitsetAllocatorTests: public testing::Test 
    {
    public:
        virtual void SetUp() override {            
        }

        virtual void TearDown() override {
            
        }

    protected:
        db0::TestWorkspace m_workspace;
    };
    
    TEST_F( BitsetAllocatorTests , testAllocAssignsValidAddresses )
    {        
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        
        auto base_addr = Address::fromOffset(0);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, 1);
        auto ptr1 = cut.alloc(page_size);
        auto ptr2 = cut.alloc(page_size);
        ASSERT_NE(ptr1, ptr2);
    }
    
    TEST_F( BitsetAllocatorTests , testAllocThenFree )
    {        
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
                
        auto base_addr = Address::fromOffset(0);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, 1);
        auto ptr1 = cut.alloc(page_size);
        auto ptr2 = cut.alloc(page_size);
        cut.free(ptr2);
        // throws on double free
        ASSERT_ANY_THROW(cut.free(ptr2));
        // throws on get alloc size of the freed range
        ASSERT_ANY_THROW(cut.getAllocSize(ptr2));
        // allocates the same range again
        auto ptr3 = cut.alloc(page_size);
        ASSERT_EQ(ptr2, ptr3);        
        // get alloc size works for allocated ranges
        ASSERT_EQ(page_size, cut.getAllocSize(ptr1));
        ASSERT_EQ(page_size, cut.getAllocSize(ptr3));
    }
    
    TEST_F( BitsetAllocatorTests , testBitsetAllocatorCanHandleSubAddresses )
    {
        // this functionality allows allocating some range and then requesting a subrange of it
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        
        auto base_addr = Address::fromOffset(0);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, 1);
        auto ptr1 = cut.alloc(page_size);
        std::size_t offset = 128;
        ASSERT_EQ(cut.getAllocSize(ptr1 + offset), page_size - offset);
    }
    
    TEST_F( BitsetAllocatorTests , testBitsetAllocatorCanAllocateInNegativeDirection )
    {
        // this functionality allows allocating some range and then requesting a subrange of it
        std::size_t page_size = 4096;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        auto base_addr = Address::fromOffset(page_size * 1024);
        db0::BitsetAllocator<db0::VFixedBitset<123> > cut(db0::VFixedBitset<123>(memspace), base_addr, page_size, -1);
        auto ptr1 = cut.alloc(page_size);
        ASSERT_EQ(ptr1.getOffset(), page_size * 1024 - page_size);
        auto ptr2 = cut.alloc(page_size);
        ASSERT_TRUE(ptr2 < ptr1);
    }
    
}   
