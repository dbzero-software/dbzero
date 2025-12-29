// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/AlgoAllocator.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <utils/TestWorkspace.hpp>

using namespace std;

namespace tests

{

    using Address = db0::Address;

    class AlgoAllocatorTests: public testing::Test
    {
    public:
        virtual void SetUp() override {
            // set up the address pool functions
            m_pool_f = [](unsigned int i) -> Address { 
                return Address::fromOffset(i * (PAGE_SIZE + SLAB_SIZE * PAGE_SIZE));
            };

            m_reverse_pool_f = [](Address address)->unsigned int {
                if (address % (PAGE_SIZE + SLAB_SIZE * PAGE_SIZE) != 0) {
                    THROWF(db0::InternalException) << "AlgoAllocatorTests: invalid address " << address;
                }
                return address / (PAGE_SIZE + SLAB_SIZE * PAGE_SIZE); 
            };
        }

        virtual void TearDown() override {            
        }
        
    protected:
        static constexpr unsigned int PAGE_SIZE = 4096;
        // as number of pages
        static constexpr unsigned int SLAB_SIZE = 1024;

        db0::AlgoAllocator::AddressPoolF m_pool_f;
        db0::AlgoAllocator::ReverseAddressPoolF m_reverse_pool_f;
    };
    
    TEST_F( AlgoAllocatorTests , testAlgoAllocatorAllocatesMonotonicAddresses )
    { 
        db0::AlgoAllocator cut(m_pool_f, m_reverse_pool_f, PAGE_SIZE);
        std::uint64_t last_address = 0;
        for (int i = 0;i < 100;++i) {
            auto address = cut.alloc(PAGE_SIZE);            
            ASSERT_TRUE(address >= last_address);
            last_address = address;
        }
    }
    
    TEST_F( AlgoAllocatorTests , testAlgoAllocatorThrowsOnInvalidFree )
    {
        db0::AlgoAllocator cut(m_pool_f, m_reverse_pool_f, PAGE_SIZE);
        for (int i = 0;i < 10;++i) {
            cut.alloc(PAGE_SIZE);            
        }
        // unaligned address is OK (inner address)
        ASSERT_NO_THROW(cut.free(Address::fromOffset(7 * (PAGE_SIZE + SLAB_SIZE * PAGE_SIZE) + 13)));
        // address out of range
        ASSERT_ANY_THROW(cut.free(Address::fromOffset(11 * (PAGE_SIZE + SLAB_SIZE * PAGE_SIZE))));
    }

    TEST_F( AlgoAllocatorTests , testAlgoAllocatorGetAllocSize )
    {
        db0::AlgoAllocator cut(m_pool_f, m_reverse_pool_f, PAGE_SIZE);
        for (int i = 0;i < 10;++i) {
            cut.alloc(PAGE_SIZE);            
        }
        ASSERT_EQ(PAGE_SIZE, cut.getAllocSize(Address::fromOffset(0)));
        // inner address
        ASSERT_EQ(PAGE_SIZE - 13, cut.getAllocSize(Address::fromOffset(7 * (PAGE_SIZE + SLAB_SIZE * PAGE_SIZE) + 13)));
        // address out of range
        ASSERT_ANY_THROW(cut.getAllocSize(Address::fromOffset(11 * (PAGE_SIZE + SLAB_SIZE * PAGE_SIZE))));
    }
    
}
