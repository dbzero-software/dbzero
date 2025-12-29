// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_CompressedLookupTree.hpp>
#include <dbzero/core/memory/BitSpace.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/utils/bisect.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    class SGB_CompressedLookupTreeTest: public testing::Test 
    {
    public:
        SGB_CompressedLookupTreeTest()
            : m_memspace(m_workspace.getMemspace("my-test-prefix_1"))
            // configure bitspace to use the entire 4kb page - i.e. 0x8000 bits
            , m_bitspace(m_memspace.getPrefixPtr(), Address::fromOffset(0), page_size)
        {
        }
        
        void SetUp() override {
            m_bitspace.clear();
        }
        
        void TearDown() override {
            m_bitspace.clear();        
        }

    protected:
        TestWorkspace m_workspace;
        static constexpr std::size_t page_size = 4096;
        Memspace m_memspace;
        BitSpace<0x8000> m_bitspace;
    };
    
    template <typename IntT = std::uint16_t>
    struct [[gnu::packed]] CompressingTestHeader: public o_fixed<CompressingTestHeader<IntT> > 
    {
        std::uint32_t m_base = 0;

        /// initialize header and compress the first item
        IntT compressFirst(std::uint32_t first_item) 
        {
            m_base = first_item;
            return 0;
        }

        IntT compress(std::uint32_t key_item) const
        {
            if (!canFit(key_item)) {
                THROWF(db0::InternalException) << "Unable to fit " << key_item << " with base: " << m_base;
            }
            return static_cast<IntT>(key_item - m_base);
        }
        
        std::uint32_t uncompress(IntT item) const {
            return m_base + item;
        }

        bool canFit(std::uint32_t item) const 
        {
            if (item < m_base) {
                return false;
            }
            return item - m_base <= std::numeric_limits<IntT>::max();
        }

        std::string toString(IntT item) const {
            return std::to_string(uncompress(item));
        }

        std::string toString() const {
            return "Header{base=" + std::to_string(m_base) + "}";
        }
    };
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeCanBeCreatedOnBitspace )
    {
        // compress uint64 to uint16
        using HeaderT = CompressingTestHeader<std::uint16_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint16_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        ASSERT_TRUE(cut.getAddress().isValid());
    }
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeCanCompressInsertedElements )
    {
        // compress uint64 to uint16
        using HeaderT = CompressingTestHeader<std::uint16_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint16_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        unsigned int i = 0;
        while (m_bitspace.span() < 2) {
            cut.insert(i++);
        }
        // make sure there's less space used then total number of elements
        ASSERT_TRUE(page_size < sizeof(std::uint64_t) * i);
    }
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeNodesStayCompressedAndBalancedAfterSplit )
    {
        // compress uint64 to uint16
        using HeaderT = CompressingTestHeader<std::uint16_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint16_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        unsigned int i = 0;
        while (m_bitspace.span() < 2) {
            cut.insert(i++);
        }
        
        auto size = cut.size();
        int sorted = 0;
        for (auto node = cut.cbegin_nodes(); node != cut.cend_nodes(); ++node) {
            long int diff = (int)node->size() - size / 2;                      
            ASSERT_TRUE(diff <= 2);
            if (node->is_sorted()) {
                sorted++;
            }            
        }
        ASSERT_TRUE(sorted > 0);
    }

    template <typename TreeT> int countNodes(const TreeT &tree) 
    {
        int result = 0;
        for (auto node = tree.cbegin_nodes(); node != tree.cend_nodes(); ++node) {
            ++result;
        }
        return result;
    }
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeHeaderIsInitialized )
    {
        // compress uint64 to uint16
        using HeaderT = CompressingTestHeader<std::uint16_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint16_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        cut.insert(123);
        ASSERT_EQ(cut.cbegin_nodes()->header().m_base, 123);
    }
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeSplitNodesIfUnableToCompressElement )
    {
        // compress uint64 to uint16
        using HeaderT = CompressingTestHeader<std::uint16_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint16_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        std::uint16_t value = std::numeric_limits<std::uint16_t>::max();
        cut.insert(value);
        ASSERT_EQ(countNodes(cut), 1);
        // a new node must be created to fit the higher value
        cut.insert(value * 2 + 2);
        ASSERT_EQ(countNodes(cut), 2);
        cut.insert(value + 1);
        cut.insert(value * 2 + 3);
        ASSERT_EQ(countNodes(cut), 2);
    }
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeCanSplitFirstNode )
    {
        // compress uint64 to uint16
        using HeaderT = CompressingTestHeader<std::uint16_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint16_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        cut.insert(123);
        ASSERT_EQ(countNodes(cut), 1);
        // the second node should be created if the element is less than min
        // NOTE: in future node rebase may be implemented to handle such case
        cut.insert(100);
        ASSERT_EQ(countNodes(cut), 2);
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeCanFindLowerEqualBound )
    {
        using HeaderT = CompressingTestHeader<std::uint16_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint16_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        std::vector<std::uint32_t> values;
        srand(781785u);
        for (int i = 0; i < 10000; ++i) {
            std::uint32_t value = rand() % 100000;
            values.push_back(value);
            cut.insert(value);
        }

        std::sort(values.begin(), values.end());
        for (int i = 0; i < 1000; ++i) {
            std::uint32_t value = rand() % 100000;
            auto item = cut.lower_equal_bound(value);
            auto le = bisect::lower_equal(values.begin(), values.end(), value, std::less<std::uint32_t>());
            if (le != values.end()) {
                ASSERT_TRUE(item.has_value());
                ASSERT_EQ(*item, *le);
            } else {
                ASSERT_FALSE(item.has_value());
            }
        }
    }
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeFindLowerWhenUnableToFit )
    {
        // NOTE: in this test we're compreessing to 8 bits
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        
        // Populate the first node densely
        for (std::uint32_t i = 0; i < 256u; ++i) {
            cut.insert(i);
        }

        // force a new distant node
        cut.insert(1000);

        // locate element in between nodes 
        auto item = cut.findLower(500);
        ASSERT_EQ(item.second->header().uncompress(*item.first), 255u);        
    }
    
    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeFindUpperWhenUnableToFit )
    {
        // NOTE: in this test we're compreessing to 8 bits
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace, 
            page_size, AccessType::READ_WRITE);
        
        // Populate the first node densely
        for (std::uint32_t i = 0; i < 256u; ++i) {
            cut.insert(i);
        }

        // force a new distant node
        cut.insert(1000);        
        ASSERT_EQ(cut.upper_equal_bound(500).value(), 1000u);
    }

}

