// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <vector>
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

    template <typename TreeT>
    std::vector<std::uint64_t> collectSorted(const TreeT &tree)
    {
        std::vector<std::uint64_t> result;
        for (auto it = tree.sortedBegin(); !it.is_end(); ++it) {
            result.push_back(*it);
        }
        return result;
    }

    template <typename TreeT>
    std::vector<std::uint64_t> collectSortedFrom(const TreeT &tree, std::uint64_t first)
    {
        std::vector<std::uint64_t> result;
        for (auto it = tree.sortedBeginFrom(first); !it.is_end(); ++it) {
            result.push_back(*it);
        }
        return result;
    }

    template <typename TreeT>
    std::vector<std::uint64_t> collectSortedRange(const TreeT &tree, std::uint64_t first, std::uint64_t end)
    {
        std::vector<std::uint64_t> result;
        for (auto it = tree.sortedBeginFrom(first); !it.is_end() && *it < end; ++it) {
            result.push_back(*it);
        }
        return result;
    }

    template <typename TreeT>
    std::vector<std::uint64_t> collectForRange(const TreeT &tree, std::uint64_t first, std::uint64_t end)
    {
        std::vector<std::uint64_t> result;
        tree.forRange(first, end, [&](const auto &item) {
            result.push_back(item);
        });
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

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeCanEraseCompressedKey )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        for (std::uint32_t i = 0; i < 256u; ++i) {
            cut.insert(i);
        }
        cut.insert(1000);

        ASSERT_TRUE(cut.erase_equal(42u));
        ASSERT_FALSE(cut.erase_equal(42u));
        ASSERT_EQ(cut.size(), 256u);
        ASSERT_EQ(cut.lower_equal_bound(42u).value(), 41u);

        ASSERT_TRUE(cut.erase_equal(1000u));
        ASSERT_EQ(cut.lower_equal_bound(1000u).value(), 255u);
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeCanEraseCompressedRange )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        for (std::uint32_t i = 0; i < 256u; ++i) {
            cut.insert(i);
        }
        cut.insert(1000);
        cut.insert(1001);

        ASSERT_EQ(cut.erase_range(40u, 200u), 160u);
        ASSERT_EQ(cut.size(), 98u);
        ASSERT_EQ(cut.lower_equal_bound(39u).value(), 39u);
        ASSERT_EQ(cut.lower_equal_bound(199u).value(), 39u);
        ASSERT_EQ(cut.lower_equal_bound(200u).value(), 200u);
        ASSERT_EQ(cut.lower_equal_bound(1001u).value(), 1001u);
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorVisitsAllItems )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected { 3000, 0, 255, 1000, 1005, 40, 41, 2000, 2255, 5 };
        for (auto value : expected) {
            cut.insert(value);
        }

        std::sort(expected.begin(), expected.end());

        ASSERT_GT(countNodes(cut), 1);
        ASSERT_EQ(collectSorted(cut), expected);
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorCanStartFromItem )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected;
        for (std::uint64_t base = 0; base <= 3000; base += 1000) {
            for (std::uint64_t offset : { 0u, 1u, 40u, 200u, 255u }) {
                auto value = base + offset;
                cut.insert(value);
                expected.push_back(value);
            }
        }
        std::sort(expected.begin(), expected.end());

        auto expected_begin = std::lower_bound(expected.begin(), expected.end(), 1002u);
        ASSERT_GT(countNodes(cut), 1);
        ASSERT_EQ(collectSortedFrom(cut, 1002u), std::vector<std::uint64_t>(expected_begin, expected.end()));
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorHandlesStartEdges )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected { 100, 101, 102, 1000 };
        for (auto value : expected) {
            cut.insert(value);
        }
        std::sort(expected.begin(), expected.end());

        ASSERT_EQ(collectSortedFrom(cut, 1u), expected);
        ASSERT_TRUE(cut.sortedBeginFrom(2000u).is_end());
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorHandlesEmptyTree )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        ASSERT_TRUE(cut.sortedBegin().is_end());
        ASSERT_TRUE(cut.sortedBeginFrom(100u).is_end());
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorStartsWithinSingleNode )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected { 10, 20, 30, 40 };
        for (auto value : expected) {
            cut.insert(value);
        }

        ASSERT_EQ(countNodes(cut), 1);
        ASSERT_EQ(collectSortedFrom(cut, 0u), expected);
        ASSERT_EQ(collectSortedFrom(cut, 10u), expected);
        ASSERT_EQ(collectSortedFrom(cut, 25u), (std::vector<std::uint64_t> { 30, 40 }));
        ASSERT_TRUE(cut.sortedBeginFrom(41u).is_end());
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorStartsAtMultiNodeBoundaries )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected { 0, 1, 255, 1000, 1001, 1255, 2000 };
        for (auto value : expected) {
            cut.insert(value);
        }
        std::sort(expected.begin(), expected.end());

        ASSERT_GT(countNodes(cut), 1);
        ASSERT_EQ(collectSortedFrom(cut, 255u), (std::vector<std::uint64_t> { 255, 1000, 1001, 1255, 2000 }));
        ASSERT_EQ(collectSortedFrom(cut, 256u), (std::vector<std::uint64_t> { 1000, 1001, 1255, 2000 }));
        ASSERT_EQ(collectSortedFrom(cut, 1000u), (std::vector<std::uint64_t> { 1000, 1001, 1255, 2000 }));
        ASSERT_EQ(collectSortedFrom(cut, 1256u), (std::vector<std::uint64_t> { 2000 }));
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorSupportsBoundedSingleNodeRanges )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        for (auto value : { 10u, 20u, 30u, 40u, 50u }) {
            cut.insert(value);
        }

        ASSERT_EQ(countNodes(cut), 1);
        ASSERT_EQ(collectSortedRange(cut, 15u, 45u), (std::vector<std::uint64_t> { 20, 30, 40 }));
        ASSERT_EQ(collectSortedRange(cut, 20u, 20u), (std::vector<std::uint64_t> {}));
        ASSERT_EQ(collectSortedRange(cut, 0u, 10u), (std::vector<std::uint64_t> {}));
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorSupportsBoundedMultiNodeRanges )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected { 0, 100, 255, 1000, 1001, 1255, 2000, 2001 };
        for (auto value : expected) {
            cut.insert(value);
        }

        ASSERT_GT(countNodes(cut), 1);
        ASSERT_EQ(collectSortedRange(cut, 100u, 1001u), (std::vector<std::uint64_t> { 100, 255, 1000 }));
        ASSERT_EQ(collectSortedRange(cut, 256u, 2001u), (std::vector<std::uint64_t> { 1000, 1001, 1255, 2000 }));
        ASSERT_EQ(collectSortedRange(cut, 1256u, 1999u), (std::vector<std::uint64_t> {}));
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeForRangeVisitsSortedHalfOpenRange )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected { 0, 100, 255, 1000, 1000, 1001, 1255, 2000, 2001 };
        for (auto value : expected) {
            cut.insert(value);
        }

        ASSERT_GT(countNodes(cut), 1);
        ASSERT_EQ(collectForRange(cut, 100u, 1001u), (std::vector<std::uint64_t> { 100, 255, 1000, 1000 }));
        ASSERT_EQ(collectForRange(cut, 256u, 2001u), (std::vector<std::uint64_t> { 1000, 1000, 1001, 1255, 2000 }));
        ASSERT_EQ(collectForRange(cut, 1256u, 1999u), (std::vector<std::uint64_t> {}));
        ASSERT_EQ(collectForRange(cut, 2001u, 2001u), (std::vector<std::uint64_t> {}));
        ASSERT_EQ(collectForRange(cut, 2002u, 2001u), (std::vector<std::uint64_t> {}));
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeConstSortedIteratorKeepsDuplicateItems )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        std::vector<std::uint64_t> expected { 10, 10, 20, 20, 20, 1000, 1000 };
        for (auto value : expected) {
            cut.insert(value);
        }
        std::sort(expected.begin(), expected.end());

        ASSERT_GT(countNodes(cut), 1);
        ASSERT_EQ(collectSorted(cut), expected);
        ASSERT_EQ(collectSortedFrom(cut, 20u), (std::vector<std::uint64_t> { 20, 20, 20, 1000, 1000 }));
    }

    TEST_F( SGB_CompressedLookupTreeTest , testSGBCompressedLookupTreeEraseRangeEdgeCasesWithSmallNodes )
    {
        using HeaderT = CompressingTestHeader<std::uint8_t>;
        SGB_CompressedLookupTree<std::uint64_t, std::uint8_t, HeaderT> cut(m_bitspace,
            page_size, AccessType::READ_WRITE);

        ASSERT_EQ(cut.erase_range(0u, 1u), 0u);
        ASSERT_TRUE(cut.empty());

        std::vector<std::uint64_t> expected;
        for (std::uint64_t base = 0; base <= 3000; base += 1000) {
            for (std::uint64_t offset = 0; offset < 256; ++offset) {
                cut.insert(base + offset);
                expected.push_back(base + offset);
            }
        }
        ASSERT_GT(countNodes(cut), 1);
        ASSERT_EQ(cut.size(), expected.size());

        auto erase_expected = [&](std::uint64_t first, std::uint64_t last) {
            auto first_it = std::lower_bound(expected.begin(), expected.end(), first);
            auto last_it = std::lower_bound(expected.begin(), expected.end(), last);
            auto count = static_cast<std::size_t>(last_it - first_it);
            expected.erase(first_it, last_it);
            return count;
        };

        ASSERT_EQ(cut.erase_range(40u, 40u), 0u);
        ASSERT_EQ(cut.erase_range(41u, 40u), 0u);
        ASSERT_EQ(cut.erase_range(260u, 900u), 0u);
        ASSERT_EQ(cut.erase_range(4000u, 4100u), 0u);
        ASSERT_EQ(cut.size(), expected.size());

        ASSERT_EQ(cut.erase_range(250u, 1005u), erase_expected(250u, 1005u));
        auto lower_250 = cut.lower_equal_bound(250u);
        auto upper_250 = cut.upper_equal_bound(250u);
        auto lower_1004 = cut.lower_equal_bound(1004u);
        auto lower_1005 = cut.lower_equal_bound(1005u);
        ASSERT_TRUE(lower_250.has_value());
        ASSERT_TRUE(upper_250.has_value());
        ASSERT_TRUE(lower_1004.has_value());
        ASSERT_TRUE(lower_1005.has_value());
        ASSERT_EQ(lower_250.value(), 249u);
        ASSERT_EQ(upper_250.value(), 1005u);
        ASSERT_EQ(lower_1004.value(), 249u);
        ASSERT_EQ(lower_1005.value(), 1005u);
        ASSERT_EQ(cut.size(), expected.size());

        ASSERT_EQ(cut.erase_range(0u, 3u), erase_expected(0u, 3u));
        ASSERT_FALSE(cut.lower_equal_bound(2u).has_value());
        auto lower_3 = cut.lower_equal_bound(3u);
        ASSERT_TRUE(lower_3.has_value());
        ASSERT_EQ(lower_3.value(), 3u);
        ASSERT_EQ(cut.size(), expected.size());

        ASSERT_EQ(cut.erase_range(3250u, 4000u), erase_expected(3250u, 4000u));
        auto lower_4000 = cut.lower_equal_bound(4000u);
        ASSERT_TRUE(lower_4000.has_value());
        ASSERT_EQ(lower_4000.value(), 3249u);
        ASSERT_EQ(cut.size(), expected.size());

        ASSERT_EQ(cut.erase_range(0u, 4000u), expected.size());
        ASSERT_TRUE(cut.empty());
        ASSERT_EQ(cut.size(), 0u);
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
