// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_Tree.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_Key.hpp>
#include <dbzero/core/memory/BitSpace.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    class SGB_TreeTests: public testing::Test 
    {
    public:
        SGB_TreeTests()
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
    
    TEST_F( SGB_TreeTests , testSGBTreeCanBeCreatedOnBitspace )
    {
        db0::SGB_Tree<std::uint64_t> cut(m_bitspace, page_size);
        ASSERT_TRUE(cut.getAddress() != 0);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanCreateRootNodeOnTheSamePageAsHeadNode )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        cut.insert(0);
        // make sure there was only a single node created
        ASSERT_EQ(m_bitspace.span(), 1);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanFitMultipleItemsInASingleAllocatedBlock )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        // let's insert 10 items
        for (std::uint64_t i = 0; i < 10; ++i) {
            cut.insert(i);
        }        
        // make sure there was only a single node created
        ASSERT_EQ(m_bitspace.span(), 1);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanBeIterated )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        // let's insert 10 items
        for (std::uint64_t i = 0; i < 10; ++i) {
            cut.insert(i);
        }        
        std::vector<std::uint64_t> items;
        for (auto it = cut.cbegin(); !it.is_end(); ++it) {
            items.push_back(*it);
        }
        ASSERT_EQ(items, std::vector<std::uint64_t>({0,1,2,3,4,5,6,7,8,9}));
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanGrowBeyondOneBlock )
    {
        // validate pre-condition
        ASSERT_EQ(m_bitspace.span(), 0);
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        // let's insert 800 items to make sure te utilized capacity is more than one page size
        for (std::uint64_t i = 0; i < 800; ++i) {
            cut.insert(i);
        }
        // make sure there were 3 or less allocations in the bitspace
        ASSERT_TRUE(m_bitspace.span() <= 3);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanSortRadomlyInsertedItems )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        std::vector<std::uint64_t> items_to_add = { 5, 6, 1, 2, 0, 4, 3, 7, 8 };
        for (auto item : items_to_add) {
            cut.insert(item);
        }
        std::vector<std::uint64_t> items;
        for (auto it = cut.cbegin(); !it.is_end(); ++it) {
            items.push_back(*it);
        }
        ASSERT_EQ(items, std::vector<std::uint64_t>({0,1,2,3,4,5,6,7,8}));
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanProperlyBalanceBlocks )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);

        // insert items until the 1st block is full
        unsigned int item = 1000;
        while (m_bitspace.span() == 1) {
            cut.insert(item++);
        }
        // insert items before the 1st block next
        item = 1000;
        for (int i = 0;i <100;++i) {
            cut.insert(--item);
        }
        // inspect block sizes
        ASSERT_TRUE(m_bitspace.span() <= 3);
        // validate tree after insertion
        std::uint64_t last_item = 0;
        for (auto it = cut.cbegin(); !it.is_end(); ++it) {
            ASSERT_TRUE(*it > last_item);
            last_item = *it;
        }
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanDeleteItems )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        std::set<int> items;
        
        // fill 10 pages with elements
        unsigned int item = 1000;
        while (m_bitspace.span() < 10) {
            items.insert(item);
            cut.insert(item++);     
        }

        srand(time(0));
        // erase 100 items next
        for (int i = 0; i < 100; ++i) {
            // pick a random value to erase
            int num = rand() % item;
            auto it = items.find(num);
            ASSERT_EQ(cut.erase_equal(num).first, it != items.end());
            if (it != items.end()) {
                items.erase(it);
            }
        }
        
        // validate tree after deletions
        std::uint64_t last_item = 0;
        for (auto it = cut.cbegin(); !it.is_end(); ++it) {
            ASSERT_TRUE(items.find(*it) != items.end());
            ASSERT_TRUE(*it > last_item);
            last_item = *it;
        }
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanFindLowerEqualBound )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        for (std::uint64_t i = 0; i < 10; ++i) {
            cut.insert(i * 3);
        }
        
        auto result_1 = cut.lower_equal_bound(7);
        ASSERT_TRUE(result_1.first);
        ASSERT_EQ(*result_1.first, 6u);

        auto result_2 = cut.lower_equal_bound(6);
        ASSERT_TRUE(result_2.first);
        ASSERT_EQ(*result_2.first, 6u);

        auto result_3 = cut.lower_equal_bound(17);
        ASSERT_TRUE(result_3.first);
        ASSERT_EQ(*result_3.first, 15u);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanFindUpperEqualBound )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        for (std::uint64_t i = 0; i < 10; ++i) {
            cut.insert(i * 3);
        }
        
        auto result_1 = cut.upper_equal_bound(7);
        ASSERT_TRUE(result_1.first);
        ASSERT_EQ(*result_1.first, 9u);

        auto result_2 = cut.upper_equal_bound(6);
        ASSERT_TRUE(result_2.first);
        ASSERT_EQ(*result_2.first, 6u);

        auto result_3 = cut.upper_equal_bound(17);
        ASSERT_TRUE(result_3.first);
        ASSERT_EQ(*result_3.first, 18u);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanEraseItemsByIteratorPair )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        for (std::uint64_t i = 0; i < 10; ++i) {
            cut.insert(i * 3);
        }
        
        // element '9' erased
        {
            auto it = cut.upper_equal_bound(7);
            cut.erase(it);
        }
        // element '6' erased
        {
            auto it = cut.upper_equal_bound(6);
            cut.erase(it);
        }
        // element '18' erased
        {
            auto it = cut.upper_equal_bound(17);
            cut.erase(it);
        }        

        std::vector<std::uint64_t> items;
        for (auto it = cut.cbegin(); !it.is_end(); ++it) {
            items.push_back(*it);
        }
        ASSERT_EQ(items, std::vector<std::uint64_t>({ 0,3,12,15,21,24,27 }));
    }

    struct ComplexItem
    {
        // primary key
        std::uint32_t m_key;
        float m_value;

        ComplexItem(std::uint32_t key, float value)
            : m_key(key)
            , m_value(value)
        {
        }

        static std::uint32_t getKey(const ComplexItem &item) {
            return item.m_key;
        }

        // Extracts key from construction args
        static std::uint32_t getKey(std::uint32_t key, float) {
            return key;
        }

        struct CompT {
            bool operator()(const ComplexItem &lhs, const ComplexItem &rhs) const {
                return lhs.m_key < rhs.m_key;
            }

            bool operator()(const ComplexItem &lhs, std::uint32_t key) const {
                return lhs.m_key < key;
            }

            bool operator()(std::uint32_t key, const ComplexItem &rhs) const {
                return key < rhs.m_key;
            }
        };
        
        struct EqualCompT {
            bool operator()(const ComplexItem &lhs, const ComplexItem &rhs) const {
                return lhs.m_key == rhs.m_key;
            }

            bool operator()(const ComplexItem &lhs, std::uint32_t key) const {
                return lhs.m_key == key;
            }

            bool operator()(std::uint32_t key, const ComplexItem &rhs) const {
                return key == rhs.m_key;
            }
        };
    };

    TEST_F( SGB_TreeTests , testSGBTreeCanStoreCompoundItems )
    {
        db0::SGB_Tree<ComplexItem, ComplexItem::CompT, ComplexItem::EqualCompT> cut(m_bitspace, page_size);
        // append as item
        cut.insert(ComplexItem(3, 1312.1f));
        // append as initializer list
        cut.emplace(10, 11.3f);

        // find by key
        auto it = cut.lower_equal_bound(6);
        ASSERT_TRUE(it.first);
        ASSERT_EQ(it.first->m_key, 3);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanFindEqualComplexItemByKey )
    {
        db0::SGB_Tree<ComplexItem, ComplexItem::CompT, ComplexItem::EqualCompT> cut(m_bitspace, page_size);
        cut.insert(ComplexItem(3, 1312.1f));        
        cut.emplace(10, 11.3f);

        auto it = cut.find_equal(10);
        ASSERT_TRUE(it.first != nullptr);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanFindLowerEqualWindow )
    {
        using SGB_TreeT = db0::SGB_Tree<SGB_KeyT<std::uint64_t> >;
        SGB_TreeT cut(m_bitspace, page_size);
        for (std::uint64_t i = 0; i < 50; ++i) {
            cut.insert(i * 3);
        }
        
        SGB_TreeT::WindowT window;
        cut.lower_equal_window(7, window);
        ASSERT_TRUE(window[1].first);
        ASSERT_EQ(*window[1].first, 6u);

        ASSERT_TRUE(window[0].first);
        ASSERT_EQ(*window[0].first, 3u);

        ASSERT_TRUE(window[2].first);
        ASSERT_EQ(*window[2].first, 9u);

        cut.lower_equal_window(0, window);
        ASSERT_TRUE(window[1].first);
        ASSERT_EQ(*window[1].first, 0u);

        ASSERT_FALSE(window[0].first);
        
        ASSERT_TRUE(window[2].first);
        ASSERT_EQ(*window[2].first, 3u);
    }
    
    TEST_F( SGB_TreeTests , testSGBTreeFindLowerEqualFromTwoNodes )
    {
        using SGB_TreeT = db0::SGB_Tree<SGB_KeyT<std::uint64_t> >;

        SGB_TreeT cut(m_bitspace, page_size);            
        std::uint64_t value = 0;
        // add elements until the 2nd node is created
        while (m_bitspace.span() < 2) {
            cut.insert(value);
            value += 3;
        }

        // Identify min / max from nodes
        auto last_node = --cut.cend_nodes();
        auto max_item = last_node->find_max({});
        SGB_TreeT::WindowT window;
        cut.lower_equal_window(*max_item + 10, window);
        ASSERT_TRUE(window[1].first);
        ASSERT_EQ(*window[1].first, *max_item);

        ASSERT_TRUE(window[0].first);
        ASSERT_EQ(*window[0].first, *max_item - 3);

        ASSERT_FALSE(window[2].first);

        auto key_item = last_node->keyItem();
        cut.lower_equal_window(key_item, window);
        ASSERT_TRUE(window[1].first);
        ASSERT_EQ(*window[1].first, key_item);

        ASSERT_TRUE(window[0].first);
        ASSERT_EQ(*window[0].first, key_item - 3);
        
        ASSERT_TRUE(window[2].first);
        ASSERT_EQ(*window[2].first, key_item + 3);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanMultipleIdenticalKeys )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        unsigned int size = 0;
        for (std::uint64_t i = 0; i < 50; ++i) {
            cut.insert(i * 3);
            size += 1;
            // add repeated elements
            unsigned int repeat = i / 10;
            for (unsigned int j = 0; j < repeat; ++j) {
                cut.insert(i * 3);
                size += 1;
            }
        }
        
        ASSERT_EQ(cut.size(), size);
        
        // validate tree after insertions
        std::uint64_t last_item = 0;
        for (auto it = cut.cbegin(); !it.is_end(); ++it) {
            ASSERT_TRUE(*it >= last_item);
            last_item = *it;
        }
    }
    
    TEST_F( SGB_TreeTests , testSGBTreeCanBeIteratedUnsorted )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        for (std::uint64_t i = 0; i < 200; ++i) {
            cut.insert(i * 3);
        }

        int count = 0;
        auto it = cut.rbegin_unsorted();
        while (!it.is_end()) {
            ++it;
            ++count;
        }
        ASSERT_EQ(count, 200);
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanRetrieveUpperSlice )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        for (std::uint64_t i = 0; i < 200; ++i) {
            cut.insert(i * 3);
        }

        // note that upper slice may contain elements <311 but must contain all elements >=311
        std::set<std::uint64_t> expected_elements;
        for (std::uint64_t i = 104; i < 200; ++i) {
            expected_elements.insert(i * 3);
        }

        auto it = cut.upper_equal_bound(311);
        auto slice = cut.upper_slice(it);
        while (!slice.is_end()) {
            auto it = expected_elements.find(*slice);
            if (it != expected_elements.end()) {
                expected_elements.erase(it);
            }
            ++slice;        
        }
        
        // make sure all expected elements have been found
        ASSERT_EQ(expected_elements.size(), 0);        
    }

    TEST_F( SGB_TreeTests , testSGBTreeNodesCanBeRebalanced )
    {
        using NodeT = typename db0::SGB_Tree<SGB_KeyT<std::uint64_t>>::NodeT;
        NodeT node_1(m_bitspace, 0, page_size);
        std::vector<std::uint64_t> values_1 { 5, 6, 7, 3, 4, 8, 9, 1, 2 };
        for (auto value : values_1) {
            node_1.modify().append({}, value);
        }

        NodeT node_2(m_bitspace, 10, page_size);
        std::vector<std::uint64_t> values_2 { 17, 18, 12 };
        for (auto value : values_2) {
            node_2.modify().append({}, value);
        }

        node_1.modify().rebalance(node_2.modify(), {});
        ASSERT_TRUE(abs(node_1->size() - node_2->size()) <= 4);

        // make sure all elements from node_1 are less than those from node_2
        for (auto it = node_1->cbegin(), end = node_1->cend(); it != end; ++it) {
            ASSERT_TRUE(*it < *node_2->cbegin());
        }
    }
    
    TEST_F( SGB_TreeTests , testSGBTreeStorageOverhead )
    {
        // this test checks how much of additional storage is required, on average
        // to store a single element when elements are added in random order
        db0::SGB_Tree<SGB_KeyT<std::uint64_t> > cut(m_bitspace, page_size);
        srand(123622u);
        // NOTE: change to 1M for testing at limits
        for (int i = 0; i < 100000; ++i) {
            cut.insert(rand());
            if (i > 0 && i % 100 == 0) {
                auto size_of_items = sizeof(std::uint64_t) * i;
                auto storage_used = m_bitspace.span() * page_size;
                auto overhead = (double)(storage_used - size_of_items) / (double)size_of_items;                
                auto max_overhead = 1.5 + (double)page_size / (double)size_of_items;
                // assure excess overhead is avoided
                ASSERT_TRUE(overhead < max_overhead);

            }
        }
    }

    TEST_F( SGB_TreeTests , testSGBTreeCanLowerEqualWindowLookupTest )
    {
        using SGB_TreeT = db0::SGB_Tree<SGB_KeyT<std::uint64_t> >;

        srand(893752u);
        SGB_TreeT cut(m_bitspace, page_size);
        std::vector<std::uint64_t> values;
        for (std::uint64_t i = 0; i < 100; ++i) {
            auto value = rand() % 1000;
            cut.insert(value);
            values.push_back(value);
        }

        SGB_TreeT::WindowT window;
        std::sort(values.begin(), values.end());
        
        for (int i = 0;i < 100;++i) {
            std::uint64_t key = rand() % 1500;
            auto result = cut.lower_equal_window(key, window);
            auto it = std::lower_bound(values.begin(), values.end(), key);
            if (it != values.end() && *it > key) {
                --it;
            }
            
            if (it == values.end()) {
                it = --values.end();
            }

            if (it != values.end()) {
                ASSERT_TRUE(result);
                ASSERT_TRUE(window[1].first != nullptr);
                ASSERT_EQ(*window[1].first, *it);

                if (it != values.begin()) {
                    auto prev = it;
                    --prev;

                    ASSERT_TRUE(window[0].first != nullptr);
                    ASSERT_EQ(*window[0].first, *prev);
                } else {
                    ASSERT_TRUE(window[0].first == nullptr);
                }

                auto next = it;
                ++next;
                if (next != values.end()) {
                    ASSERT_TRUE(window[2].first != nullptr);
                    ASSERT_EQ(*window[2].first, *next);
                } else {
                    ASSERT_TRUE(window[2].first == nullptr);
                }
            } else {
                ASSERT_FALSE(result);
            }
        }
    }
    
    TEST_F( SGB_TreeTests , testSGBTreeWorksWithNonStandardPageSize )
    {        
        auto large_page_size = 64 * 1024;
        db0::TestWorkspace workspace(large_page_size);
        auto memspace = workspace.getMemspace("my-test-prefix_2");

        auto base_addr = Address::fromOffset(0);
        db0::BitSpace<0x8000> bitspace(memspace.getPrefixPtr(), base_addr, large_page_size);
        // Note: CapacityT need to be upgraded to 32 bits to support large page sizes
        db0::SGB_Tree<SGB_KeyT<std::uint64_t>, std::less<std::uint64_t>, std::equal_to<std::uint64_t>, std::uint32_t> cut(
            bitspace, large_page_size
        );

        // let's insert 10 items
        for (std::uint64_t i = 0; i < 10; ++i) {
            cut.insert(i);
        }
        ASSERT_EQ(cut.size(), 10);
    }

    struct [[gnu::packed]] o_test_header: public o_fixed<o_test_header> 
    {
        std::uint32_t first;
        std::uint64_t second;    
    };

    TEST_F( SGB_TreeTests , testSGBTreeWorksWithNonNodeHeaders )
    {    
        db0::SGB_Tree<SGB_KeyT<std::uint64_t>, std::less<std::uint64_t>, std::equal_to<std::uint64_t>, 
            std::uint16_t, std::uint32_t, o_test_header>
        cut(m_bitspace, page_size);
        // let's insert 10 items
        for (std::uint64_t i = 0; i < 1000; ++i) {
            cut.insert(i);
        }

        // store values in headers
        unsigned int index = 0;
        for (auto node = cut.cbegin_nodes(); node != cut.cend_nodes(); ++node, ++index) {            
            node.modify().header().first = 123 * index;
            node.modify().header().second = 456 * index;
        }
        
        // validate values in headers
        index = 0;
        for (auto node = cut.cbegin_nodes(); node != cut.cend_nodes(); ++node, ++index) {
            auto header = node->header();
            ASSERT_EQ(header.first, 123 * index);
            ASSERT_EQ(header.second, 456 * index);
        }

        // validate tree elements
        index = 0;
        for (auto it = cut.cbegin(); !it.is_end(); ++it, ++index) {
            ASSERT_EQ(*it, index);
        }
    }
    
    TEST_F( SGB_TreeTests , testLowerEqualBoundFailingCase )
    {
        db0::SGB_Tree<SGB_KeyT<std::uint64_t>, std::less<std::uint64_t>, std::equal_to<std::uint64_t>, 
            std::uint16_t, std::uint32_t, o_test_header>
        
        cut(m_bitspace, page_size);
        ASSERT_TRUE(cut.lower_equal_bound(0).isEnd());
        cut.insert(0);
        ASSERT_FALSE(cut.lower_equal_bound(0).isEnd());
    }
    
}