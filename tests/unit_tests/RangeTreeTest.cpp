// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/range_tree/RangeTree.hpp>
#include <dbzero/core/collections/range_tree/RT_SortIterator.hpp>
#include <dbzero/core/collections/range_tree/RT_RangeIterator.hpp>
#include <dbzero/core/collections/range_tree/RangeIteratorFactory.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/core/collections/range_tree/RT_FTIterator.hpp>

namespace tests

{

    using namespace db0;
    
    class RangeTreeTest: public MemspaceTestBase
    {
    public:
    };

    TEST_F( RangeTreeTest , testBulkInsertIntoEmptyRangeTree )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        RangeTreeT cut(memspace);
        std::vector<ItemT> values {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };
        cut.bulkInsert(values.begin(), values.end());
        ASSERT_EQ(cut.getRangeCount(), 1u);
    }

    TEST_F( RangeTreeTest , testSingleRangeIsSortedByValues )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        RangeTreeT cut(memspace);
        std::vector<ItemT> values {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };
        cut.bulkInsert(values.begin(), values.end());
        auto range = cut.beginRange();
        std::uint64_t last = 0;
        for (auto item: *range) {
            ASSERT_TRUE(last <= item.m_value);
            last = item.m_value;
        }
    }

    TEST_F( RangeTreeTest , testRangeSizeLimitCanBeSpecified )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
            
        auto memspace = getMemspace();
        // create with the limit of 8 items per range
        RangeTreeT cut(memspace, 8);
        std::vector<ItemT> values {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };        
        cut.bulkInsert(values.begin(), values.end());
        ASSERT_EQ(cut.getRangeCount(), 2u);
    }

    TEST_F( RangeTreeTest , testNewElementsCanBeInsertedIntoExistingRange )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 8 items per range
        RangeTreeT cut(memspace, 8);
        std::vector<ItemT> values_1 {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };        
        cut.bulkInsert(values_1.begin(), values_1.end());
        
        // append 1 element to the last range
        std::vector<ItemT> values_2 {
            { 94134, 22 }
        };
        cut.bulkInsert(values_2.begin(), values_2.end());
        ASSERT_EQ(cut.getRangeCount(), 2u);
    }

    TEST_F( RangeTreeTest , testCanIterateOverCreatedRanges )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 8 items per range
        RangeTreeT cut(memspace, 8);
        std::vector<ItemT> values_1 {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };
        
        cut.bulkInsert(values_1.begin(), values_1.end());
        auto range = cut.beginRange();
        std::stringstream _str;
        while (!range.isEnd()) {
            _str << range.getBounds().first->m_key << ":" << range->size() << ";";
            range.next();
        }
        ASSERT_EQ(_str.str(), "0:8;3312:2;");
    }

    TEST_F( RangeTreeTest , testCanIterateOverCreatedRangesInDescOrder )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 4 items per range
        RangeTreeT cut(memspace, 4);
        std::vector<ItemT> values_1 {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };
        
        cut.bulkInsert(values_1.begin(), values_1.end());
        auto range = cut.beginRange(false);
        std::stringstream _str;
        while (!range.isEnd()) {
            _str << range.getBounds().first->m_key << ":" << range->size() << ";";
            range.next();
        }
        ASSERT_EQ(_str.str(), "3312:2;152:4;0:4;");
    }

    TEST_F( RangeTreeTest , testCanExplodeExistingRanges )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 8 items per range
        RangeTreeT cut(memspace, 8);
        std::vector<ItemT> values_1 {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };
        
        cut.bulkInsert(values_1.begin(), values_1.end());

        // explode the 1st range by inserting more elements
        std::vector<ItemT> values_2 {
            { 29, 4 }, { 199, 3 }, { 142, 8}
        };
        cut.bulkInsert(values_2.begin(), values_2.end());
        ASSERT_EQ(cut.getRangeCount(), 3);
    }
    
    TEST_F( RangeTreeTest , testCanUpdateExistingRange )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 8 items per range
        RangeTreeT cut(memspace, 8);
        std::vector<ItemT> values_1 {
            { 27, 4 }, { 99, 3 }, { 152, 8}, { 199, 3 },
        };
        
        cut.bulkInsert(values_1.begin(), values_1.end());

        // unique elements should be added to an existing range
        std::vector<ItemT> values_2 {
            { 13, 4 }, { 199, 3 }, { 142, 8}
        };
        cut.bulkInsert(values_2.begin(), values_2.end());
        auto range = cut.beginRange();
        std::stringstream _str;
        while (!range.isEnd()) {
            _str << range.getBounds().first->m_key << ":" << range->size() << ";";
            range.next();
        }
        ASSERT_EQ(_str.str(), "13:6;");
    }
    
    TEST_F( RangeTreeTest , testRangeTreeValuesCanBeSortedUsingSortIterator )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        // create with the limit of 8 items per range
        auto rt = std::make_shared<RangeTreeT>(memspace, 8);
        std::vector<ItemT> values_1 {
            { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 }
        };
        
        rt->bulkInsert(values_1.begin(), values_1.end());
        
        std::vector<std::uint64_t> values;
        RT_SortIterator<int, std::uint64_t> cut(index, rt);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.push_back(value); 
        }

        ASSERT_EQ(values, (std::vector<std::uint64_t> { 2, 4, 3, 9, 8, 5, 7 }));
    }

    TEST_F( RangeTreeTest , testSortIteratorCanSortDescending )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 4);
        std::vector<ItemT> values_1 {
            { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 },
            { 123, 6}, { 148, 11 }, { 391, 10 }, { 9234, 12 }
        };

        rt->bulkInsert(values_1.begin(), values_1.end());
        std::vector<std::uint64_t> values;
        RT_SortIterator<int, std::uint64_t> cut(index, rt, false);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.push_back(value); 
        }
        
        ASSERT_EQ(values, (std::vector<std::uint64_t> { 12, 10, 7, 5, 8, 11, 9, 6, 3, 4, 2 }));
    }

    TEST_F( RangeTreeTest , testRangeTreeSotedIteratorCanBeJoinedWithFtIndex )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;

        auto memspace = getMemspace();
        // create with the limit of 8 items per range
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 8);
        std::vector<ItemT> values_1 {
            { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 }
        };
        
        rt->bulkInsert(values_1.begin(), values_1.end());
        
        FixedObjectList shared_object_list(100);
        VObjectCache cache(memspace, shared_object_list);
        
        // prepare full-text index to join with
        FT_BaseIndex<std::uint64_t, std::uint64_t> ft_index(memspace, cache);
        {
            auto batch_data = ft_index.beginBatchUpdate();
            batch_data->addTags({4, nullptr}, std::vector<std::uint64_t> { 1, 2, 3 });
            batch_data->addTags({3, nullptr}, std::vector<std::uint64_t> { 1, 2 });
            batch_data->addTags({8, nullptr}, std::vector<std::uint64_t> { 1, 2 });
            batch_data->flush();
        }
        
        auto ft_query = ft_index.makeIterator(1);
        std::vector<std::uint64_t> values;
        RT_SortIterator<int, std::uint64_t> cut(index, rt, std::move(ft_query));
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.push_back(value); 
        }

        ASSERT_EQ(values, (std::vector<std::uint64_t> { 4, 3, 8 }));
    }

    TEST_F( RangeTreeTest , testRangeTreeLowerBound )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
            
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges        
        RangeTreeT rt(memspace, 4);
        std::vector<ItemT> values_1 {
            { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 },
            { 123, 6}, { 148, 11 }, { 391, 10 }, { 9234, 12 }
        };

        rt.bulkInsert(values_1.begin(), values_1.end());
        std::vector<std::uint64_t> lookup_keys { 100, 150, 198, 199, 200, 300, 10000 };
        std::stringstream _str;
        for (auto key: lookup_keys) {
            auto it = rt.lowerBound(key, true);
            ASSERT_FALSE(it.isEnd());
            _str << it.getBounds().first->m_key << ",";
        }

        ASSERT_EQ(_str.str(), "13,142,142,142,199,199,199,");
    }

    TEST_F( RangeTreeTest , testRangeTreeCanIterateAllValues )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
                
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges
        RangeTreeT rt(memspace, 4);
        std::vector<ItemT> values_1 {
            { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 },
            { 123, 6}, { 148, 11 }, { 391, 10 }, { 9234, 12 }
        };
        
        rt.bulkInsert(values_1.begin(), values_1.end());
        std::unordered_set<std::uint64_t> values;
        auto it_range = rt.beginRange(true);
        while (!it_range.isEnd()) {
            auto it = it_range->makeIterator();
            assert(it);
            assert(!it->isEnd());
            while (!it->isEnd()) {
                std::uint64_t value;
                it->next(&value);
                values.insert(value);
            }
            it_range.next();            
        }
        ASSERT_EQ(values, (std::unordered_set<std::uint64_t> { 12, 10, 7, 5, 8, 11, 9, 6, 3, 4, 2 }));
    }

    TEST_F( RangeTreeTest , testInclusiveRangeFilterCanBeCreated )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
                
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 4);
        std::vector<ItemT> values_1 {
            { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 },
            { 123, 6}, { 148, 11 }, { 391, 10 }, { 9234, 12 }
        };

        rt->bulkInsert(values_1.begin(), values_1.end());
        std::unordered_set<std::uint64_t> values;
        RT_RangeIterator<int, std::uint64_t> cut(index, rt, 100, true, 199, true);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.insert(value); 
        }
        
        ASSERT_EQ(values, (std::unordered_set<std::uint64_t> { 7, 5, 8, 9, 11, 6 }));
    }

    TEST_F( RangeTreeTest , testFTCompliantRangeFilterIteratorCanBeCreated )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 4);
        std::vector<ItemT> values_1 {
            { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 },
            { 123, 6}, { 148, 11 }, { 391, 10 }, { 9234, 12 }
        };

        rt->bulkInsert(values_1.begin(), values_1.end());
        std::unordered_set<std::uint64_t> values;
        RT_FTIterator<int, std::uint64_t> cut(index, rt, 100, true, 199, true, false);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.insert(value); 
        }
        
        ASSERT_EQ(values, (std::unordered_set<std::uint64_t> { 7, 5, 8, 9, 11, 6 }));
    }

    TEST_F( RangeTreeTest , testRangeFilterIssue_1 )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 128);
        std::vector<ItemT> values_1 {
            {666, 0}, {22, 1}, {99, 2}, {888, 3}, {444, 4}
        };

        rt->bulkInsert(values_1.begin(), values_1.end());
        std::unordered_set<std::uint64_t> values;
        RT_RangeIterator<int, std::uint64_t> cut(index, rt, 22, true, 444, true);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.insert(value); 
        }
        
        ASSERT_EQ(values, (std::unordered_set<std::uint64_t> { 1, 2, 4 }));
    }

    TEST_F( RangeTreeTest , testRangeTreeInclusiveLowerBoundIssue_1 )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
            
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges
        RangeTreeT rt(memspace, 128);
        std::vector<ItemT> values_1 {
            {666, 0}, {22, 1}, {99, 2}, {888, 3}, {444, 4}
        };

        rt.bulkInsert(values_1.begin(), values_1.end());
        auto it = rt.lowerBound(22, true);
        ASSERT_FALSE(it.isEnd());
    }

    TEST_F( RangeTreeTest , testRangeTreeBulkInsertNull )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();    
        RangeTreeT rt(memspace, 128);
        std::vector<std::uint64_t> values_1 { 0, 1, 2, 3, 4 };        
        rt.bulkInsertNull(values_1.begin(), values_1.end());
        ASSERT_EQ(rt.size(), 5u);
        ASSERT_FALSE(rt.hasAnyNonNull());
    }

    TEST_F( RangeTreeTest , testRangeTreeRangeIteratorUnboundQueryOverNullElements )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 128);
        std::vector<std::uint64_t> values_1 { 0, 1, 2, 3, 4 };        
        rt->bulkInsertNull(values_1.begin(), values_1.end());

        RangeIteratorFactory<int, std::uint64_t> factory(index, rt);
        std::unordered_set<std::uint64_t> x_values;
        auto it = factory.createBaseIterator();
        std::uint64_t next_value;
        while (!it->isEnd()) {
            it->next(&next_value);
            x_values.insert(next_value);        
        }        
        ASSERT_EQ(x_values, (std::unordered_set<std::uint64_t> { 0, 1, 2, 3, 4 }));
    }
    
    TEST_F( RangeTreeTest , testFTCompliantRangeFilterOverNullElements )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        // create with the limit of 4 items per range, make 3 ranges
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 4);
        std::vector<std::uint64_t> values_1 { 0, 1, 2, 3, 4 };
        rt->bulkInsertNull(values_1.begin(), values_1.end());

        std::unordered_set<std::uint64_t> values;
        RangeIteratorFactory<int, std::uint64_t> factory(index, rt);
        auto it = factory.createFTIterator();
        while (!it->isEnd()) {
            std::uint64_t value;
            it->next(&value);
            values.insert(value); 
        }
        
        ASSERT_EQ(values, (std::unordered_set<std::uint64_t> { 0, 1, 2, 3, 4 }));
    }

    TEST_F( RangeTreeTest , testRangeTreeBulkEraseExistingNonNull )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 4);
        std::vector<ItemT> values_1 {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94}
        };
        rt->bulkInsert(values_1.begin(), values_1.end());
        
        std::vector<ItemT> erase_values {
            { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 123, 9 }, { 152, 12 }, { 3312, 19, }
        };
        rt->bulkErase(erase_values.begin(), erase_values.end());

        std::vector<std::uint64_t> values;
        RT_SortIterator<int, std::uint64_t> cut(index, rt);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.push_back(value); 
        }

        ASSERT_EQ(values, (std::vector<std::uint64_t> { 0, 8, 444, 94 }));
    }
    
    TEST_F( RangeTreeTest , testRangeTreeBulkEraseIgnoresNonExistingItems )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto);
        auto rt = std::make_shared<RangeTreeT>(memspace, 4);
        std::vector<ItemT> values_1 {
            { 0, 0 }, { 27, 4 }, { 42134, 44 }, { 99, 3 }, { 152, 8}, { 123, 9 }, { 152, 12 }, 
            { 3312, 19, }, { 921, 444 }, { 1923, 94 }
        };
        rt->bulkInsert(values_1.begin(), values_1.end());
        
        // NOTE: both key and value must match for the item to be erased
        std::vector<ItemT> erase_values {
            { 27, 3 }, { 42134, 44 }, { 98, 3 }, { 123, 9 }, { 152, 12 }, { 3312, 20 }
        };
        rt->bulkErase(erase_values.begin(), erase_values.end());
        
        std::vector<std::uint64_t> values;
        RT_SortIterator<int, std::uint64_t> cut(index, rt);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.push_back(value); 
        }
        
        ASSERT_EQ(values, (std::vector<std::uint64_t> { 0, 4, 3, 8, 444, 94, 19 }));
    }
    
    TEST_F( RangeTreeTest , testRangeTreeBulkEraseNull )
    {
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;
        
        auto memspace = getMemspace();
        IndexBase index(memspace, db0::IndexType::Unknown, db0::IndexDataType::Auto); 
        auto rt = std::make_shared<RangeTreeT>(memspace, 128);
        std::vector<std::uint64_t> values_1 { 0, 1, 2, 3, 4 };
        rt->bulkInsertNull(values_1.begin(), values_1.end());

        std::vector<std::uint64_t> erase_values { 0, 7, 1, 4 };
        rt->bulkEraseNull(erase_values.begin(), erase_values.end());

        std::vector<std::uint64_t> values;
        RT_SortIterator<int, std::uint64_t> cut(index, rt);
        while (!cut.isEnd()) {
            std::uint64_t value;
            cut.next(&value);
            values.push_back(value); 
        }
        
        ASSERT_EQ(values, (std::vector<std::uint64_t> { 3, 2 }));
    }
    
} 
