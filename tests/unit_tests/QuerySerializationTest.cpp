#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/range_tree/RangeTree.hpp>
#include <dbzero/core/collections/range_tree/RT_SortIterator.hpp>
#include <dbzero/core/collections/range_tree/RT_Serialization.hpp>
#include <dbzero/core/collections/range_tree/RT_RangeIterator.hpp>
#include <dbzero/core/collections/range_tree/RangeIteratorFactory.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>
#include <dbzero/core/collections/range_tree/RT_FTIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace tests

{

    using namespace db0;
    
    class QuerySerializationTest: public FixtureTestBase
    {
    public:
        using RangeTreeT = RangeTree<int, std::uint64_t>;
        using ItemT = typename RangeTreeT::ItemT;

        void runTestCase(std::function<void(RangeTreeT &, FT_BaseIndex<std::uint64_t> &)> test)
        {
            auto fixture = getFixture();
            // create with the limit of 8 items per range
            RangeTreeT rt(*fixture, 8);
            std::vector<ItemT> values_1 {
                { 99, 3 },  { 199, 5 }, { 13, 2 }, { 199, 7 }, { 142, 9}, { 152, 8}, { 27, 4 }
            };
            
            rt.bulkInsert(values_1.begin(), values_1.end());
            
            FixedObjectList shared_object_list(100);
            VObjectCache cache(*fixture, shared_object_list);

            // prepare full-text index to join with
            auto &ft_index = fixture->addResource<FT_BaseIndex<std::uint64_t> >(*fixture, cache);
            {
                auto batch_data = ft_index.beginBatchUpdate();
                batch_data->addTags(4, std::vector<std::uint64_t> { 1, 2, 3 });
                batch_data->addTags(3, std::vector<std::uint64_t> { 1, 2 });
                batch_data->addTags(8, std::vector<std::uint64_t> { 1, 2 });
                batch_data->flush();
            }
            test(rt, ft_index);
        }
    };
    
    TEST_F( QuerySerializationTest , testRangeTreeFTSortedIteratorCanBeSerialized )
    {
        auto test = [](RangeTreeT &rt, FT_BaseIndex<std::uint64_t> &ft_index) {
            auto ft_query = ft_index.makeIterator(1);
            std::vector<std::uint64_t> values;
            RT_SortIterator<int, std::uint64_t> cut(rt, std::move(ft_query));
            std::vector<std::byte> buf;
            cut.serialize(buf);
            ASSERT_TRUE(buf.size() > 0);
        };
        runTestCase(test);
    }
    
    TEST_F( QuerySerializationTest , testRangeTreeFTSortedIteratorCanBeDeserialized )
    {                
        auto test = [&](RangeTreeT &rt, FT_BaseIndex<std::uint64_t> &ft_index) {
            std::vector<std::byte> buf;
            auto ft_query = ft_index.makeIterator(1);
            std::vector<std::uint64_t> values;
            RT_SortIterator<int, std::uint64_t> cut(rt, std::move(ft_query));
            
            cut.serialize(buf);
            ASSERT_TRUE(buf.size() > 0);

            // deserialization part
            {
                auto iter = buf.cbegin(), end = buf.cend();
                auto iter_type = db0::serial::read<db0::SortedIteratorType>(iter, end);
                ASSERT_EQ(iter_type, db0::SortedIteratorType::RT_Sort);
                // deserialize-construct 
                auto cut = deserializeRT_SortIterator<int, std::uint64_t>(m_workspace, iter, end);
                // iterate to confirm it was deserialized correctly
                std::vector<std::uint64_t> values;
                while (!cut->isEnd()) {
                    std::uint64_t value;
                    cut->next(&value);
                    values.push_back(value); 
                }

                ASSERT_EQ(values, (std::vector<std::uint64_t> { 4, 3, 8 }));
            }
        };    
        runTestCase(test);
    }

}