// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/range_tree/IndexBase.hpp>
#include <dbzero/core/collections/range_tree/RangeTree.hpp>
#include <dbzero/core/collections/range_tree/RT_SortIterator.hpp>
#include <dbzero/core/collections/range_tree/RT_Serialization.hpp>
#include <dbzero/core/collections/range_tree/RT_RangeIterator.hpp>
#include <dbzero/core/collections/range_tree/RangeIteratorFactory.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>
#include <dbzero/core/collections/range_tree/RT_FTIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>

namespace tests

{

    using namespace db0;
    using namespace db0::object_model;
    using UniqueAddress = db0::UniqueAddress;
    
    class QuerySerializationTest: public FixtureTestBase
    {
    public:
        using RangeTreeT = RangeTree<int, UniqueAddress>;
        using ItemT = typename RangeTreeT::ItemT;
        using ShortTagT = db0::object_model::TagIndex::ShortTagT;

        static ShortTagT tag(std::uint64_t value) {
            return ShortTagT::fromOffset(value);
        }

        void runTestCase(std::function<void(IndexBase &, std::shared_ptr<RangeTreeT>,
            TagIndex::TagBaseIndexShortT &)> test)
        {
            auto fixture = getFixture();
            // create with the limit of 8 items per range
            IndexBase index(*fixture, IndexType::RangeTree, IndexDataType::Auto);
            auto rt = std::make_shared<RangeTreeT>(*fixture, 8);
            index.modify().m_index_addr = rt->getAddress();
            std::vector<ItemT> values_1 {
                { 99, makeUniqueAddr(3, 1) },  { 199, makeUniqueAddr(5, 1) }, { 13, makeUniqueAddr(2, 1) }, 
                { 199, makeUniqueAddr(7, 1) }, { 142, makeUniqueAddr(9, 1) }, { 152, makeUniqueAddr(8, 1) }, 
                { 27, makeUniqueAddr(4, 1) }
            };
            
            rt->bulkInsert(values_1.begin(), values_1.end());
            
            FixedObjectList shared_object_list(100);
            db0::object_model::ClassFactory class_factory(fixture);
            db0::object_model::EnumFactory enum_factory(fixture);
            VObjectCache cache(*fixture, shared_object_list);
            
            // prepare full-text index to join with
            using TagIndex = db0::object_model::TagIndex;
            auto &tag_index = fixture->addResource<TagIndex>(
                *fixture, class_factory, enum_factory, fixture->getLimitedStringPool(), cache, fixture->addMutationHandler()
            );
            auto &ft_index = tag_index.getBaseIndexShort();
            {
                auto batch_data = ft_index.beginBatchUpdate();
                batch_data->addTags({ makeUniqueAddr(4, 1), nullptr }, std::vector<ShortTagT> { tag(1), tag(2), tag(3) });
                batch_data->addTags({ makeUniqueAddr(3, 1), nullptr }, std::vector<ShortTagT> { tag(1), tag(2) });
                batch_data->addTags({ makeUniqueAddr(8, 1), nullptr }, std::vector<ShortTagT> { tag(1), tag(2) });
                batch_data->flush();
            }
            test(index, rt, ft_index);
        }
    };
    
    TEST_F( QuerySerializationTest , testRangeTreeFTSortedIteratorCanBeSerialized )
    {
        auto test = [](IndexBase &index, std::shared_ptr<RangeTreeT> rt,
            TagIndex::TagBaseIndexShortT &ft_index) {
            auto ft_query = ft_index.makeIterator<UniqueAddress>(tag(1));
            std::vector<std::uint64_t> values;
            RT_SortIterator<int, UniqueAddress> cut(index, rt, std::move(ft_query));
            std::vector<std::byte> buf;
            cut.serialize(buf);
            ASSERT_TRUE(buf.size() > 0);
        };
        runTestCase(test);
    }
    
    TEST_F( QuerySerializationTest , testRangeTreeFTSortedIteratorCanBeDeserialized )
    {                
        auto test = [&](IndexBase &index, std::shared_ptr<RangeTreeT> rt,
            TagIndex::TagBaseIndexShortT &ft_index) {
            std::vector<std::byte> buf;
            auto ft_query = ft_index.makeIterator<UniqueAddress>(tag(1));
            std::vector<std::uint64_t> values;
            RT_SortIterator<int, UniqueAddress> cut(index, rt, std::move(ft_query));
            
            cut.serialize(buf);
            ASSERT_TRUE(buf.size() > 0);

            // deserialization part
            {
                auto iter = buf.cbegin(), end = buf.cend();
                auto iter_type = db0::serial::read<db0::SortedIteratorType>(iter, end);
                ASSERT_EQ(iter_type, db0::SortedIteratorType::RT_Sort);
                // deserialize-construct
                auto cut = deserializeRT_SortIterator<int, UniqueAddress>(m_workspace, iter, end);
                // iterate to confirm it was deserialized correctly
                std::vector<std::uint64_t> values;
                while (!cut->isEnd()) {
                    UniqueAddress value;
                    cut->next(&value);
                    ASSERT_EQ(value.getInstanceId(), 1);                    
                    // compare offsets only
                    values.push_back(value.getOffset());
                }

                ASSERT_EQ(values, (std::vector<std::uint64_t> { 4, 3, 8 }));
            }
        };    
        runTestCase(test);
    }

    TEST_F( QuerySerializationTest , testCompositeTagPathIteratorCanBeDeserializedFromRoot )
    {
        auto fixture = getFixture();
        FixedObjectList shared_object_list(100);
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        VObjectCache cache(*fixture, shared_object_list);

        auto &tag_index = fixture->addResource<TagIndex>(
            *fixture, class_factory, enum_factory, fixture->getLimitedStringPool(), cache, fixture->addMutationHandler()
        );

        auto child = tag_index.addComposite(nullptr, tag(11));
        auto grandchild = child->addComposite(nullptr, tag(22));
        {
            auto batch_data = grandchild->getBaseIndexShort().beginBatchUpdate();
            batch_data->addTags({ makeUniqueAddr(101, 1), nullptr }, std::vector<ShortTagT> { tag(33) });
            batch_data->flush();
        }
        {
            auto batch_data = tag_index.getBaseIndexShort().beginBatchUpdate();
            batch_data->addTags({ makeUniqueAddr(202, 1), nullptr }, std::vector<ShortTagT> { tag(33) });
            batch_data->flush();
        }

        auto ft_query = tag_index.makeIterator(std::vector<TagIndex::ShortTagT> { tag(11), tag(22), tag(33) });
        ASSERT_TRUE(ft_query);

        std::vector<std::byte> buf;
        ft_query->serialize(buf);

        auto iter = buf.cbegin(), end = buf.cend();
        auto cut = deserializeFT_Iterator<UniqueAddress>(m_workspace, iter, end);
        ASSERT_TRUE(cut);
        ASSERT_FALSE(cut->isEnd());

        UniqueAddress value;
        cut->next(&value);
        ASSERT_EQ(value, makeUniqueAddr(101, 1));
        ASSERT_TRUE(cut->isEnd());
    }

    TEST_F( QuerySerializationTest , testMissingTagIteratorCanBeDeserializedAfterTagIsCreated )
    {
        auto fixture = getFixture();
        FixedObjectList shared_object_list(100);
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        VObjectCache cache(*fixture, shared_object_list);

        auto &tag_index = fixture->addResource<TagIndex>(
            *fixture, class_factory, enum_factory, fixture->getLimitedStringPool(), cache, fixture->addMutationHandler()
        );

        auto ft_query = tag_index.makeMissingIterator(std::vector<TagIndex::ShortTagT> { tag(44) });
        ASSERT_TRUE(ft_query);
        ASSERT_TRUE(ft_query->isEnd());

        std::vector<std::byte> buf;
        ft_query->serialize(buf);

        {
            auto batch_data = tag_index.getBaseIndexShort().beginBatchUpdate();
            batch_data->addTags({ makeUniqueAddr(303, 1), nullptr }, std::vector<ShortTagT> { tag(44) });
            batch_data->flush();
        }

        auto iter = buf.cbegin(), end = buf.cend();
        auto cut = deserializeFT_Iterator<UniqueAddress>(m_workspace, iter, end);
        ASSERT_TRUE(cut);
        ASSERT_FALSE(cut->isEnd());

        UniqueAddress value;
        cut->next(&value);
        ASSERT_EQ(value, makeUniqueAddr(303, 1));
        ASSERT_TRUE(cut->isEnd());
    }

}
