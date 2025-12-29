// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>

namespace tests

{

	using namespace db0;
    using UniqueAddress = db0::UniqueAddress;

    class FT_BaseIndexTest: public MemspaceTestBase
    {
    protected:
        FT_BaseIndexTest()
            : m_shared_object_list(100)
            , m_memspace(getMemspace()) 
            , m_cache(m_memspace, m_shared_object_list)
        {
        }

        FixedObjectList m_shared_object_list;
        Memspace m_memspace;
        VObjectCache m_cache;
    };
    
	TEST_F( FT_BaseIndexTest , testFT_BaseIndexCanBePopulatedWithBatchOpertionBuilder )
	{
        FT_BaseIndex<std::uint64_t> cut(m_memspace, m_cache);
        auto batch_data = cut.beginBatchUpdate();
        batch_data->addTags({ makeUniqueAddr(123, 1), nullptr }, std::vector<std::uint64_t> { 1, 2, 3 });
        batch_data->addTags({ makeUniqueAddr(99, 1), nullptr }, std::vector<std::uint64_t> { 4, 5 });
        batch_data->flush();

        ASSERT_EQ(cut.size(), 5u);
    }

	TEST_F( FT_BaseIndexTest , testFT_BaseIndexCanRemoveTagsWithOpertionBuilder )
	{        
        FT_BaseIndex<std::uint64_t> cut(m_memspace, m_cache);
        // initalize with some tags
        {
            auto batch_data = cut.beginBatchUpdate();
            batch_data->addTags({ makeUniqueAddr(123, 1), nullptr }, std::vector<std::uint64_t> { 1, 2, 3 });
            batch_data->addTags({ makeUniqueAddr(99, 1), nullptr }, std::vector<std::uint64_t> { 4, 5 });
            batch_data->flush();
        }

        // remove some tags with batch operation
        {
            auto batch_data = cut.beginBatchUpdate();
            batch_data->removeTags({ makeUniqueAddr(123, 1), nullptr }, std::vector<std::uint64_t> { 1, 2 });
            batch_data->flush();
        }

        ASSERT_EQ(cut.size(), 3u);
    }

	TEST_F( FT_BaseIndexTest , testFT_BaseIndexCanBeOpenedFromMemspace )
	{        
        Address ft_base_index_addr = {};
        {
            FT_BaseIndex<std::uint64_t> cut(m_memspace, m_cache);
            auto batch_data = cut.beginBatchUpdate();
            batch_data->addTags({ makeUniqueAddr(123, 1), nullptr }, std::vector<std::uint64_t> { 1, 2, 3 });
            batch_data->addTags({ makeUniqueAddr(99, 1), nullptr }, std::vector<std::uint64_t> { 4, 5 });
            batch_data->flush();
            ft_base_index_addr = cut.getAddress();
        }
        
        // Open existing
        FT_BaseIndex<std::uint64_t> cut(m_memspace.myPtr(ft_base_index_addr), m_cache);
        ASSERT_EQ(cut.size(), 5u);
    }

}