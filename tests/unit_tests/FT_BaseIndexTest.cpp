#include <gtest/gtest.h>
#include <utils/WorkspaceBaseTest.hpp>
#include <dbzero/core/collections/full_text/FT_BaseIndex.hpp>

namespace tests

{

	using namespace db0;

    class FT_BaseIndexTest: public WorkspaceBaseTest
    {
    };
    
	TEST_F( FT_BaseIndexTest , testFT_BaseIndexCanBePopulatedWithBatchOpertionBuilder )
	{
        auto memspace = getMemspace();
        FT_BaseIndex cut(memspace);
        auto batch_data = cut.beginBatchUpdate();
        batch_data->addTags(123, std::vector<std::uint64_t> { 1, 2, 3 });
        batch_data->addTags(99, std::vector<std::uint64_t> { 4, 5 });
        batch_data->flush();

        ASSERT_EQ(cut.size(), 5u);
    }

	TEST_F( FT_BaseIndexTest , testFT_BaseIndexCanRemoveTagsWithOpertionBuilder )
	{
        auto memspace = getMemspace();
        FT_BaseIndex cut(memspace);
        // initalize with some tags
        {
            auto batch_data = cut.beginBatchUpdate();
            batch_data->addTags(123, std::vector<std::uint64_t> { 1, 2, 3 });
            batch_data->addTags(99, std::vector<std::uint64_t> { 4, 5 });
            batch_data->flush();
        }

        // remove some tags with batch operation
        {
            auto batch_data = cut.beginBatchUpdate();
            batch_data->removeTags(123, std::vector<std::uint64_t> { 1, 2 });
            batch_data->flush();
        }

        ASSERT_EQ(cut.size(), 3u);
    }

	TEST_F( FT_BaseIndexTest , testFT_BaseIndexCanBeOpenedFromMemspace )
	{
        auto memspace = getMemspace();
        std::uint64_t ft_base_index_addr = 0;
        {
            FT_BaseIndex cut(memspace);
            auto batch_data = cut.beginBatchUpdate();
            batch_data->addTags(123, std::vector<std::uint64_t> { 1, 2, 3 });
            batch_data->addTags(99, std::vector<std::uint64_t> { 4, 5 });
            batch_data->flush();
            ft_base_index_addr = cut.getAddress();
        }
        
        // Open existing
        FT_BaseIndex cut(memspace.myPtr(ft_base_index_addr));
        ASSERT_EQ(cut.size(), 5u);
    }

}