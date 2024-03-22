#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_LookupTree.hpp>
#include <dbzero/core/memory/BitSpace.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    class SGB_LookupTreeTest: public testing::Test 
    {
    public:
        SGB_LookupTreeTest()
            : m_memspace(m_workspace.getMemspace("my-test-prefix_1"))
            // configure bitspace to use the entire 4kb page - i.e. 0x8000 bits
            , m_bitspace(m_memspace.getPrefixPtr(), 0, page_size)
        {
        }
        
        void SetUp() override {
            m_bitspace.clear();
        }

        void TearDown() override {
            m_bitspace.clear();
        }

    protected:
        db0::TestWorkspace m_workspace;
        static constexpr std::size_t page_size = 4096;
        db0::Memspace m_memspace;
        db0::BitSpace<0x8000> m_bitspace;
    };
    
    TEST_F( SGB_LookupTreeTest , testSGBLookupTreeCanBeCreatedOnBitspace )
    {
        db0::SGB_LookupTree<std::uint64_t> cut(m_bitspace, page_size, AccessType::READ_WRITE);
        ASSERT_TRUE(cut.getAddress() != 0);
    }

    TEST_F( SGB_LookupTreeTest , testSGBLookupTreeCanSortNodeAfterThresholdOfReadsIsExceeded )
    {
        srand(212319451u);
        auto sort_threshold = 4;
        db0::SGB_LookupTree<std::uint64_t> cut(m_bitspace, page_size, AccessType::READ_WRITE, sort_threshold);
        // insert 1000 random elements
        for (int i = 0; i < 1000; ++i) {
            cut.insert(rand() % 10000);
        }
        
        // make sure nodes are not sorted after insert
        for (auto node = cut.cbegin_nodes(); node != cut.cend_nodes(); ++node) {
            ASSERT_FALSE(node->header().m_flags[LookupHeaderFlags::sorted]);
        }

        // try looking up some element from the 1st node
        auto first_node = cut.cbegin_nodes();
        auto value = first_node->at(first_node->size() / 2);

        // lookup the value multiple times (more than the threshold)
        for (int i = 0; i < sort_threshold + 1; ++i) {
            cut.lower_equal_bound(value);
        }

        // make sure the 1st node was sorted
        std::uint32_t last_value = 0;
        auto step_  = first_node->step();
        for (auto it = first_node->cbegin(); it != first_node->cend(); it += step_) {
            ASSERT_TRUE(*it >= last_value);
            last_value = *it;
        }
    }

    TEST_F( SGB_LookupTreeTest , testSGBLookupTreeCanLookupInSortedNodes )
    {
        srand(212319451u);
        auto sort_threshold = 4;
        db0::SGB_LookupTree<std::uint64_t> cut(m_bitspace, page_size, AccessType::READ_WRITE, sort_threshold);
        // insert 1000 random elements
        for (int i = 0; i < 1000; ++i) {
            cut.insert(rand() % 10000);
        }

        // try looking up some element from the 1st node
        auto first_node = cut.cbegin_nodes();
        auto value = first_node->at(first_node->size() / 2);

        // lookup the value multiple times (more than the threshold)
        for (int i = 0; i < sort_threshold + 1; ++i) {
            cut.lower_equal_bound(value);
        }
        
        // lookup in a sorted node
        auto result = cut.lower_equal_bound(value);
        ASSERT_TRUE(result.first != nullptr);
        ASSERT_EQ(*result.first, value);
    }

    TEST_F( SGB_LookupTreeTest , testSGBLookupTreeCanInsertIntoSortedNodes )
    {
        srand(212319451u);
        auto sort_threshold = 4;
        db0::SGB_LookupTree<std::uint64_t> cut(m_bitspace, page_size, AccessType::READ_WRITE, sort_threshold);
        // insert 1000 random elements
        for (int i = 0; i < 1000; ++i) {
            cut.insert(rand() % 10000);
        }

        // try looking up some element from the 1st node
        auto first_node = cut.cbegin_nodes();
        auto value = first_node->at(first_node->size() / 2);

        // trigger sorting by looking up multiple times
        for (int i = 0; i < sort_threshold + 1; ++i) {
            cut.lower_equal_bound(value);
        }

        // insert into a sorted node
        auto new_value = value + 15;
        cut.insert(new_value);

        // look up inserted value
        auto result = cut.lower_equal_bound(new_value);
        ASSERT_TRUE(result.first != nullptr);
        ASSERT_EQ(*result.first, new_value);
    }

    TEST_F( SGB_LookupTreeTest , testLookupSpeedComparison )
    {
        // use 16kb page size
        auto page_size = 16 * 1024;
        auto sort_threshold = 3;
        BitSpace<0x8000> bitspace(m_memspace.getPrefixPtr(), 0, page_size);
        db0::SGB_LookupTree<std::uint64_t> cut(bitspace, page_size, AccessType::READ_WRITE, sort_threshold);

        srand(9376412u);
        std::vector<std::uint64_t> values;
        int item_count = 1000000;
        for (int i = 0; i < item_count; ++i) {
            values.push_back(rand());
        }
        
        // populate SGB_LookupTree and std::map
        std::set<std::uint64_t> std_set;
        for (auto value: values) {
            cut.insert(value);
            std_set.insert(value);
        }
        
        std::vector<std::uint64_t> lookup_values;
        int lookup_count = 100000;
        for (int i = 0; i < lookup_count; ++i) {
            lookup_values.push_back(rand());
        }
        
        // measure lookup speed
        {
            auto start = std::chrono::high_resolution_clock::now();
            auto count = 0;
            for (auto value: lookup_values) {
                auto result = cut.lower_equal_bound(value);
                if (result.first != nullptr) {
                    ++count;
                }
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

            std::cout << "Elements found: " << count << std::endl;
            std::cout << "SGB_LookupTree lookup " << lookup_count << " items in " << elapsed.count() << " ms" << std::endl;
        }    
        
        {
            auto start = std::chrono::high_resolution_clock::now();
            auto count = 0;
            for (auto value: lookup_values) {
                auto it = std_set.lower_bound(value);
                if (it != std_set.end()) {
                    ++count;
                }
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            std::cout << "Elements found: " << count << std::endl;
            std::cout << "std::set lookup " << lookup_count << " items in " << elapsed.count() << " ms" << std::endl;
        }            
    }

    TEST_F( SGB_LookupTreeTest , testSGBLookupTreeOneElementNodeIsMarkedAsSorted )
    {
        SGB_LookupTree<std::uint64_t> cut(m_bitspace, page_size, AccessType::READ_WRITE);
        cut.insert(1);
        ASSERT_TRUE(cut.cbegin_nodes()->is_sorted());
    }

}
