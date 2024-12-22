#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class SparseIndexQueryTest: public testing::Test
    {    
    };

    TEST_F( SparseIndexQueryTest , testSparseIndexQueryNoDiffs )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_cut(16 * 1024);
        sparse_index.emplace(1, 1, 1);
        sparse_index.emplace(1, 3, 17);
        sparse_index.emplace(4, 7, 2343);
        
        // existing item
        {
            SparseIndexQuery cut(sparse_index, diff_cut, 1, 5);
            ASSERT_EQ(cut.first(), 17);
        }

        // non-existing item
        {
            SparseIndexQuery cut(sparse_index, diff_cut, 2, 5);
            ASSERT_EQ(cut.first(), 0);
        }
    }
    
}