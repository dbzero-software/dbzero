#include <thread>
#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/collections/vector/SparseBoolMatrix.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class SparseBoolMatrixTests: public MemspaceTestBase
    {
    public:
    };
    
    TEST_F( SparseBoolMatrixTests , testSparseBoolMatrixSetGet )
    { 
        SparseBoolMatrix cut;
        cut.set({0,0}, true);
        cut.set({5,0}, true);
        cut.set({13,7}, true);
        cut.set({11,2}, true);
        cut.set({0,6}, true);
        cut.set({5,0}, false);

        std::vector<std::pair<std::pair<std::uint32_t, std::uint32_t>, bool> > expected = {
            {{0,0}, true}, {{0,6}, true}, {{5,0}, false}, {{11,2}, true}, {{13,7}, true}
        };

        for (auto &item: expected) {
            ASSERT_EQ(cut.get(item.first), item.second);
        }
    }
    
    TEST_F( SparseBoolMatrixTests , testSparseBoolMatrixWithDimLimtAndSortThreshold )
    { 
        SparseBoolMatrix cut(8, 2);
        cut.set({0,0}, true);
        cut.set({5,0}, true);
        cut.set({13,7}, true);
        cut.set({11,2}, true);
        cut.set({0,6}, true);
        cut.set({5,0}, false);

        std::vector<std::pair<std::pair<std::uint32_t, std::uint32_t>, bool> > expected = {
            {{0,0}, true}, {{0,6}, true}, {{5,0}, false}, {{11,2}, true}, {{13,7}, true}
        };

        for (auto &item: expected) {
            ASSERT_EQ(cut.get(item.first), item.second);
        }
    }

} 
