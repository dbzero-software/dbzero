#include <gtest/gtest.h>
#include <algorithm>
#include <dbzero/core/utils/bisect.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    TEST( BisectTest , testBisectLowerEqual )
    {
        std::vector<int> data = { 3, 4, 5, 6, 7, 1, 2, 9, 123, 412, 5, 6 };
        std::vector<std::pair<int, int> > queries {
            { 999, 412 }, { 5, 5 }, { 11, 9 }, { 1, 1}
        };

        std::sort(data.begin(), data.end());
        for (auto q: queries) {
            auto it = bisect::lower_equal(data.begin(), data.end(), q.first, std::less<int>());
            ASSERT_EQ(*it, q.second);
        }

        // special cases
        ASSERT_EQ(bisect::lower_equal(data.end(), data.end(), 999, std::less<int>()), data.end());
        ASSERT_EQ(bisect::lower_equal(data.begin(), data.end(), 0, std::less<int>()), data.end());
    }
    
    TEST( BisectTest , testBisectLowerEqualSingleElementCase )
    {
        std::vector<int> data = { 0 };
        auto it = bisect::lower_equal(data.begin(), data.end(), 0, std::less<int>());
        ASSERT_EQ(it, data.begin());
    }

}