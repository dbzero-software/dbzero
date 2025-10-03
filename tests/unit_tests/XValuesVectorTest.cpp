#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/object_model/object/XValuesVector.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/value/XValue.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;

namespace tests

{
    
    class XValuesVectorTest: public testing::Test
    {
    public:
    };
    
    TEST_F( XValuesVectorTest, testSetGetRegularTypes )
    {   
        XValuesVector cut;
        cut.push_back({ 0, StorageClass::INT64, Value(123) });
        cut.push_back({ 12, StorageClass::DB0_BYTES, Value(912) });
        cut.push_back({ 3, StorageClass::DATETIME_TZ, Value(523) });

        std::pair<StorageClass, Value> val;
        ASSERT_TRUE(cut.tryGetAt(0, val));
        ASSERT_EQ(StorageClass::INT64, val.first);
        ASSERT_EQ(123, val.second.cast<std::uint64_t>());

        ASSERT_TRUE(cut.tryGetAt(3, val));
        ASSERT_EQ(StorageClass::DATETIME_TZ, val.first);
        ASSERT_EQ(523, val.second.cast<std::uint64_t>());

        ASSERT_TRUE(cut.tryGetAt(12, val));
        ASSERT_EQ(StorageClass::DB0_BYTES, val.first);
        ASSERT_EQ(912, val.second.cast<std::uint64_t>());
        
        ASSERT_FALSE(cut.tryGetAt(7, val));
        ASSERT_FALSE(cut.tryGetAt(1, val));
    }
    
}