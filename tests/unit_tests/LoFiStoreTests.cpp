#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/object_model/object/lofi_store.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{
    
    class LoFiStoreTest: public testing::Test
    {
    public:
    };
    
    TEST_F( LoFiStoreTest, testLoFiStoreCreatedInitiallyEmpty )
    {
        lofi_store<2> cut;
        ASSERT_EQ(cut.size(), 21u);
        ASSERT_FALSE(cut.isFull());
    }

    TEST_F( LoFiStoreTest, testLoFiStoreAddSequentialUntilFull )
    {
        lofi_store<2> cut; // capacity 21
        for (unsigned int i = 0; i < cut.size(); ++i) {
            ASSERT_FALSE(cut.isFull() && i < cut.size() - 1) << "Store reported full too early at i=" << i;
            auto idx = cut.add(i & 0x3u); // value fits 2 bits
            ASSERT_EQ(idx, i);
            ASSERT_TRUE(cut.isSet(idx));
            ASSERT_EQ(cut.get(idx), (i & 0x3u));
        }
        ASSERT_TRUE(cut.isFull());
    }

    TEST_F( LoFiStoreTest, testLoFiStoreReinterpretCastFromValue )
    {
        std::uint64_t value = 0;
        lofi_store<2>::fromValue(value).set(0, 3);
        ASSERT_TRUE(reinterpret_cast<lofi_store<2>&>(value).isSet(0));
        ASSERT_EQ(reinterpret_cast<lofi_store<2>&>(value).get(0), 3u);
    }
    
}