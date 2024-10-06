#include <gtest/gtest.h>
#include <dbzero/core/collections/pools/RC_LimitedPool.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/core/serialization/string.hpp>

using namespace std;

namespace tests

{
    
    class LimitedPoolTest: public MemspaceTestBase
    {
    };
    
    TEST_F( LimitedPoolTest , testLimitedPoolCanStoreSimpleInts )
    {
        using PoolT = db0::pools::LimitedPool<db0::o_simple<int> >;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
                
        PoolT pool(memspace);
        auto addr_0 = pool.add(123);
        auto addr_1 = pool.add(456);
        ASSERT_NE(addr_0, addr_1);
    }
    
    TEST_F( LimitedPoolTest , testLimitedPoolFetchSimpleInt )
    {
        using PoolT = db0::pools::LimitedPool<db0::o_simple<int> >;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        
        PoolT pool(memspace);
        auto addr_0 = pool.add(123);
        auto addr_1 = pool.add(456);
        int value;
        value = pool.fetch<int>(addr_0);
        ASSERT_EQ(value, 123);

        value = pool.fetch<int>(addr_1);
        ASSERT_EQ(value, 456);
    }

    TEST_F( LimitedPoolTest , testLimitedStringPoolAddAndFetch )
    {
        using PoolT = db0::pools::RC_LimitedStringPool;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        
        PoolT pool(memspace, memspace);
        auto addr_0 = pool.addRef("anna");
        auto addr_1 = pool.addRef("maria");

        // retrieve as std::string
        ASSERT_EQ(pool.fetch(addr_0), "anna");        
        ASSERT_EQ(pool.fetch(addr_1), "maria");
    }

    TEST_F( LimitedPoolTest , testLP_StringBoolConversion )
    {
        using PoolT = db0::pools::RC_LimitedStringPool;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        
        PoolT pool(memspace, memspace);
        db0::LP_String str_0;
        db0::LP_String str_1 = pool.addRef("anna");
        ASSERT_FALSE(str_0);
        ASSERT_TRUE(str_1);
    }
    
    TEST_F( LimitedPoolTest , testRC_LimitedPoolCanFindElementByKey )
    {
        using PoolT = db0::pools::RC_LimitedPool<db0::o_string, db0::o_string::comp_t>;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        PoolT pool(memspace, memspace);
        auto addr_0 = pool.addRef("jeden");
        auto addr_1 = pool.addRef("dwa");
        auto addr_2 = pool.addRef("trzy");
        decltype(addr_0) find_result;
        ASSERT_TRUE(pool.find("jeden", find_result));
        ASSERT_EQ(find_result, addr_0);
        ASSERT_TRUE(pool.find("trzy", find_result));
        ASSERT_EQ(find_result, addr_2);
        ASSERT_TRUE(pool.find("dwa", find_result));
        ASSERT_EQ(find_result, addr_1);

        ASSERT_FALSE(pool.find("cztery", find_result));
    }
    
    TEST_F( LimitedPoolTest , testRC_LimitedPoolCanUnRefByAddress )
    {
        using PoolT = db0::pools::RC_LimitedPool<db0::o_string, db0::o_string::comp_t>;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");

        PoolT pool(memspace, memspace);
        auto addr_0 = pool.addRef("jeden");
        auto addr_1 = pool.addRef("dwa");        
        auto addr_2 = pool.addRef("trzy");
        pool.addRef("dwa");

        ASSERT_EQ(pool.size(), 3);
        pool.unRefByAddr(addr_0);
        ASSERT_EQ(pool.size(), 2);
        pool.unRefByAddr(addr_1);
        ASSERT_EQ(pool.size(), 2);
        pool.unRefByAddr(addr_1);        
        ASSERT_EQ(pool.size(), 1);
        pool.unRefByAddr(addr_2);
        ASSERT_EQ(pool.size(), 0);
    }

}