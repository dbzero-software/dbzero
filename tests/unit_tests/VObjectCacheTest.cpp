#include <utils/WorkspaceTest.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/vspace/v_object.hpp>

using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class VObjectCacheTests: public WorkspaceBaseTest
    {        
    protected:
        static constexpr std::size_t CACHE_SIZE = 16;

        VObjectCacheTests()
            : m_sol(CACHE_SIZE)
        {
        }

        FixedObjectList m_sol;
    };
    
    struct [[gnu::packed]] o_test_inst: db0::o_fixed<o_test_inst>
    {
        std::uint32_t m_value = 691;

        o_test_inst() = default;
        o_test_inst(std::uint32_t value)
            : m_value(value)
        {
        }
    };
    
	TEST_F( VObjectCacheTests , testVObjecCacheCanCreateNewInstance )
	{
        using VType = db0::v_object<o_test_inst>;
        auto memspace = getMemspace();
        VObjectCache cut(memspace, m_sol);
        auto obj = cut.create<VType>();
        ASSERT_EQ(691u, (*obj)->m_value);
	}

	TEST_F( VObjectCacheTests , testVObjecCacheCanCreateNewInstanceWithArguments )
	{
        using VType = db0::v_object<o_test_inst>;
        auto memspace = getMemspace();
        VObjectCache cut(memspace, m_sol);
        auto obj = cut.create<VType>(81392);
        ASSERT_EQ(81392u, (*obj)->m_value);
	}

	TEST_F( VObjectCacheTests , testCanRetrieveInstanceFromVObjectCache )
	{
        using VType = db0::v_object<o_test_inst>;
        auto memspace = getMemspace();
        VObjectCache cut(memspace, m_sol);
        // add to cache first
        auto address = cut.create<VType>(81392)->getAddress();
        auto obj = cut.tryFind<VType>(address);
        ASSERT_TRUE(obj);
        ASSERT_EQ(81392u, (*obj)->m_value);
	}
    
	TEST_F( VObjectCacheTests , testVObjectCacheInstancesAreRemovedAfterCacheCapacityIsExhausted )
	{
        using VType = db0::v_object<o_test_inst>;
        auto memspace = getMemspace();
        VObjectCache cut(memspace, m_sol);
        auto address = cut.create<VType>(81392)->getAddress();
        // add more instances so that the cache size is exhausted
        // use twice the cache size because extra capacity may be assigned by the implementation
        for (std::size_t i = 0; i < CACHE_SIZE * 2; ++i) {
            cut.create<VType>(i);
        }
        // check if the first instance is still in the cache
        auto obj = cut.tryFind<VType>(address);
        ASSERT_FALSE(obj);
	}

}
