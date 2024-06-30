#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/PrefixImpl.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class PrefixImplTest: public testing::Test
    {
    public:
        PrefixImplTest()
            : m_cache_recycler(16 * 1024 * 1024)
        {
        }

        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override {
            drop(file_name);
        }

        virtual void TearDown() override {
            drop(file_name);          
        }    

    protected:
        CacheRecycler m_cache_recycler;
    };
    
    TEST_F( PrefixImplTest , testPrefixImplCanMapRangeFromIndividualPages )
    {
        BDevStorage::create(file_name);
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, {}, file_name);
        // within page = 0 (access must be write only since this is a new page)
        auto r0 = cut.mapRange(0, 100, { AccessOptions::write });
        // within page = 0 (access can be read / write)
        auto r1 = cut.mapRange(100, 100, { AccessOptions::read, AccessOptions::write });
        // within page = 1
        auto r2 = cut.mapRange(4102, 3312, { AccessOptions::write });
        ASSERT_EQ(r0.m_lock, r1.m_lock);
        ASSERT_NE(r0, r1);
        ASSERT_NE(r0.m_lock, r2.m_lock);
        cut.close();
    }

    TEST_F( PrefixImplTest , testPrefixImplCanHandleCrossBoundaryLock )
    {
        BDevStorage::create(file_name);
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, {}, file_name);
        // page #0 lock        
        auto r0 = cut.mapRange(0, 100, { AccessOptions::write });
        // page #1 lock
        auto r1 = cut.mapRange(4096 + 100, 100, { AccessOptions::write });
        // cross-boundary lock
        ASSERT_NO_THROW(cut.mapRange(4096 - 100, 200, { AccessOptions::write }));
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testPrefixImpLockAdvanceFromReadToWrite )
    {
        BDevStorage::create(file_name);
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, {}, file_name);
        ASSERT_EQ(cut.getStateNum(), 1);
        // within page = 0 (access must be write only since this is a new page)
        auto r0 = cut.mapRange(0, 100, { AccessOptions::write });
        const std::string str_data = "1234567890";
        const std::string str_data_2 = "X2345X789X";
        memcpy(r0.modify(), str_data.data(), str_data.size());
        cut.commit();
        ASSERT_EQ(cut.getStateNum(), 2);

        // read-lock obtains version from state 1
        auto r1 = cut.mapRange(0, 100, { AccessOptions::read });
        // write-lock obtains version from state 2
        auto r2 = cut.mapRange(0, 100, { AccessOptions::write });
        {
            std::vector<char> data(str_data.size() + 1);
            memcpy(data.data(), r2.m_buffer, str_data.size());
            data[str_data.size()] = 0;
            ASSERT_EQ(std::string(data.data()), str_data);            
            memcpy(r2.modify(), str_data_2.data(), str_data_2.size());
        }
        cut.commit();

        auto r3 = cut.mapRange(0, 100, { AccessOptions::read });
        {
            std::vector<char> data(str_data.size() + 1);
            memcpy(data.data(), r3.m_buffer, str_data.size());
            data[str_data.size()] = 0;
            ASSERT_EQ(std::string(data.data()), str_data_2);
        }

        ASSERT_EQ(cut.getStateNum(), 3);
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testMapRangeCanReuseResourceLocks )
    {
        BDevStorage::create(file_name);
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, {}, file_name);
        ASSERT_EQ(cut.getStateNum(), 1);
        // create a new range (use current state num)
        {
            auto r0 = cut.mapRange(12288, 4, { AccessOptions::create, AccessOptions::write });
            memcpy(r0.modify(), "1234", 4);
        }
        
        // create another range in the same page, use state_num = 2
        cut.commit();
        auto r1 = cut.mapRange(12292, 4, { AccessOptions::create, AccessOptions::write });
        // make sure the contents from previous write has been preserved
        ASSERT_EQ(std::string((char *)r1.m_buffer - 4, 4), "1234");

        cut.close();
    }

    TEST_F( PrefixImplTest , testMapRangeWithoutCache )
    {
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl<BDevStorage> cut(file_name, nullptr, {}, file_name);
        ASSERT_EQ(cut.getStateNum(), 1);
        // create a new range (use current state num)
        {
            auto r0 = cut.mapRange(12288, 4, { AccessOptions::create, AccessOptions::write });
            memcpy(r0.modify(), "1234", 4);
            r0.release();
        }
        
        cut.commit();
        // create another range in the same page, use state_num = 2
        auto r1 = cut.mapRange(12292, 4, { AccessOptions::create, AccessOptions::write });
        // make sure the contents from previous write has been preserved
        ASSERT_EQ(std::string((char *)r1.m_buffer - 4, 4), "1234");
        r1.release();
        cut.close();
    }

    TEST_F( PrefixImplTest , testCreateMapBoundaryRange )
    {
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, {}, file_name);
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // create boundary range 1
        auto r0 = cut.mapRange(page_size * 2 - 4, 8, { AccessOptions::create, AccessOptions::write });
        memcpy(r0.modify(), "12345678", 8);
        r0.release();
        
        // create single-page range
        cut.mapRange(page_size * 3 - 1080, 120, { AccessOptions::write });
        // create boundary range 2
        auto r1 = cut.mapRange(page_size * 3 - 4, 8, { AccessOptions::create, AccessOptions::write });
        memcpy(r1.modify(), "12345678", 8);
        
        // open boundary range for read (same transaction)
        auto r2 = cut.mapRange(page_size * 3 - 4, 8, { AccessOptions::read });
        auto str_value = std::string((char *)r2.m_buffer, 8);

        cut.close();
        ASSERT_EQ(str_value, "12345678");
    }

    TEST_F( PrefixImplTest , testBoundaryReadIssue1 )
    {
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, {}, file_name);
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        {
            // write lhs range 1
            auto w1 = cut.mapRange(page_size * 0 + 16, 8, { AccessOptions::create, AccessOptions::write });
            memcpy(w1.modify(), "12345678", 8);

            // write boundary range (without write to rhs)
            auto b1 = cut.mapRange(page_size * 1 - 16, 32, { AccessOptions::create, AccessOptions::write });
            memcpy(b1.modify(), "12345678ABCDABCD", 16);
        }
        
        // modify boundary range in a new state (+1) due to atomic
        cut.beginAtomic();
        auto b2 = cut.mapRange(page_size * 1 - 16, 32, { AccessOptions::read, AccessOptions::write });
        memcpy(b2.modify(), "XYZC", 4);
        auto str_value = std::string((char *)b2.m_buffer, 16);
        ASSERT_EQ(str_value, "XYZC5678ABCDABCD");
        b2.release();
        cut.close();        
    }

}
