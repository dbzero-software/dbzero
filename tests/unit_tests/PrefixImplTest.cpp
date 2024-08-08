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
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
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
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
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
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
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
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
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
        PrefixImpl<BDevStorage> cut(file_name, nullptr, file_name);
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
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
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
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
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
        b2.release();
        cut.close();
        ASSERT_EQ(str_value, "XYZC5678ABCDABCD");
    }

    TEST_F( PrefixImplTest , testBoundaryUpdateAfterRead )
    {
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // write boundary range in state = 1
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::create, AccessOptions::write });
        memcpy(w1.modify(), "12345678", 8);
        w1.release();
        
        // read boundary range in state = 2 (after beginAtomic)
        cut.beginAtomic();
        auto r1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read });
        
        // note that cache will keep the boundary lock (since we don't release it)
        // write boundary range in state = 2 
        auto w2 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::write });
        memcpy(w2.modify(), "ABCDEFGH", 8);
        w2.release();
        r1.release();

        // validate contents from both states
        {
            auto lock = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read });
            auto str_value = std::string((char *)lock.m_buffer, 8);
            ASSERT_EQ(str_value, "ABCDEFGH");            
        }

        cut.cancelAtomic();
        {
            auto lock = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read });
            auto str_value = std::string((char *)lock.m_buffer, 8);
            ASSERT_EQ(str_value, "12345678");
        }
        
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testRandomReadWritesInTransactions )
    {
        srand(time(nullptr));
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
        auto page_size = cut.getPageSize();
        // number of pages to write to
        auto range = 5;
        auto transaction_count = 10;
        auto op_count = 10000;

        std::vector<char> data(page_size * range);
        std::memset(data.data(), 0, data.size());

        auto rand_vec = [](int size) {
            std::vector<char> vec(size);
            for (int i = 0; i < size; i++) {
                vec[i] = rand() % 256;
            }
            return vec;
        };

        for (int i = 0; i < range; ++i) {
            cut.mapRange(page_size * i, page_size, { AccessOptions::create, AccessOptions::write });
        }
        // op_codes: 0 = read, 1 = write, 2 = create
        for (int i = 0; i < transaction_count; i++) {
            for (int j = 0; j < op_count; j++) {
                auto op_code = rand() % 3;
                auto addr = rand() % (page_size * range - 128);
                auto size = rand() % 128 + 1;
                switch (op_code) {
                    case 0: {
                        auto lock = cut.mapRange(addr, size, { AccessOptions::read });                        
                        // validate data read
                        auto str_value = std::string((char *)lock.m_buffer, size);
                        assert(str_value == std::string(data.data() + addr, size));
                        ASSERT_EQ(str_value, std::string(data.data() + addr, size));
                        break;
                    }
                    case 1: {
                        auto lock = cut.mapRange(addr, size, { AccessOptions::write });
                        auto rand_data = rand_vec(size);
                        std::memcpy(lock.modify(), rand_data.data(), size);
                        std::memcpy(data.data() + addr, rand_data.data(), size);
                        break;
                    }
                    case 2: {
                        auto lock = cut.mapRange(addr, size, { AccessOptions::create, AccessOptions::write });
                        std::memset(lock.modify(), 0, size);
                        std::memset(data.data() + addr, 0, size);
                        break;
                    }
                }
            }
            cut.commit();
        }

        cut.close();
    }
    
    TEST_F( PrefixImplTest , testReadNonConsecutiveTransactions )
    {
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl<BDevStorage> *cut = nullptr;
        auto prefix = std::shared_ptr<Prefix>(
            cut = new PrefixImpl<BDevStorage>(file_name, &m_cache_recycler, file_name));
        auto page_size = prefix->getPageSize();
        
        // create page versions in transactions 1, 2, 3
        for (int i = 0; i < 3; i++) {
            auto r0 = prefix->mapRange(0, page_size, { AccessOptions::create, AccessOptions::write });
            std::memset(r0.modify(), i + 1, page_size);
            r0.release();
            prefix->commit();
        }

        // remove all locks from cache
        cut->getCache().clear();
        // read page in state #1 (snapshot)
        auto p1 = prefix->getSnapshot(1)->mapRange(0, page_size, { AccessOptions::read });
        for (unsigned int i = 0; i < page_size; i++) {
            ASSERT_EQ(((char *)p1.m_buffer)[i], 1);
        }
        
        // try reading from state #3 next (while state #1 is cached)
        auto p3 = prefix->getSnapshot(3)->mapRange(0, page_size, { AccessOptions::read });
        for (unsigned int i = 0; i < page_size; i++) {
            ASSERT_EQ(((char *)p3.m_buffer)[i], 3);
        }
        
        prefix->close();
    }

    TEST_F( PrefixImplTest , testRevertAtomicBoundaryUpdate )
    {
        BDevStorage::create(file_name);        
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // write boundary range in state = 1
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::create, AccessOptions::write });
        memcpy(w1.modify(), "12345678", 8);
        w1.release();

        // update boundary lock
        cut.beginAtomic();
        auto w2 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read, AccessOptions::write });
        memcpy((char*)w2.modify() + 2, "ABCD", 4);
        w2.release();

        cut.cancelAtomic();
        {
            auto lock = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read });
            auto str_value = std::string((char *)lock.m_buffer, 8);
            ASSERT_EQ(str_value, "12345678");
        
            // also read page-wise        
            auto p1 = cut.mapRange(page_size * 1 - 4, 4, { AccessOptions::read });
            str_value = std::string((char *)p1.m_buffer, 4);
            ASSERT_EQ(str_value, "1234");

            auto p2 = cut.mapRange(page_size * 1, 4, { AccessOptions::read });
            str_value = std::string((char *)p2.m_buffer, 4);
            ASSERT_EQ(str_value, "5678");
        }

        cut.close();
    }

    TEST_F( PrefixImplTest , testCommitAtomicBoundaryUpdate )
    {
        BDevStorage::create(file_name);        
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // write boundary range in state = 1
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::create, AccessOptions::write });
        memcpy(w1.modify(), "12345678", 8);
        w1.release();

        // update boundary lock
        cut.beginAtomic();
        auto w2 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read, AccessOptions::write });
        memcpy((char*)w2.modify() + 2, "ABCD", 4);
        w2.release();

        cut.endAtomic();
        {
            auto lock = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read });
            auto str_value = std::string((char *)lock.m_buffer, 8);
            ASSERT_EQ(str_value, "12ABCD78");
        }
        
        cut.close();
    }

    TEST_F( PrefixImplTest , testMergingAtomicAndNonAtomicUpdates )
    {
        BDevStorage::create(file_name);        
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);        
        
        // initial update, keep the lock active
        auto w1 = cut.mapRange(0, 8, { AccessOptions::create, AccessOptions::write });
        memcpy(w1.modify(), "12345678", 8);

        // atomic update the same page, release lock
        cut.beginAtomic();
        auto w2 = cut.mapRange(8, 4, { AccessOptions::read, AccessOptions::write });
        memcpy(w2.modify(), "ABCD", 4);
        w2.release();        
        cut.endAtomic();

        // partially update w1 again
        memcpy((char*)w1.modify() + 2 , "CKJA", 4);
        w1.release();
        // commit & validate the contents
        cut.commit();

        auto r1 = cut.mapRange(0, 12, { AccessOptions::read });
        auto str_value = std::string((char *)r1.m_buffer, 12);
        ASSERT_EQ(str_value, "12CKJA78ABCD");
        cut.close();
    }

    TEST_F( PrefixImplTest , testMultipleAtomicBoundaryUpdates )
    {
        BDevStorage::create(file_name);        
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // create boundary range inside atomic
        cut.beginAtomic();
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::create, AccessOptions::write });
        memcpy(w1.modify(), "12345678", 8);
        w1.release();        
        cut.endAtomic();
        
        // read boundary range inside atomic operation
        cut.beginAtomic();
        {
            auto lock = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read, AccessOptions::write });
            auto str_value = std::string((char *)lock.m_buffer, 8);
            ASSERT_EQ(str_value, "12345678");
        }
        
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testAtomicBoundaryUpdatesWithPreExistingLocks )
    {
        BDevStorage::create(file_name);
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
        auto page_size = cut.getPageSize();

        // create boundary range in state = 1 but don't flush it
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::create, AccessOptions::write });        
        memcpy(w1.modify(), "12345678", 8);
        
        // update the pre-existing boundary lock as atomic
        cut.beginAtomic();
        {
            auto w2 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read, AccessOptions::write });
            memcpy((char*)w2.modify() + 2, "abcd", 4);
            w2.release();
        }
        cut.endAtomic();
        w1.release();
        
        auto lock = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read, AccessOptions::write });
        auto str_value = std::string((char *)lock.m_buffer, 8);
        ASSERT_EQ(str_value, "12abcd78");
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testWideAllocInconsistentLockIssue )
    {
        BDevStorage::create(file_name);
        PrefixImpl<BDevStorage> cut(file_name, &m_cache_recycler, file_name);
        auto page_size = cut.getPageSize();

        // map short unaligned range at the end of 2nd page
        auto w1 = cut.mapRange(page_size * 2 - 32, 16, { AccessOptions::create, AccessOptions::write });
        // map wide range spanning page 1 & 2
        auto w2 = cut.mapRange(0, page_size + 128, { AccessOptions::create, AccessOptions::write });
        ASSERT_TRUE(w1);
        ASSERT_TRUE(w2);
        cut.close();
    }    
    
}
