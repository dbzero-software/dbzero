// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
            : m_cache_recycler(16 * 1024 * 1024, m_dirty_meter)
        {
        }

        static constexpr const char *file_name = "my-test-prefix_1.db0";

        virtual void SetUp() override 
        {
            drop(file_name);
            m_dirty_meter = 0;
            m_cache_recycler.clear();
        }

        virtual void TearDown() override 
        {
            drop(file_name);          
            m_dirty_meter = 0;
            m_cache_recycler.clear();
        }

    protected:
        std::atomic<std::size_t> m_dirty_meter = 0;
        CacheRecycler m_cache_recycler;
    };
    
    TEST_F( PrefixImplTest , testPrefixImplCanMapRangeFromIndividualPages )
    {
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        // within page = 0 (access must be write only since this is a new page)
        auto r0 = cut.mapRange(0, 100, { AccessOptions::write });
        // within page = 0 (access can be read / write)
        auto r1 = cut.mapRange(100, 100, { AccessOptions::read, AccessOptions::write });
        // within page = 1
        auto r2 = cut.mapRange(4102, 3312, { AccessOptions::write });
        ASSERT_EQ(r0.lock(), r1.lock());
        ASSERT_NE(r0, r1);
        ASSERT_NE(r0.lock(), r2.lock());
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testPrefixImplCanHandleCrossBoundaryLock )
    {
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
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
    
    TEST_F( PrefixImplTest , testMapRangeCanReuseDP_Locks )
    {
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        ASSERT_EQ(cut.getStateNum(), 1);
        // create a new range (use current state num)
        {
            auto r0 = cut.mapRange(12288, 4, { AccessOptions::write });
            memcpy(r0.modify(), "1234", 4);
        }
        
        // create another range in the same page, use state_num = 2
        cut.commit();
        auto r1 = cut.mapRange(12292, 4, { AccessOptions::write });
        // make sure the contents from previous write has been preserved
        ASSERT_EQ(std::string((char *)r1.m_buffer - 4, 4), "1234");

        cut.close();
    }

    TEST_F( PrefixImplTest , testMapRangeWithoutCache )
    {
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl cut(file_name, m_dirty_meter, nullptr, std::make_shared<BDevStorage>(file_name));
        ASSERT_EQ(cut.getStateNum(), 1);
        // create a new range (use current state num)
        {
            auto r0 = cut.mapRange(12288, 4, { AccessOptions::write });
            memcpy(r0.modify(), "1234", 4);
            r0.release();
        }
        
        cut.commit();
        // create another range in the same page, use state_num = 2
        auto r1 = cut.mapRange(12292, 4, { AccessOptions::write });
        // make sure the contents from previous write has been preserved
        ASSERT_EQ(std::string((char *)r1.m_buffer - 4, 4), "1234");
        r1.release();
        cut.close();
    }

    TEST_F( PrefixImplTest , testCreateMapBoundaryRange )
    {
        BDevStorage::create(file_name);
        // initialize without cache
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // create boundary range 1
        auto r0 = cut.mapRange(page_size * 2 - 4, 8, { AccessOptions::write });
        memcpy(r0.modify(), "12345678", 8);
        r0.release();
        
        // create single-page range
        cut.mapRange(page_size * 3 - 1080, 120, { AccessOptions::write });
        // create boundary range 2
        auto r1 = cut.mapRange(page_size * 3 - 4, 8, { AccessOptions::write });
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        {
            // write lhs range 1
            auto w1 = cut.mapRange(page_size * 0 + 16, 8, { AccessOptions::write });
            memcpy(w1.modify(), "12345678", 8);

            // write boundary range (without write to rhs)
            auto b1 = cut.mapRange(page_size * 1 - 16, 32, { AccessOptions::write });
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // write boundary range in state = 1
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::write });
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
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
            cut.mapRange(page_size * i, page_size, { AccessOptions::write });
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
                        auto lock = cut.mapRange(addr, size, { AccessOptions::write });
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
        PrefixImpl *cut = nullptr;
        auto prefix = std::shared_ptr<Prefix>(cut = new PrefixImpl(
            file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name)));
        auto page_size = prefix->getPageSize();
        
        // create page versions in transactions 1, 2, 3
        for (int i = 0; i < 3; i++) {
            auto r0 = prefix->mapRange(0, page_size, { AccessOptions::write });
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // write boundary range in state = 1
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::write });
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // write boundary range in state = 1
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::write });
        memcpy(w1.modify(), "12345678", 8);
        w1.release();

        // update boundary lock
        cut.beginAtomic();
        auto w2 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read, AccessOptions::write });
        memcpy((char*)w2.modify() + 2, "ABCD", 4);
        w2.release();

        cut.endAtomic();
        
        // validate boundary lock
        {
            auto lock = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::read });
            auto str_value = std::string((char *)lock.m_buffer, 8);
            ASSERT_EQ(str_value, "12ABCD78");
        }
        
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testCommitAtomicWideUpdate )
    {
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // write wide range with residual part in state = 1
        {
            auto w1 = cut.mapRange(page_size, page_size + 8, { AccessOptions::write });
            memcpy((char*)w1.modify() + page_size, "12345678", 8);
            // write the residual part
            auto w2 = cut.mapRange(page_size * 2 + 8, 8, { AccessOptions::write });
            memcpy(w2.modify(), "AAAAAAAA", 8);
            w1.release();
            w2.release();
        }
        
        // update wide lock as atomic
        cut.beginAtomic();
        {
            auto w1 = cut.mapRange(page_size, page_size + 8, { AccessOptions::read, AccessOptions::write });
            memcpy((char*)w1.modify() + page_size, "ABCDABCD", 8);
            // update the residual part
            auto w2 = cut.mapRange(page_size * 2 + 8, 8, { AccessOptions::read, AccessOptions::write });
            memcpy(w2.modify(), "BBBBBBBB", 8);
            w1.release();
            w2.release();
        }
        
        cut.endAtomic();
        cut.commit();
        
        // validate merge result
        {
            auto lock = cut.mapRange(page_size, page_size + 8, { AccessOptions::read });
            auto str_value = std::string((char *)lock.m_buffer + page_size, 8);
            ASSERT_EQ(str_value, "ABCDABCD");
            lock = cut.mapRange(page_size * 2 + 8, 8, { AccessOptions::read });
            str_value = std::string((char *)lock.m_buffer, 8);
            ASSERT_EQ(str_value, "BBBBBBBB");
        }
        
        cut.close();
    }

    TEST_F( PrefixImplTest , testMergingAtomicAndNonAtomicUpdates )
    {
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        
        // initial update, keep the lock active
        auto w1 = cut.mapRange(0, 8, { AccessOptions::write });
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        ASSERT_EQ(cut.getStateNum(), 1);
        
        // create boundary range inside atomic
        cut.beginAtomic();
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::write });
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();

        // create boundary range in state = 1 but don't flush it
        auto w1 = cut.mapRange(page_size * 1 - 4, 8, { AccessOptions::write });        
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
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();

        // map short unaligned range at the end of 2nd page
        auto w1 = cut.mapRange(page_size * 2 - 32, 16, { AccessOptions::write });
        // map wide range spanning page 1 & 2
        auto w2 = cut.mapRange(0, page_size + 128, { AccessOptions::write });
        ASSERT_TRUE(w1);
        ASSERT_TRUE(w2);
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testInconsistentLocksFromMultipleTransactionsIssue_1 )
    {
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();

        // lock page #1 from transaction #1
        auto w1 = cut.mapRange(0, 32, { AccessOptions::write });        
        w1.release();
        cut.commit();

        // lock page #2 from transaction #2
        auto w2 = cut.mapRange(page_size + 16, 16, { AccessOptions::write });        
        w2.release();

        // lock page #1 + #2 from transaction #3 (as a wide lock)
        auto w3 = cut.mapRange(0, page_size + 128, { AccessOptions::write });    
        w3.release();
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testInconsistentLocksFromMultipleTransactionsIssue_2 )
    {
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();

        auto w1 = cut.mapRange(0, 32, { AccessOptions::write });        
        memcpy(w1.modify(), "88888888", 8);
        w1.release();
        cut.commit();
        
        // lock page #2 from transaction #2 & update it
        auto w2 = cut.mapRange(page_size + 1024, 16, { AccessOptions::write });
        memcpy(w2.modify(), "12345678abcdefgh", 16);
        w2.release();

        // lock page #1 + #2 (as incomplete wide range) from transaction #3 & update it
        auto w3 = cut.mapRange(0, page_size + 16, { AccessOptions::write });
        memcpy((char*)w3.modify() + page_size, "99999999", 8);
        w3.release();
        cut.commit();
        
        // now, try reading the page #2 using the latest transaction number
        auto r1 = cut.mapRange(page_size + 1024, 16, { AccessOptions::read });
        auto str_value = std::string((char *)r1.m_buffer, 16);
        ASSERT_EQ(str_value, "12345678abcdefgh");

        cut.close();
    }
    
    TEST_F( PrefixImplTest , testInconsistentWideLockInDifferentTransactions )
    {
        // This test simulates creating a wide object, deleting it and then replacing
        // it with a larger object at the same address (in 2 distinct transactions)
        // such a situation cannot happen during a single transaction due to deferred free-s in MetaAllocator.
        BDevStorage::create(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, std::make_shared<BDevStorage>(file_name));
        auto page_size = cut.getPageSize();
        
        auto w1 = cut.mapRange(0, page_size * 2, { AccessOptions::write });
        w1.modify();
        w1.release();
        cut.commit();
        
        // non-conflicting operations, independent locks should be created
        auto w2 = cut.mapRange(0, page_size * 3, { AccessOptions::write });
        memcpy(w2.modify(), "12345678abcdefgh", 16);
        w2.release();
        cut.commit();
        
        auto r1 = cut.mapRange(0, page_size * 3, { AccessOptions::read, AccessOptions::write });
        auto str_value = std::string((char *)r1.m_buffer, 16);
        ASSERT_EQ(str_value, "12345678abcdefgh");
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testDiffWriterUseAfterMultipleTransactions )
    {
        // The purpose of this test is to check how the diff mechanism is used
        // in a series of modifiactions of a single page across multiple transactions.
        BDevStorage::create(file_name);
        auto storage = std::make_shared<BDevStorage>(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, storage);

        for (unsigned int i = 0; i < 10; ++i) {
            auto w1 = cut.mapRange(16, 32, { AccessOptions::write });
            auto buf = w1.modify();
            std::memset(buf, i + 1, 32);
            w1.release();
            cut.commit();
        }
        
        auto stats = storage->getDiff_IOStats();
        cut.close();
        // make sure most writes are diff writes
        ASSERT_TRUE((double)stats.second / (double)stats.first > 0.75);        
    }

    TEST_F( PrefixImplTest , testVolatileLockCoWDataHandling )
    {
        // we check if the CoW data is handled correctly by the volatile locks
        BDevStorage::create(file_name);
        auto storage = std::make_shared<BDevStorage>(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, storage);

        // 1. Create a regular DP lock and modify it
        {
            auto w1 = cut.mapRange(16, 16, { AccessOptions::write });        
            std::memset(w1.modify(), 1, 16);        
        }
        // 2. Update the range using "atomic" operation
        {
            cut.beginAtomic();
            auto w2 = cut.mapRange(18, 18, { AccessOptions::read, AccessOptions::write });
            std::memset(w2.modify(), 2, 18);
            w2.release();
            cut.endAtomic();
        }
        auto r1 = cut.mapRange(16, 16, { AccessOptions::read });
        ASSERT_TRUE(r1.lock()->isDirty());
        // make sure the CoW's diff is evaluated correctly
        std::vector<std::uint16_t> diffs;
        ASSERT_TRUE(r1.lock()->getDiffs(diffs));
        // NOTE: the 2 leading 0, 0 elements mean the zero-based DP
        ASSERT_EQ(diffs, (std::vector<std::uint16_t> { 0, 0, 0, 16, 20 }));
        cut.close();
    }
    
    TEST_F( PrefixImplTest , testReusedVolatileLockCoWDataHandling )
    {
        // we check if the CoW data is handled correctly by the volatile locks
        BDevStorage::create(file_name);
        auto storage = std::make_shared<BDevStorage>(file_name);
        PrefixImpl cut(file_name, m_dirty_meter, &m_cache_recycler, storage);

        // 1. Create a regular DP lock and modify it
        {
            auto w1 = cut.mapRange(16, 16, { AccessOptions::write });        
            std::memset(w1.modify(), 1, 16);        
        }
    
        // Clear cache to force reuse of atomic lock
        cut.flushDirty(std::numeric_limits<std::size_t>::max());        
        m_cache_recycler.clear();
        
        // 2. Update the range using "atomic" operation
        {
            cut.beginAtomic();
            auto w2 = cut.mapRange(18, 18, { AccessOptions::read, AccessOptions::write });
            std::memset(w2.modify(), 2, 18);
            w2.release();            
        }
        
        cut.flushDirty(std::numeric_limits<std::size_t>::max());
        m_cache_recycler.clear();
        cut.endAtomic();

        auto r1 = cut.mapRange(16, 16, { AccessOptions::read });
        ASSERT_TRUE(r1.lock()->isDirty());
        // make sure the CoW's diff is evaluated correctly (i.e. either no diff or correct diff)
        std::vector<std::uint16_t> diffs;
        if (r1.lock()->getDiffs(diffs)) {
            ASSERT_EQ(diffs, (std::vector<std::uint16_t> { 0, 0, 0, 16, 20 }));
        }
        cut.close();
    } 
    
}
