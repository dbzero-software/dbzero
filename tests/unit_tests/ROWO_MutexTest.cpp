// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/threading/ROWO_Mutex.hpp>

namespace tests

{
    
    static constexpr std::uint32_t flag_read    = 0x0001;
    static constexpr std::uint32_t flag_write   = 0x0002;
    static constexpr std::uint32_t flag_lock    = 0x0004;
    static constexpr std::uint32_t flag_dirty   = 0x0008;

    using ReadMutexT = db0::ROWO_Mutex<std::uint32_t, flag_read, flag_read, flag_lock>;
    using RWMutexT = db0::ROWO_Mutex<std::uint32_t, flag_write, flag_read | flag_write, flag_lock>;    

    TEST( ROWO_MutexTest , testROWO_MutexCanLockResourceForRead )
    {
        // lock resource for read-only
        std::atomic<std::uint32_t> value = 0;
        while (!ReadMutexT::__ref(value).get())
        {
            ReadMutexT::WriteOnlyLock lock(value);
            if (lock.isLocked()) {
                // resource should be initialized here
                lock.commit_set();
            }
        }

        ASSERT_TRUE(value & flag_read);
        ASSERT_FALSE(value & flag_lock);
        ASSERT_FALSE(value & flag_write);
    }

    TEST( ROWO_MutexTest , testROWO_MutexCanLockResourceForReadWrite )
    {
        // lock resource for read / write
        std::atomic<std::uint32_t> value = 0;
        while (!RWMutexT::__ref(value).get())
        {
            RWMutexT::WriteOnlyLock lock(value);
            if (lock.isLocked()) {
                // resource should be initialized here
                lock.commit_set();
            }
        }

        ASSERT_TRUE(value & flag_read);
        ASSERT_TRUE(value & flag_write);
        ASSERT_FALSE(value & flag_lock);        
    }

    TEST( ROWO_MutexTest , testROWO_MutexCanLockResourceForReadFirstAndThenForWrite )
    {
        // lock resource for read-only first
        std::atomic<std::uint32_t> value = 0;
        while (!ReadMutexT::__ref(value).get())
        {
            ReadMutexT::WriteOnlyLock lock(value);
            if (lock.isLocked()) {
                // resource should be initialized here
                lock.commit_set();
            }
        }

        // not available for write yet
        ASSERT_FALSE(RWMutexT::__ref(value).get());

        // lock same resource for read / write        
        while (!RWMutexT::__ref(value).get())
        {
            RWMutexT::WriteOnlyLock lock(value);
            if (lock.isLocked()) {
                // resource should be initialized here                
                lock.commit_set();
            }
        }

        // available for write now
        ASSERT_TRUE(RWMutexT::__ref(value).get());
    }

    TEST( ROWO_MutexTest , testROWO_MutexCanAccessResourceForReadIfLockedForReadWrite )
    {
        // lock resource for read / write
        std::atomic<std::uint32_t> value = 0;
        while (!RWMutexT::__ref(value).get())
        {
            RWMutexT::WriteOnlyLock lock(value);
            if (lock.isLocked()) {
                // resource should be initialized here                
                lock.commit_set();
            }
        }

        // check if accessible for read
        ASSERT_TRUE(ReadMutexT::__ref(value).get());
    }

    TEST( ROWO_MutexTest , testROWO_MutexCanResetFlagOnCommit )
    {
        using DirtyMutexT = db0::ROWO_Mutex<std::uint32_t, flag_dirty, flag_dirty, flag_lock>;

        // lock resource for read / write
        std::atomic<std::uint32_t> value = flag_dirty;
        while (DirtyMutexT::__ref(value).get())
        {
            DirtyMutexT::WriteOnlyLock lock(value);
            if (lock.isLocked()) {
                // resource should be flushed here
                lock.commit_reset();
            }
        }

        // check if dirty flag was removed
        ASSERT_FALSE(DirtyMutexT::__ref(value).get());
    }

}