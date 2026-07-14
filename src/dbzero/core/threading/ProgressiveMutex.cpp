// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/threading/ProgressiveMutexDuration.hpp>
    
namespace db0

{

    std::uint32_t progressive_mutex::scoped_lock::val_unique = std::numeric_limits<std::uint32_t>::max();
    std::uint32_t progressive_mutex::scoped_lock::val_zero = 0;

    progressive_mutex::scoped_lock::~scoped_lock()
    {
        if (n_lock) {
            if (n_lock == val_unique) {
                mx.m_num_readers = 0;
                mx.mx_unique.unlock();
                std::lock_guard<std::mutex> event_lock(mx.mx_event);
                mx.cv.notify_one();
            } else {
                auto new_val = --mx.m_num_readers;
                if (new_val == 0) {
                    std::lock_guard<std::mutex> event_lock(mx.mx_event);
                    mx.cv.notify_one();
                }
            }
        }
    }

    void progressive_mutex::scoped_lock::lock()
    {
        if (n_lock) {
            // upgrade to unique lock
            uniqueLock();
        } else {
            n_lock = 1;
            // try upgrade the number of readers with CAS
            for (;;) {
                auto val = mx.m_num_readers.load();
                if (val == val_unique) {
                    // must wait until unique lock released
                    std::lock_guard<std::timed_mutex> _lock(mx.mx_unique);
                    continue;
                }
                // Explanation why the weak version has been chosen
                // The weak forms ((1) and (3)) of the functions are allowed to fail spuriously, that is, 
                // act as if *obj != *expected even if they are equal. When a compare-and-exchange is in a loop, 
                // the weak version will yield better performance on some platforms.
                if (mx.m_num_readers.compare_exchange_weak(val, val + 1)) {
                    // read lock granted
                    break;
                }
            }
        }
    }

    void progressive_mutex::scoped_lock::readLock()
    {
        if (n_lock == 1) {
            return;
        }
        n_lock = 1;
        // try upgrade the number of readers with CAS
        for (;;) {
            auto val = mx.m_num_readers.load();
            if (val == val_unique) {
                // must wait until unique lock released
                std::lock_guard<std::timed_mutex> _lock(mx.mx_unique);
                continue;
            }
            if (mx.m_num_readers.compare_exchange_weak(val, val + 1)) {
                // read lock granted
                break;
            }
        }
    }

    bool progressive_mutex::scoped_lock::readLockWait(const ProgressiveMutexDuration &static_duration)
    {
        // try upgrade the number of readers with CAS
        if (n_lock == 1) {
            return true;
        }
        ProgressiveMutexDuration duration = static_duration;
        for (;;) {
            auto val = mx.m_num_readers.load();
            if (val == val_unique) {
                if (duration.get().count() <= 0) {
                    return false;
                }
                auto start = std::chrono::steady_clock::now();
                std::unique_lock<std::timed_mutex> _lock(mx.mx_unique, duration.get());
                if (!_lock.owns_lock()) {
                    return false;
                }
                duration.get() -= std::chrono::duration_cast<ProgressiveMutexDuration::time_type>(
                    std::chrono::steady_clock::now() - start);
                _lock.unlock();
                continue;
            }
            if (mx.m_num_readers.compare_exchange_weak(val, val + 1)) {
                // read lock granted
                n_lock = 1;
                return true;
            }
        }    
    }

    void progressive_mutex::scoped_lock::uniqueLock()
    {        
        if (n_lock) {
            if (n_lock == val_unique) {
                // already owns unique lock
                return;
            }
            release();
        }
        n_lock = val_unique;
        mx.mx_unique.lock();
        for (;;) {
            std::unique_lock<std::mutex> mx_lock(mx.mx_event);
            while (mx.m_num_readers.load()) {
                mx.cv.wait(mx_lock);
            }
            mx_lock.unlock();
            if (mx.m_num_readers.compare_exchange_weak(val_zero, val_unique)) {
                // mx locked / unique lock granted
                break;
            }
        }
    }

    bool progressive_mutex::scoped_lock::uniqueLockWait(const ProgressiveMutexDuration &static_duration) 
    {
        if (n_lock) {
            if (n_lock == val_unique) {
                // already owns unique lock
                return true;
            }
            release();
        }
        ProgressiveMutexDuration duration = static_duration;
        mx.mx_unique.lock();
        for (;;) {
            std::unique_lock<std::mutex> mx_lock(mx.mx_event);
            while (mx.m_num_readers.load()) {
                if (duration.get().count() <= 0) {
                    mx_lock.unlock();
                    mx.mx_unique.unlock();
                    return false;
                }
                auto start = std::chrono::steady_clock::now();
                if (mx.cv.wait_for(mx_lock, duration.get()) == std::cv_status::timeout) {
                    mx_lock.unlock();
                    mx.mx_unique.unlock();
                    return false;
                }
                duration.get() -= std::chrono::duration_cast<ProgressiveMutexDuration::time_type>(
                    std::chrono::steady_clock::now() - start);
            }
            mx_lock.unlock();
            if (mx.m_num_readers.compare_exchange_weak(val_zero, val_unique)) {
                // mx locked / unique lock granted
                break;
            }
        }
        n_lock = val_unique;
        return true;        
    }

    bool progressive_mutex::scoped_lock::upgradeToUniqueLock()
    {
        if (n_lock == val_unique) {
            // already owns unique lock
            return true;
        }
        if (mx.mx_unique.try_lock()) {
            if (n_lock) {
                // unregister from readers (atomic DEC)
                if (--mx.m_num_readers == 0) {
                    // try with atomic CAS
                    if (mx.m_num_readers.compare_exchange_weak(val_zero, val_unique)) {
                        // successfully upgraded single reader to unique reader
                        n_lock = val_unique;
                        return true;
                    }
                }
            }
            n_lock = val_unique;
            for (;;) {
                std::unique_lock<std::mutex> mx_lock(mx.mx_event);
                while (mx.m_num_readers.load()) {
                    mx.cv.wait(mx_lock);
                }
                mx_lock.unlock();
                if (mx.m_num_readers.compare_exchange_weak(val_zero, val_unique)) {
                    // mx locked / unique lock granted
                    break;
                }
            }
            return true;
        } else {
            // unable to upgrade
            return false;
        }
    }

    void progressive_mutex::scoped_lock::downgradeLock()
    {
        if (n_lock) {
            if (n_lock == val_unique) {
                mx.m_num_readers = 1;
                n_lock = 1;                
                mx.mx_unique.unlock();
            } else {
                n_lock = 0;
                if (--mx.m_num_readers == 0) {
                    std::lock_guard<std::mutex> event_lock(mx.mx_event);
                    mx.cv.notify_one();
                }
            }
        }
    }

    void progressive_mutex::scoped_lock::release()
    {
        if (n_lock) {
            if (n_lock == val_unique) {
                mx.m_num_readers = 0;
                n_lock = 0;                
                mx.mx_unique.unlock();
                std::lock_guard<std::mutex> event_lock(mx.mx_event);
                mx.cv.notify_one();
            } else {
                n_lock = 0;
                if (--mx.m_num_readers == 0) {
                    std::lock_guard<std::mutex> event_lock(mx.mx_event);
                    mx.cv.notify_one();
                }
            }
        }
    }

}