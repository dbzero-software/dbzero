// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <deque>
#include <mutex>
#include <functional>
#include <atomic>
#include "ResourceLock.hpp"

namespace db0

{

    // A cache specialized in storing dirty locks
    class DirtyCache
    {        
    public:        
        // A function to consume a single resource (for serialization)
        using SinkFunction = std::function<void(std::uint64_t page_num, const void *)>;
        
        DirtyCache(std::size_t page_size, std::atomic<std::size_t> &dirty_meter);
        DirtyCache(std::size_t page_size, std::atomic<std::size_t> *dirty_meter_ptr = nullptr);
        
        // register resource with the dirty locks
        void append(std::shared_ptr<ResourceLock>);
        // only flush locks which support a specific flush method
        void tryFlush(FlushMethod);
        void flush();
        
        // try flushing up to 'size' bytes
        // @return number of bytes actually flushed
        std::size_t flush(std::size_t limit);
        
        /**
         * Output all modified pages into a user provided sink function
         * and mark pages as non-dirty
         * The flush order is undefined
        */
        void flushDirty(SinkFunction);

        // NOTE: size only works for a metered cache (i.e. initialized with the dirty_meter)
        std::size_t size() const;
        
        void forAll(std::function<void(const ResourceLock &)>) const;
                
    private:
        std::atomic<std::size_t> *m_dirty_meter_ptr = nullptr;
        mutable std::mutex m_mutex;
        std::deque<std::shared_ptr<ResourceLock> > m_locks;
        const std::size_t m_page_size;
        const unsigned int m_shift;
        // total bytes supported by this cache
        std::atomic<std::size_t> m_size = 0;
    };
    
} 