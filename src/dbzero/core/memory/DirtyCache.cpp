// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DirtyCache.hpp"
#include <dbzero/core/memory/utils.hpp>

namespace db0

{

    DirtyCache::DirtyCache(std::size_t page_size, std::atomic<std::size_t> &dirty_meter)
        : DirtyCache(page_size, &dirty_meter)
    {
    }
    
    DirtyCache::DirtyCache(std::size_t page_size, std::atomic<std::size_t> *dirty_meter_ptr)
        : m_dirty_meter_ptr(dirty_meter_ptr)
        , m_page_size(page_size)
        , m_shift(getPageShift(page_size, false))
    {
    }
        
    void DirtyCache::append(std::shared_ptr<ResourceLock> res_lock)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_locks.push_back(res_lock);
        if (m_dirty_meter_ptr) {
            auto lock_size = res_lock->usedMem();
            m_size += lock_size;
            *m_dirty_meter_ptr += lock_size;
        }
    }
    
    void DirtyCache::tryFlush(FlushMethod flush_method)
    {
        std::size_t flushed = 0;
        std::unique_lock<std::mutex> lock(m_mutex);
        auto it = m_locks.begin();
        while (it != m_locks.end()) {
            if ((*it)->tryFlush(flush_method)) {
                flushed += (*it)->usedMem();
                it = m_locks.erase(it);
            } else {
                ++it;
            }
        }
        if (m_dirty_meter_ptr) {
            *m_dirty_meter_ptr -= flushed;
            m_size -= flushed;
        }        
    }
    
    void DirtyCache::flush()
    {        
        std::unique_lock<std::mutex> lock(m_mutex);
        for (auto &res_lock: m_locks) {
            res_lock->flush();
        }
        m_locks.clear();
        if (m_dirty_meter_ptr) {
            *m_dirty_meter_ptr -= m_size;
            m_size = 0;
        }        
    }
    
    std::size_t DirtyCache::flush(std::size_t limit)
    {
        assert(m_dirty_meter_ptr);
        std::unique_lock<std::mutex> lock(m_mutex);
        std::size_t flushed = 0;
        auto it = m_locks.begin();        
        while (flushed < limit && it != m_locks.end()) {
            // only flush locks with use_count below 2 
            // i.e. - owned by the DirtyCache and possibly by the CacheRecycler
            if ((*it).use_count() <= 2) {
                // flush using the FlushMethod::full method (default)
                (*it)->flush();
                flushed += (*it)->usedMem();
                it = m_locks.erase(it);
            } else {
                ++it;
            }
        }
        if (m_dirty_meter_ptr) {
            *m_dirty_meter_ptr -= flushed;
            m_size -= flushed;
        }
        return flushed;
    }
    
    void DirtyCache::flushDirty(SinkFunction sink)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        for (auto &res_lock : m_locks) {
            if (res_lock->resetDirtyFlag()) {
                if (m_shift) {
                    sink(res_lock->getAddress() >> m_shift, res_lock->getBuffer());
                } else {
                    sink(res_lock->getAddress() / m_page_size, res_lock->getBuffer());
                }
            }
        }
        m_locks.clear();
        if (m_dirty_meter_ptr) {
            *m_dirty_meter_ptr -= m_size;
            m_size = 0;
        }
    }
    
    std::size_t DirtyCache::size() const
    {
        assert(m_dirty_meter_ptr);
        return m_size;        
    }
    
    void DirtyCache::forAll(std::function<void(const ResourceLock &)> f) const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        for (auto &res_lock : m_locks) {
            f(*res_lock);
        }
    }
        
}