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
    
    void DirtyCache::append(std::shared_ptr<ResourceLock> lock)
    {
        std::unique_lock<std::mutex> _lock(m_mutex);
        m_locks.push_back(lock);
        if (m_dirty_meter_ptr) {
            auto lock_size = lock->size();
            m_size += lock_size;
            *m_dirty_meter_ptr += lock_size;
        }
    }

    void DirtyCache::flush()
    {        
        std::unique_lock<std::mutex> _lock(m_mutex);
        for (auto &lock_ptr: m_locks) {
            lock_ptr->flush();
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
        std::unique_lock<std::mutex> _lock(m_mutex);
        std::size_t flushed = 0;
        auto it = m_locks.begin();
        while (flushed < limit && it != m_locks.end()) {
            (*it)->flush();    
            flushed += (*it)->size();
            ++it;
        }
        m_locks.erase(m_locks.begin(), it);
        *m_dirty_meter_ptr -= flushed;
        m_size -= flushed;
        return flushed;
    }
    
    void DirtyCache::flushDirty(SinkFunction sink)
    {
        std::unique_lock<std::mutex> _lock(m_mutex);
        for (auto &lock : m_locks) {
            if (lock->resetDirtyFlag()) {
                if (m_shift) {
                    sink(lock->getAddress() >> m_shift, lock->getBuffer());
                } else {
                    sink(lock->getAddress() / m_page_size, lock->getBuffer());
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

}