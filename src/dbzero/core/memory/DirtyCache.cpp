#include "DirtyCache.hpp"
#include <dbzero/core/memory/utils.hpp>

namespace db0

{

    DirtyCache::DirtyCache(std::size_t page_size)
        : m_shift(getPageShift(page_size))
    {
    }

    void DirtyCache::append(std::shared_ptr<ResourceLock> lock)
    {
        std::unique_lock<std::mutex> _lock(m_mutex);
        m_locks.push_back(lock);
    }

    void DirtyCache::flush()
    {
        std::unique_lock<std::mutex> _lock(m_mutex);
        for (auto &lock_ptr: m_locks) {
            lock_ptr->flush();
        }
        m_locks.clear();
    }

    void DirtyCache::flushDirty(SinkFunction sink)
    {
        std::unique_lock<std::mutex> _lock(m_mutex);
        for (auto &lock : m_locks) {
            if (lock->resetDirtyFlag()) {
                sink(lock->getAddress() >> m_shift, lock->getBuffer());
            }
        }
        m_locks.clear();
    }
    
}