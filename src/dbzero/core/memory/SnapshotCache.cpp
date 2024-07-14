#include "SnapshotCache.hpp"
#include <dbzero/core/memory/CacheRecycler.hpp>

namespace db0

{

    SnapshotCache::SnapshotCache(BaseStorage &storage, CacheRecycler *recycler_ptr)
        : PrefixCache(storage, recycler_ptr)
    {
    }
    
    void SnapshotCache::insertRange(std::shared_ptr<ResourceLock> lock, std::uint64_t state_num)
    {
        auto first_page = lock->getAddress() >> m_shift;
        auto end_page = ((lock->getAddress() + lock->size() - 1) >> m_shift) + 1;
        m_page_map.insertRange(state_num, lock, first_page, end_page);

        // register / update lock with the recycler
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(lock);
        }                
    }
    
}