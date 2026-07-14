// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SnapshotCache.hpp"
#include <dbzero/core/memory/CacheRecycler.hpp>

namespace db0

{
    
    SnapshotCache::SnapshotCache(BaseStorage &storage, CacheRecycler *recycler_ptr)        
        : PrefixCache(storage, recycler_ptr)
    {
    }
    
    void SnapshotCache::insert(std::shared_ptr<DP_Lock> lock, std::uint64_t state_num)
    {
        m_dp_map.insert(state_num, lock);
        // register / update lock with the recycler
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(lock);
        }                
    }
    
    void SnapshotCache::insertWide(std::shared_ptr<WideLock> lock, std::uint64_t state_num)
    {
        m_wide_map.insert(state_num, lock);
        // register / update lock with the recycler
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(lock);
        }                
    }

}