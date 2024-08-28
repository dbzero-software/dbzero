#pragma once

#include "PrefixCache.hpp"

namespace db0

{

    class SnapshotCache: public PrefixCache
    {
    public:
        SnapshotCache(BaseStorage &, CacheRecycler *);

        // adds the read-only range to this instance
        void insert(std::shared_ptr<DP_Lock>, std::uint64_t state_num);
        void insertWide(std::shared_ptr<WideLock>, std::uint64_t state_num);
    };
    
}   