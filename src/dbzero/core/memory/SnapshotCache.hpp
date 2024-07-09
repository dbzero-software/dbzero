#pragma once

#include "PrefixCache.hpp"

namespace db0

{

    class SnapshotCache: public PrefixCache
    {
    public:
        SnapshotCache(BaseStorage &, CacheRecycler *);

        // adds the read-only range to this instance
        void insertRange(std::shared_ptr<ResourceLock>, std::uint64_t state_num);
    };

}