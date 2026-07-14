// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "PrefixCache.hpp"

namespace db0

{
    
    class SnapshotCache: public PrefixCache
    {
    public:
        // SnapshotCache is read-only, no dirty meter required
        SnapshotCache(BaseStorage &, CacheRecycler *);

        // adds the read-only range to this instance
        void insert(std::shared_ptr<DP_Lock>, std::uint64_t state_num);
        void insertWide(std::shared_ptr<WideLock>, std::uint64_t state_num);
    };
    
}   