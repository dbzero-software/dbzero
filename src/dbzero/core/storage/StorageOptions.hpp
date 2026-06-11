// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <functional>
#include <dbzero/core/dram/MetaSpace.hpp>
#include <dbzero/core/memory/MetaAllocator.hpp>

namespace db0

{

    struct StorageOptions
    {
        using StorageSlabBucket = MetaAllocator::StorageSlabBucketingFunction::Bucket;

        MS_MetaSpace::MappingPolicy m_meta_mapping_policy = MS_MetaSpace::MappingPolicy::eager;

        /**
         * Maps a raw application storage byte address to the meta-space slot that
         * hosts the SparsePair metadata for pages in that address bucket.
        */
        std::function<std::uint32_t(std::uint64_t)> m_storage_slab_bucketing;

        /**
         * Extended storage bucketing API.
         *
         * Returns the same meta-space slot id as m_storage_slab_bucketing plus the
         * half-open logical page span covered by the slot. This is populated by
         * defaults and is used for multi-page read/write lookups.
        */
        std::function<StorageSlabBucket(std::uint64_t)> m_storage_slab_bucket;
    };

}
