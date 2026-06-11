// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaSpace.hpp"
#include "MetaPrefix.hpp"
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <algorithm>
#include <utility>

namespace db0

{
    
    Memspace MetaSpace::create(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io)
    {
        auto prefix = std::make_shared<MetaPrefix>(page_size, sparse_pair);
        load(*prefix, page_io);
        auto allocator = std::make_shared<DRAM_Allocator>(
            [&](DRAM_Allocator::AddressSinkFunction sink) {
                prefix->forAllocatedAddresses(sink);
            },
            page_size
        );
        return { prefix, allocator };
    }

    MS_MetaSpace::MS_MetaSpace(std::shared_ptr<MS_MetaPrefix> prefix, std::shared_ptr<MS_MetaAllocator> allocator)
        : Memspace(std::move(prefix), std::move(allocator))
    {
    }

    MS_MetaSpace MS_MetaSpace::create(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io)
    {
        return create(page_size, sparse_pair, page_io, MappingPolicy::eager);
    }

    MS_MetaSpace MS_MetaSpace::create(std::size_t page_size, SparsePair &sparse_pair, Diff_IO &page_io,
        MappingPolicy mapping_policy)
    {
        std::shared_ptr<MS_MetaPrefix> prefix;
        if (mapping_policy == MappingPolicy::eager) {
            // page_io not required with eager loading policy
            prefix = std::make_shared<MS_MetaPrefix>(page_size, sparse_pair);
            db0::load(*prefix, page_io);
        } else {
            prefix = std::make_shared<MS_MetaPrefix>(page_size, sparse_pair, &page_io);
        }

        auto allocator = std::make_shared<MS_MetaAllocator>(sparse_pair, page_size);
        return { prefix, allocator };
    }
    
    std::shared_ptr<MS_MetaPrefix> MS_MetaSpace::getMSPrefixPtr() const
    {
        return std::static_pointer_cast<MS_MetaPrefix>(m_prefix);
    }

    std::shared_ptr<MS_MetaAllocator> MS_MetaSpace::getMSAllocatorPtr() const
    {
        return std::static_pointer_cast<MS_MetaAllocator>(m_allocator);
    }

}
