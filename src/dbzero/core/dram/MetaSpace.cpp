// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MetaSpace.hpp"
#include "MetaPrefix.hpp"
#include <dbzero/core/dram/DRAM_Allocator.hpp>

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

}
