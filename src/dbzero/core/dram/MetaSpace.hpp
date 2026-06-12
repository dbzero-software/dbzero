// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/dram/DRAMSpace.hpp>
#include <dbzero/core/dram/MS_MetaPrefix.hpp>
#include <dbzero/core/storage/SparsePairFwd.hpp>
namespace db0

{

    class RandomIO_Stream;

    struct MetaSpace: public DRAMSpace
    {
        static Memspace create(std::size_t page_size, SparsePair &sparse_pair, RandomIO_Stream &page_io);
    };
    
    class MS_MetaSpace: public Memspace
    {
    public:        
        static MS_MetaSpace create(std::size_t page_size, SparsePair &sparse_pair, RandomIO_Stream &page_io,
            MappingPolicy mapping_policy = MappingPolicy::eager);

        std::shared_ptr<MS_MetaPrefix> getMSPrefixPtr() const;

        std::shared_ptr<MS_MetaAllocator> getMSAllocatorPtr() const;

    private:
        MS_MetaSpace(std::shared_ptr<MS_MetaPrefix> prefix, std::shared_ptr<MS_MetaAllocator> allocator);
    };

}
