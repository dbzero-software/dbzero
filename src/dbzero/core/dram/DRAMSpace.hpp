// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include <dbzero/core/memory/Memspace.hpp>

namespace db0 

{
    
    class DRAM_Prefix;
    class DRAM_Allocator;
    using DRAM_Pair = std::pair<std::shared_ptr<DRAM_Prefix>, std::shared_ptr<DRAM_Allocator> >;

    struct DRAMSpace
    {
        static Memspace create(std::size_t page_size, std::function<void(DRAM_Pair)> callback = {});
        static Memspace create(DRAM_Pair);
        static Memspace tryCreate(DRAM_Pair);
    };
    
}