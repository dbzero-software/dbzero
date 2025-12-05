// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstddef>

namespace db0

{

    /**
     * The minimum allowed page size
     */
    static constexpr std::size_t MIN_PAGE_SIZE = 4096;
    
    // Currently we use uint32_t as the state number which allows enumerating 4B sequential transactions
    // assuming 1s per transaction that gives us 136 years of continuous operation until the state number wraps
    using StateNumType = std::uint32_t;
    
    class Settings
    {
    public:
#ifndef NDEBUG        
        static bool __dbg_logs;
        // performs storage full read / write validation (with in-memory mirroring)
        static bool __storage_validation;
#endif
    };
    
}
