// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <dbzero/core/utils/FlagSet.hpp>
    
namespace db0

{
    
    enum class AccessOptions : std::uint16_t
    {
        read           = 0x0001,
        write          = 0x0002,
        // flag indicating the newly created resource
        create         = 0x0004,
        no_cache       = 0x0008,
        // resource which should be kept in-memory
        no_flush       = 0x0010,
        // disable copy-on-write (e.g. when accessed as read-only)
        no_cow         = 0x0020,
        // flag to excempt resource from dirty cache tracking (relevant for BoundaryLock)
        no_dirty_cache = 0x0040
    };
    
    /**
     * Top 8 high bits reserved for the rowo-mutex
    */
    static constexpr std::uint16_t RESOURCE_AVAILABLE_FOR_READ  = 0x0001;
    static constexpr std::uint16_t RESOURCE_AVAILABLE_FOR_WRITE = 0x0002;
    static constexpr std::uint16_t RESOURCE_AVAILABLE_FOR_RW    = RESOURCE_AVAILABLE_FOR_READ | RESOURCE_AVAILABLE_FOR_WRITE;
    static constexpr std::uint16_t RESOURCE_LOCK                = 0x0010;
    // DIRTY / RECYCLED flags are used by the DP_Lock
    static constexpr std::uint16_t RESOURCE_DIRTY               = 0x0100;
    // Flag indicating if the lock has been registered with cache recycler
    static constexpr std::uint16_t RESOURCE_RECYCLED            = 0x0200;
    // prevent resource from being overwritten (e.g. prevent upgrade to a higher transaction number in PrefixImpl)
    static constexpr std::uint16_t RESOURCE_FREEZE              = 0x0400;
        
    enum class AccessType: unsigned int
    {
        READ_ONLY = 1,
        READ_WRITE = 2    
    };
    
    AccessType parseAccessType(const std::string &access_type);
    
    using AccessFlags = FlagSet<AccessOptions>;
    
}

DECLARE_ENUM_VALUES(db0::AccessOptions, 7)
