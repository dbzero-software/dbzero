#pragma once

#include <cstdint>
#include <dbzero/core/utils/FlagSet.hpp>
    
namespace db0

{
    
    enum class AccessOptions : std::uint16_t
    {
        read        = 0x0001,
        write       = 0x0002,
        // flag indicating the newly created resource
        create      = 0x0004,
        no_cache    = 0x0008,
        // resource which should be kept in-memory
        no_flush    = 0x0010,
        // request to allocate a unique address
        unique      = 0x0020,
        // disable copy-on-write (e.g. when accessed as read-only)
        no_cow      = 0x0040
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
    // a flag indicating that the resource should not be cached
    static constexpr std::uint16_t RESOURCE_NO_CACHE            = 0x0400;
    
    enum class AccessType: unsigned int
    {
        READ_ONLY = 1,
        READ_WRITE = 2    
    };
    
    AccessType parseAccessType(const std::string &access_type);
    
}

DECLARE_ENUM_VALUES(db0::AccessOptions, 7)
