#pragma once

#include <cstdint>
#include <dbzero/core/utils/FlagSet.hpp>
    
namespace db0

{

    enum class AccessOptions : std::uint16_t
    {        
        read        = 0x0001,
        write       = 0x0002,
        create      = 0x0004,
        no_cache    = 0x0010,
        // resource which should be kept in-memory
        no_flush    = 0x0020
    };
    
    /**
     * Top 8 high bits reserved for the rowo-mutex
    */
    static constexpr std::uint16_t RESOURCE_AVAILABLE_FOR_READ  = 0x0001;
    static constexpr std::uint16_t RESOURCE_AVAILABLE_FOR_WRITE = 0x0002;
    static constexpr std::uint16_t RESOURCE_AVAILABLE_FOR_RW    = RESOURCE_AVAILABLE_FOR_READ | RESOURCE_AVAILABLE_FOR_WRITE;
    // the resource fetched flag should not be mixed with the read / write flags
    static constexpr std::uint16_t RESOURCE_FETCHED             = 0x0010;
    static constexpr std::uint16_t RESOURCE_LOCK                = 0x0020;
    // DIRTY / RECYCLED flags are used by the ResourceLock
    static constexpr std::uint16_t RESOURCE_DIRTY               = 0x0100;
    // Flag indicating if the lock has been registered with cache recycler
    static constexpr std::uint16_t RESOURCE_RECYCLED            = 0x0200;
    
    enum class AccessType: unsigned int
    {
        READ_ONLY = 1,
        READ_WRITE = 2    
    };

    AccessType parseAccessType(const std::string &access_type);
    
}

DECLARE_ENUM_VALUES(db0::AccessOptions, 5)
