#pragma once

#include <cstdint>

namespace db0

{
    
    std::uint64_t make_UUID();
    
    /**
     * Generate instance ID for DBZero objects.
    */
    std::uint32_t createInstanceId();

}