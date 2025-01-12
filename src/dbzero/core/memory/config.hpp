#pragma once

#include <cstdint>

namespace db0

{

    /**
     * The minimum allowed page size
     */
    static constexpr std::size_t MIN_PAGE_SIZE = 4096;
    
    // Currently we use uint32_t as the state number which allows enumerating 4B sequential transactions
    // assuming 1s per transaction that gives us 136 years of continuous operation until the state number wraps
    using StateNumType = std::uint32_t;
    
}
