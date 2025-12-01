#pragma once

#include <dbzero/core/utils/FlagSet.hpp>

namespace db0

{

    enum class StorageOptions : std::uint16_t
    {
        // Prevents loading any data into memory (e.g. when opening for copying)
        NO_LOAD = 0x0001,
    };
    
    using StorageFlags = FlagSet<StorageOptions>;

}
