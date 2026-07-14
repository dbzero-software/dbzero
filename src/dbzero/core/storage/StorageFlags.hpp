// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
