// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "Address.hpp"

namespace db0

{

    UniqueAddress makeUniqueAddr(std::uint64_t offset, std::uint16_t id) {
        return UniqueAddress(Address::fromOffset(offset), id);
    }
    
}