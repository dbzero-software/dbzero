// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Address.hpp"

namespace db0

{

    UniqueAddress makeUniqueAddr(std::uint64_t offset, std::uint16_t id) {
        return UniqueAddress(Address::fromOffset(offset), id);
    }
    
}