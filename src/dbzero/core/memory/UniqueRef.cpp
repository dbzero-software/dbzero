// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "UniqueRef.hpp"

#include <stdexcept>

namespace db0

{

    UniqueRef::UniqueRef(UniqueAddress address, bool passive)
        : m_value(address.getValue() | (passive ? PASSIVE_BIT : 0))
    {
        if (address.getOffset() >= OFFSET_MAX) {
            throw std::out_of_range("UniqueRef address offset exceeds 49-bit range");
        }
    }

}
