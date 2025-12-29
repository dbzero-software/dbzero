// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/utils/FlagSet.hpp>
#include <cstdint>

namespace db0::object_model

{
    
    enum MemoOptions: std::uint16_t
    {
        // instances of this type opted out of auto-assigned type tags
        NO_DEFAULT_TAGS = 0x0001,
        // instances of this type opted out of caching
        NO_CACHE = 0x0002,
        IMMUTABLE = 0x0004
    };
    
    using MemoFlags = db0::FlagSet<MemoOptions>;

}

DECLARE_ENUM_VALUES(db0::object_model::MemoOptions, 3)