// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/utils/num_pack.hpp>

namespace db0

{

    // field-level tags are represented as long tags
    using LongTagT = db0::num_pack<std::uint64_t, 2u>;

}

namespace std

{

    // LongTagT specialization for std::hash
    template <> struct hash<db0::LongTagT>
    {        
        std::size_t operator()(const db0::LongTagT &tag) const {
            return std::hash<std::uint64_t>()(tag.data[0]) ^ std::hash<std::uint64_t>()(tag.data[1]);
        }
    };
    
}
