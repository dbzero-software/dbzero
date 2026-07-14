// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/utils/num_pack.hpp>

namespace db0

{

    // field-level tags are represented as long tags
    using LongTagT = db0::num_pack<std::uint64_t, 2u>;

    template <> inline bool num_pack<std::uint64_t, 2u>::operator<(const num_pack &other) const
    {
        if (data[0] < other.data[0]) {
            return true;
        }
        if (data[0] > other.data[0]) {
            return false;
        }
        return data[1] < other.data[1];
    }

    template <> inline bool num_pack<std::uint64_t, 2u>::operator==(const num_pack &other) const
    {
        return data[0] == other.data[0] &&
            data[1] == other.data[1];
    }

    template <> inline bool num_pack<std::uint64_t, 2u>::operator!=(const num_pack &other) const
    {
        return !(*this == other);
    }

}

namespace std

{

    // LongTagT specialization for std::hash
    template <> struct hash<db0::LongTagT>
    {        
        std::size_t operator()(const db0::LongTagT &tag) const {
            return std::hash<std::uint64_t>()(tag.data[0]) ^
                std::hash<std::uint64_t>()(tag.data[1]);
        }
    };
    
}
