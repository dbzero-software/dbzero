// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/utils/num_pack.hpp>

namespace db0

{

    // field-level tags are represented as long tags
    using LongTagT = db0::num_pack<std::uint64_t, 2u>;

    inline std::uint64_t regularLongTagPart(std::uint64_t value) {
        return TagAddress::regularValue(value);
    }

    inline bool isPassiveLongTag(const LongTagT &tag) {
        return TagAddress::isPassiveValue(tag.data[1]);
    }

    inline LongTagT asPassiveLongTag(LongTagT tag) {
        tag.data[1] = TagAddress::fromValue(tag.data[1]).asPassive().getValue();
        return tag;
    }

    inline LongTagT asRegularLongTag(LongTagT tag) {
        tag.data[1] = regularLongTagPart(tag.data[1]);
        return tag;
    }

    template <> inline bool num_pack<std::uint64_t, 2u>::operator<(const num_pack &other) const
    {
        if (data[0] < other.data[0]) {
            return true;
        }
        if (data[0] > other.data[0]) {
            return false;
        }
        return regularLongTagPart(data[1]) < regularLongTagPart(other.data[1]);
    }

    template <> inline bool num_pack<std::uint64_t, 2u>::operator==(const num_pack &other) const
    {
        return data[0] == other.data[0] &&
            regularLongTagPart(data[1]) == regularLongTagPart(other.data[1]);
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
                std::hash<std::uint64_t>()(db0::regularLongTagPart(tag.data[1]));
        }
    };
    
}
