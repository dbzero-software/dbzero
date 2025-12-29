// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Item.hpp"
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model
{

DB0_PACKED_BEGIN

    struct DB0_PACKED_ATTR o_pair_item: public db0::o_fixed<o_pair_item>
    {
        o_typed_item m_first;
        o_typed_item m_second;

        o_pair_item() = default;

        inline o_pair_item(o_typed_item first, o_typed_item second)
            : m_first(first)
            , m_second(second)
        {
        }

        bool operator==(const o_pair_item & other) const{
            return m_first == other.m_first && m_second == other.m_second;
        }

        bool operator!=(const o_pair_item & other) const{
            return !(*this == other);
        }

        bool operator<(const o_pair_item & other) const{
            return m_first < other.m_first;
        }
    };

DB0_PACKED_END

}