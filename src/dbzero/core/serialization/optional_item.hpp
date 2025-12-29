// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include "Types.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    template <typename ItemT>
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_optional_item
    {
        // the item value (valid only if m_present != 0)
        ItemT m_value;
        // indicates if the item is present (1) or not (0)
        std::uint8_t m_present = 0;

        o_optional_item() = default;
        o_optional_item(const ItemT &value)            
            : m_value(value)
            , m_present(1)
        {
        }

        void set(const ItemT &value) {
            m_value = value;
            m_present = 1;            
        }

        void clear() {
            m_present = 0;
        }

        bool isSet() const {
            return m_present != 0;
        }

        const ItemT &get() const 
        {
            if (!isSet()) {
                throw std::runtime_error("o_optional_item: item not set");
            }
            return m_value;
        }

        ItemT &get() 
        {
            if (!isSet()) {
                throw std::runtime_error("o_optional_item: item not set");
            }
            return m_value;
        }

        operator std::optional<ItemT>() const
        {
            if (isSet()) {
                return m_value;
            } else {
                return {};
            }
        }
    };
DB0_PACKED_END
    
}
