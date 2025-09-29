#pragma once

#include <cstdint>
#include "Types.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

        template <typename ItemT> struct [[gnu::packed]] o_optional_item
    {
        // indicates if the item is present (1) or not (0)
        std::uint8_t m_present = 0;
        // the item value (valid only if m_present != 0)
        ItemT m_value;

        o_optional_item() = default;
        o_optional_item(const ItemT &value)
            : m_present(1)
            , m_value(value)
        {
        }

        void set(const ItemT &value) {
            m_present = 1;
            m_value = value;
        }

        void clear() {
            m_present = 0;
        }

        bool isSet() const {
            return m_present != 0;
        }

        const ItemT &get() const {
            if (!isSet()) {
                throw std::runtime_error("o_optional_item: item not set");
            }
            return m_value;
        }

        ItemT &get() {
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
    
}
