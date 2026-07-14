// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <functional>

namespace db0

{
    
    /**
     * Key bounds operator
     * @tparam item_t collection's item type
     * @tparam comp_t item comparator type
     */
    template <typename item_t = std::uint64_t, typename comp_t = std::less<item_t> > struct BoundCheck
    {
        int m_direction;
        bool m_has_bound = false;
        item_t m_key_bound;
        comp_t m_compare;

        BoundCheck(int direction = -1)
            : m_direction(direction)
        {
        }

        template <typename KeyT> BoundCheck(int direction, const KeyT &key)
            : m_direction(direction)
            , m_has_bound(true)
            , m_key_bound(key)
        {
        }

        template <typename KeyT> void limitBy(const KeyT &key) {
            this->m_key_bound = key;
            this->m_has_bound = true;
        }

        /**
         * @return true if specified key is within the accepted bounds
         */
        template <typename KeyT> bool operator()(const KeyT &key) const {
            if (this->m_has_bound) {
                return (m_direction > 0 && m_compare(key, m_key_bound))
                       || (m_direction < 0 && m_compare(m_key_bound, key));
            }
            return true;
        }

        bool hasBound() const {
            return this->m_has_bound;
        }

        const item_t& getBound() const {
            return this->m_key_bound;
        }
    };

}