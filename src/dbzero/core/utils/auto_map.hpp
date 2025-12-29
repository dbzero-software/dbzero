// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <deque>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    // auto_map is a simple collection with ordinal auto-generated values
    // it can be used for mapping long addresses into smaller ordinal values in-memory
    // the capacity of auto_map is determined by the capacity of the ValueT type
    template <typename KeyT, typename ValueT> class auto_map
    {
    public:
        // adds a new key if it is unique and return the auto-assigned ordinal value
        ValueT addUnique(KeyT key);
        // erase key if it exists
        void erase(KeyT);

    private:
        std::unordered_map<KeyT, ValueT> m_map;
        // reusable values
        std::deque<ValueT> m_unused;
        ValueT m_next_value = 1;
    };
    
    template <typename KeyT, typename ValueT> ValueT auto_map<KeyT, ValueT>::addUnique(KeyT key)
    {
        auto it = m_map.find(key);
        if (it != m_map.end()) {
            return it->second;
        }
        ValueT value;
        if (m_unused.empty()) {
            if (m_next_value == std::numeric_limits<ValueT>::max()) {
                THROWF(db0::InternalException) << "auto_map: capacity exceeded";
            }
            value = m_next_value++;
        }
        else {
            value = m_unused.front();
            m_unused.pop_front();
        }
        m_map[key] = value;
        return value;
    }

    template <typename KeyT, typename ValueT> void auto_map<KeyT, ValueT>::erase(KeyT key)
    {
        auto it = m_map.find(key);
        if (it == m_map.end()) {
            return;
        }
        m_unused.push_back(it->second);
        m_map.erase(it);
    }
    
}