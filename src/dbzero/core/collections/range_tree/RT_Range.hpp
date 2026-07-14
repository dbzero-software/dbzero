// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{

    template <typename KeyT> struct RT_Range
    {
        std::optional<KeyT> m_min;
        bool m_min_inclusive = false;
        std::optional<KeyT> m_max;
        bool m_max_inclusive = false;

        RT_Range() = default;
        RT_Range(const std::optional<KeyT> &min, bool min_inclusive, const std::optional<KeyT> &max, bool max_inclusive)
            : m_min(min)
            , m_min_inclusive(min_inclusive)
            , m_max(max)
            , m_max_inclusive(max_inclusive) 
        {}

        // deserialization constructor
        RT_Range(std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end);

        bool contains(KeyT key) const;

        void serialize(std::vector<std::byte> &v) const;

        static RT_Range<KeyT> deserialize(std::vector<std::byte>::const_iterator &iter, 
            std::vector<std::byte>::const_iterator end);
    };

    template <typename KeyT>
    RT_Range<KeyT>::RT_Range(std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
    {
        if (db0::serial::read<bool>(iter, end)) {
            m_min = db0::serial::read<KeyT>(iter, end);
            m_min_inclusive = db0::serial::read<bool>(iter, end);
        }
        if (db0::serial::read<bool>(iter, end)) {
            m_max = db0::serial::read<KeyT>(iter, end);
            m_max_inclusive = db0::serial::read<bool>(iter, end);
        }
    }
    
    template <typename KeyT>
    bool RT_Range<KeyT>::contains(KeyT key) const
    {        
        if (m_min && (key < *m_min || (!m_min_inclusive && key == *m_min))) {
            return false;
        }
        if (m_max && (key > *m_max || (!m_max_inclusive && key == *m_max))) {
            return false;
        }        
        return true;
    }

    template <typename KeyT>
    void RT_Range<KeyT>::serialize(std::vector<std::byte> &v) const
    {
        db0::serial::write<bool>(v, static_cast<bool>(m_min));
        if (m_min) {
            db0::serial::write(v, *m_min);
            db0::serial::write(v, m_min_inclusive);
        }
        db0::serial::write<bool>(v, static_cast<bool>(m_max));
        if (m_max) {
            db0::serial::write(v, *m_max);
            db0::serial::write(v, m_max_inclusive);
        }
    }
    
    template <typename KeyT>
    RT_Range<KeyT> RT_Range<KeyT>::deserialize(std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        bool has_min = db0::serial::read<bool>(iter, end);
        std::optional<KeyT> min;
        bool min_inclusive = false;
        if (has_min) {
            min = db0::serial::read<KeyT>(iter, end);
            min_inclusive = db0::serial::read<bool>(iter, end);            
        }        
        bool has_max = db0::serial::read<bool>(iter, end);
        std::optional<KeyT> max;
        bool max_inclusive = false;
        if (has_max) {
            max = db0::serial::read<KeyT>(iter, end);
            max_inclusive = db0::serial::read<bool>(iter, end);            
        }
        return RT_Range<KeyT>(min, min_inclusive, max, max_inclusive);
    }
    
}
