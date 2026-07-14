// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include "sgb_tree_node.hpp"
#include "sgb_types.hpp"

namespace db0

{

    // This is a generic wrapper over a simple type key
    // it demonstrates the SGB_Tree key's requirements and is mostly used for testing purposes
DB0_PACKED_BEGIN    
    template <typename TypeT = std::uint64_t>
    struct DB0_PACKED_ATTR SGB_KeyT
    {
        TypeT m_value;

        inline SGB_KeyT(TypeT value) : m_value(value) {}
        inline SGB_KeyT() = default;

        inline operator TypeT() const {
            return m_value;
        }

        static inline TypeT getKey(TypeT value) {
            return value;
        }

        // assignment operator
        inline SGB_KeyT &operator=(const SGB_KeyT &other) {
            m_value = other.m_value;
            return *this;
        }

        template <typename T>
        inline bool operator==(T other) const {
            return m_value == other;
        }

        template <typename T>
        inline bool operator!=(T other) const {
            return m_value != other;
        }
        
        template <typename T>
        inline bool operator<(T other) const {
            return m_value < other;
        }

        template <typename T>
        inline bool operator<=(T other) const {
            return m_value <= other;
        }

        template <typename T>
        inline bool operator>(T other) const {
            return m_value > other;
        }

        template <typename T>
        inline bool operator>=(T other) const {
            return m_value >= other;
        }
    };
DB0_PACKED_END
    
}