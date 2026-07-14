// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/full_text/FT_IndexIterator.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    template <typename KeyT, typename ValueT> struct DB0_PACKED_ATTR BlockItemT
    {
        using self_t = BlockItemT<KeyT, ValueT>;
        KeyT m_key = KeyT();
        ValueT m_value;

        BlockItemT() = default;

        inline BlockItemT(KeyT key, ValueT value)
            : m_key(key)
            , m_value(value)
        {
        }

        // construct from value
        inline BlockItemT(ValueT value)
            : m_value(value)
        {
        }

        // by-value + key comparator (for b-index side operations where there's by-value arrangement)
        inline bool operator<(const BlockItemT& other) const
        {
            if (m_value == other.m_value) {
                return m_key < other.m_key;
            }
            return m_value < other.m_value;
        }

        // by-key + value comparison
        inline bool ltByKey(const BlockItemT& other) const
        {
            if (m_key == other.m_key) {
                return m_value < other.m_value;
            }
            return m_key < other.m_key;
        }

        // by-key + value comparison
        inline bool gtByKey(const BlockItemT& other) const
        {
            if (m_key == other.m_key) {
                return m_value > other.m_value;
            }
            return m_key > other.m_key;
        }

        inline bool operator!=(const BlockItemT& other) const {
            return (m_key != other.m_key) || (m_value != other.m_value);
        }

        inline bool operator==(const BlockItemT& other) const {
            return (m_key == other.m_key) && (m_value == other.m_value);
        }

        // cast to value (required by the FT_Iterator implementations)
        inline operator ValueT() const {
            return m_value;
        }

        self_t &operator=(ValueT value)
        {
            m_value = value;
            return *this;
        }

        struct CompT
        {
            inline bool operator()(const BlockItemT& a, const BlockItemT& b) const {
                return a < b;
            }

            inline bool operator()(ValueT a, const BlockItemT& b) const {
                return a < b.m_value;
            }

            inline bool operator()(const BlockItemT& a, ValueT b) const {
                return a.m_value < b;
            }

            // value-only comparator, required by the FT_Iterator implementations
            inline bool operator()(ValueT a, ValueT b) const {
                return a < b;
            }
        };
        
        // comparator for building min-heap
        struct HeapCompT
        {
            bool operator()(const BlockItemT& a, const BlockItemT& b) const
            {
                if (a.m_key == b.m_key) {
                    return b.m_value < a.m_value;
                }
                return b.m_key < a.m_key;
            }
        };

        struct Hash
        {
            inline std::size_t operator()(const BlockItemT& item) const {
                return std::hash<KeyT>()(item.m_key) ^ std::hash<ValueT>()(item.m_value);
            }
        };

    };
DB0_PACKED_END
    
}
