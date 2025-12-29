// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SparseBoolMatrix.hpp"
#include <algorithm>
#include <cassert>

namespace db0

{

    SparseBoolMatrix::SparseBoolMatrix(std::optional<std::uint32_t> dim2_limit, unsigned int sort_threshold)
        : m_dim2_limit(dim2_limit)
        , m_sort_threshold(sort_threshold)
    {
    }

    void SparseBoolMatrix::set(std::pair<std::uint32_t, std::uint32_t> loc, bool value)
    {
        if (loc.second == 0) {
            if (loc.first >= m_dim1.size()) {
                m_dim1.resize(loc.first + 1, false);
            }
            m_dim1[loc.first] = value;
        } else {
            getColumn(loc.first).set(loc.second - 1, value);
        }
    }
    
    bool SparseBoolMatrix::get(std::pair<std::uint32_t, std::uint32_t> loc) const
    {
        if (loc.second == 0) {
            if (loc.first >= m_dim1.size()) {
                return false;
            }
            return m_dim1[loc.first];
        } else {
            const auto *col = findColumn(loc.first);
            if (!col) {
                return false;
            }
            return col->get(loc.second - 1);
        }
    }

    void SparseBoolMatrix::clear()
    {
        m_dim1.clear();
        m_dim2.clear();
    }

    SparseBoolMatrix::Column::Column(std::optional<std::uint32_t> dim2_limit, std::uint32_t key)
        : m_resize(!dim2_limit.has_value())
        , m_key(key)
    {
        if (dim2_limit) {
            m_data.resize(*dim2_limit, false);
        }
    }

    void SparseBoolMatrix::Column::set(std::uint32_t at, bool value)
    {
        if (m_resize) {
            if (at >= m_data.size()) {
                m_data.resize(at + 1, false);
            }
        }
        assert(at < m_data.size());
        m_data[at] = value;
    }

    bool SparseBoolMatrix::Column::get(std::uint32_t at) const
    {
        if (at >= m_data.size()) {
            return false;
        }
        return m_data[at];
    }

    const SparseBoolMatrix::Column *SparseBoolMatrix::findColumn(std::uint32_t key) const
    {
        if (m_dim2.size() < m_sort_threshold) {
            for (auto &col : m_dim2) {
                if (col.m_key == key) {
                    return &col;
                }
            }
            return nullptr;
        }

        auto it = std::lower_bound(m_dim2.begin(), m_dim2.end(), key,
            [](const Column &col, std::uint32_t key) { return col.m_key < key; });
        if (it == m_dim2.end() || it->m_key != key) {
            return nullptr;
        }
        return &(*it);
    }

    SparseBoolMatrix::Column &SparseBoolMatrix::getColumn(std::uint32_t key)
    {
        if (m_dim2.size() < m_sort_threshold) {
            for (auto &col : m_dim2) {
                if (col.m_key == key) {
                    return col;
                }
            }
            m_dim2.emplace_back(m_dim2_limit, key);
            if (m_dim2.size() < m_sort_threshold) {
                return m_dim2.back();
            } else {
                // sort existing columns
                std::sort(m_dim2.begin(), m_dim2.end(),
                    [](const Column &a, const Column &b) { return a.m_key < b.m_key; });
                // and continue with binary search ...
            }            
        }

        auto it = std::lower_bound(m_dim2.begin(), m_dim2.end(), key,
            [](const Column &col, std::uint32_t key) { return col.m_key < key; });
        if (it == m_dim2.end() || it->m_key != key) {
            // insert new column
            it = m_dim2.insert(it, Column(m_dim2_limit, key));
        }
        return *it;
    }

}
