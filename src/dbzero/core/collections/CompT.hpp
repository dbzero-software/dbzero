// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

namespace db0

{

    /**
     * @tparam data_t - overlaid container type
     * @tparam get_t - get key function object
     * @tparam key_comp_t - key comparator
     */
    template <class data_t,class get_t,class key_comp_t>
        struct CompT
    {
        using Initializer = typename data_t::Initializer;
        key_comp_t m_comp;
        get_t m_get;
        
        CompT(key_comp_t comp = key_comp_t())
            : m_comp(comp)
        {
        }
        
        bool operator()(const data_t &n0, const data_t &n1) const {
            return m_comp(m_get(n0), m_get(n1));
        }
        
        bool operator()(const Initializer &d0, const data_t &n1) const {
            return m_comp(m_get(d0), m_get(n1));
        }
        
        bool operator()(const data_t &n0, const Initializer &d1) const {
            return m_comp(m_get(n0), m_get(d1));
        }

        bool operator()(const Initializer &d0, const Initializer &d1) const {
            return m_comp(m_get(d0), m_get(d1));
        }
    };

}
