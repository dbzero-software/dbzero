// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include "v_bindex_iterator.hpp"

namespace db0

{

    template <typename item_t, typename AddrT, typename item_comp_t = std::less<item_t> >
        class v_bindex_const_iterator : protected v_bindex_iterator<item_t, AddrT, item_comp_t>
    {
        using super_t = v_bindex_iterator<item_t, AddrT, item_comp_t>;
        using iterator = v_bindex_iterator<item_t, AddrT, item_comp_t>;
        using types_t = bindex_types<item_t, AddrT, item_comp_t>;
        using node_iterator = typename types_t::node_iterator;
        using DataVectorT = typename types_t::data_vector;
    public:

        v_bindex_const_iterator() = default;

        v_bindex_const_iterator(const iterator &it)
            : iterator(it)
        {
        }

        v_bindex_const_iterator(Memspace &memspace, const node_iterator &it_node, 
            const node_iterator &it_begin, const node_iterator &it_end, const DataVectorT &data_buf, 
            typename DataVectorT::const_iterator it_data)
            : iterator(memspace, it_node, it_begin, it_end, data_buf, it_data)
        {
        }

        v_bindex_const_iterator (Memspace &memspace, const node_iterator &it_node,
            const node_iterator &it_begin, const node_iterator &it_end)
            : iterator(memspace, it_node, it_begin, it_end)
        {
        }

        const item_t &operator*() const {
            return iterator::operator *();
        }

        v_bindex_const_iterator& operator++() 
        {
            iterator::operator ++();
            return *this;
        }

        v_bindex_const_iterator& operator--() 
        {
            iterator::operator --();
            return *this;
        }

        bool operator!=(const v_bindex_const_iterator &it) const {
            return (this->m_node_iterator != it.m_node_iterator);
        }

        bool operator==(const v_bindex_const_iterator &it) const {
            return (this->m_node_iterator == it.m_node_iterator);
        }

        bool is_end() const {
            return super_t::is_end();
        }
    };

}