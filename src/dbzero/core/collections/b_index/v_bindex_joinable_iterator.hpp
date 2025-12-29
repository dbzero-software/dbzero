// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "v_bindex_joinable_const_iterator.hpp"
#include "v_bindex_iterator.hpp"

namespace db0

{

    template <typename item_t, typename AddrT, typename item_comp_t = std::less<item_t> >
    class v_bindex_joinable_iterator : public v_bindex_joinable_const_iterator<item_t, AddrT, item_comp_t> 
    {
        using super_t = v_bindex_joinable_const_iterator<item_t, AddrT, item_comp_t>;
        using types_t = bindex_types<item_t, AddrT, item_comp_t>;
        using node_iterator = typename types_t::node_iterator;
        using bindex_tree_t = typename types_t::bindex_tree_t;
        using iterator = v_bindex_iterator<item_t, AddrT, item_comp_t>;
    public :

        v_bindex_joinable_iterator(bindex_tree_t &index, std::uint32_t max_size, int direction)
            : super_t(index, max_size, direction)
        {
        }
        
        /**
         * NOTICE : not allowed to modify key part of the item
         */
        item_t &modifyItem()
        {
            this->m_data_buf.modify();
            return (item_t&)(*this->m_it_data);
        }

        /**
         * Convert to a regular iterator
         * @return as regular iterator
         */
        iterator getIterator() const
        {
            return iterator(this->getMemspace(), this->m_node, this->m_index_ptr->begin(), this->m_index_ptr->end(),
                this->m_data_buf, this->m_it_data.getIterator());
        }
    };

}