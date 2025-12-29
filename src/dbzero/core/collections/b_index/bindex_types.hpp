// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdlib>
#include <functional>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/collections/sgtree/v_sgtree.hpp>
#include <dbzero/core/collections/sgtree/sgtree_node.hpp>
#include <dbzero/core/collections/sgtree/intrusive_node.hpp>
#include <dbzero/core/collections/vector/v_sorted_vector.hpp>
#include <dbzero/core/collections/CompT.hpp>
#include <dbzero/object_model/object_header.hpp>
#include "type.hpp"
#include <dbzero/core/compiler_attributes.hpp>
namespace db0 

{
    
    /**
     * This type is used for size profiling
     */
    struct BIndexStorageSize
    {
        // size of v_bindex type itself
        std::size_t index_size = 0;
        // total number of nodes (chunks) in data structure
        std::size_t node_count = 0;
        // size occupied by nodes
        std::size_t node_size = 0;
        // size occupied by data vectors
        std::size_t data_size = 0;

        operator std::size_t() const {
            return index_size + node_size + data_size;
        }
    };
    
    /**
     * v_bindex supports following random operations :
     * find by key, insert, delete
     * data is organized in sorted blocks, blocks are indexed by SG-Tree
     * block key is the smallest item contained (lo_bound)
     * NOTICE : only fixed-size items are supported
     */
    template <class item_t, typename AddrT, class item_comp_t = std::less<item_t> >
        class bindex_types
    {
    public :
        using data_vector = v_sorted_vector<item_t, AddrT, item_comp_t>;
        using DestroyF = std::function<void(const item_t &)>;

DB0_PACKED_BEGIN
        class DB0_PACKED_ATTR bindex_node : public o_fixed<bindex_node> 
        {
            using super_t = o_fixed<bindex_node>;
        public :
            using Initializer = item_t;

            bindex_node(const item_t &data)
                : lo_bound(data)            
            {
            }

            // Copy constructor (possibly to a different memspace)
            bindex_node(Memspace &memspace, Memspace &other_memspace, const bindex_node &other)
                : lo_bound(other.lo_bound)
            {
                if (other.ptr_b_data.isValid()) {
                    data_vector other_dv(other_memspace.myPtr(other.ptr_b_data));
                    data_vector new_dv(memspace, other_dv);
                    ptr_b_data = new_dv.getAddress();
                }
            }

            void destroy(Memspace &memspace) const
            {
                if (ptr_b_data.isValid()) {
                    data_vector dv(memspace.myPtr(ptr_b_data));
                    dv.destroy();
                }
            }

        public :
            item_t lo_bound;
            // data block (v_sorted_vector)
            Address ptr_b_data = {};

            struct get_key 
            {
                const item_t &operator()(const bindex_node &node) const {
                    return *(const item_t*)(&node.lo_bound);
                }
                
                const item_t &operator()(const item_t &data) const {
                    return data;
                }
            };

            using comp_t = CompT<bindex_node,get_key,item_comp_t>;
        };
DB0_PACKED_END

        using bindex_node_traits = o_sgtree_node_traits<bindex_node, typename bindex_node::comp_t>;
        using bindex_node_t = intrusive_node<
            o_sgtree_node<bindex_node> ,
            typename bindex_node_traits::comp_t>;
        
        using bindex_tree_t = v_sgtree<bindex_node_t, intrusive::detail::h_alpha_sqrt2_t>;

        template <class KeyT, class comp_t = std::less<KeyT> >
            class cast_then_compare
        {
        public :
            comp_t key_comp;

            bool operator()(const typename bindex_node_traits::node_ptr_t &node, KeyT key) const {
                return key_comp(static_cast<KeyT>(node->m_data.lo_bound), key);
            }

            bool operator()(KeyT key,const typename bindex_node_traits::node_ptr_t &node) const {
                return key_comp(key, static_cast<KeyT>(node->m_data.lo_bound));
            }
        };
        
        using node_iterator = typename bindex_tree_t::iterator;
        using node_stack = typename bindex_tree_t::join_stack;

DB0_PACKED_BEGIN
        class DB0_PACKED_ATTR bindex_container : public o_fixed_versioned<bindex_container> 
        {
        public :
            // common dbzero object header (not copied)
            db0::o_object_header m_header;
            // block index
            Address ptr_index = {};
            // total number of items contained
            std::uint64_t size = 0;

            bindex_container() = default;

            bindex_container(const bindex_container &other)
                : size(other.size)
            {
            }
        };
DB0_PACKED_END
    };
    
} 
