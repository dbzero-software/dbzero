// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "v_sgtree.hpp"
#include <dbzero/core/intrusive/base_traits.hpp>
#include <dbzero/core/vspace/v_object.hpp>
	
namespace db0 

{
	    
    /**
     * VSPACE node type compliant with intrusive containers
     * c_type - node container type
     * comp_t - node pointer comparer type
     */
    template <typename T, class comp_t_, class ptr_set_t = tree_ptr_set<Address> >
    class intrusive_node: public v_object<T>
    {
    public :
        using super = v_object<T>;
        using c_type = T;
        using ptr_t = typename v_object<T>::ptr_t;
        using comp_t = comp_t_;
        // type compliant with intrusive NodeTraits requirements
        using traits_t = base_traits_t<intrusive_node<c_type,comp_t>, ptr_t>;
        using tree_base_t = v_object<sg_tree_data<true_size_of<c_type>(), ptr_set_t> >;
        
        template <typename... Args> intrusive_node(Memspace &memspace, Args&&... args)
            : super(memspace, std::forward<Args>(args)...)
        {
        }
        
        // Copy constructor
        struct tag_copy {};
        intrusive_node(tag_copy, Memspace &memspace, Memspace &other_memspace, const ptr_t &other)
            : super(memspace, memspace, other_memspace, *other.getData())
        {
        }

        intrusive_node(const ptr_t &ptr)
            : super(ptr)
        {
        }

        /**
         * Cast to pointer
         */
        inline operator ptr_t&() {
            return *this;
        }
        
        /**
         * Cast to const-pointer
         */
        inline operator const ptr_t&() const {
            return *this;
        }
    };

}
