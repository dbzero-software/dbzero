// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "v_sgtree.hpp"
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
	
DB0_PACKED_BEGIN
    template <class data_t, class ptr_set_t = tree_ptr_set<Address> >
    class DB0_PACKED_ATTR o_sgtree_node
        : public o_ext<o_sgtree_node<data_t, ptr_set_t>, sg_node_base<ptr_set_t>, 0, false >
    {
    public:
        typedef typename data_t::Initializer Initializer;
        using has_constant_size = typename data_t::has_constant_size;
        
        o_sgtree_node(Memspace &memspace, Memspace &other_memspace, const o_sgtree_node &other)
            : m_data(memspace, other_memspace, other.m_data)
        {
            this->getSuper() = other.getSuper();
        }

        static o_sgtree_node &__new(std::byte *buf, Memspace &memspace, Memspace &other_memspace,
            const o_sgtree_node &other)
        {
            new (buf) o_sgtree_node(memspace, other_memspace, other);
            return *(o_sgtree_node*)buf;
        }

        // overlaid constructor by initializer
        static o_sgtree_node &__new(std::byte *buf, const Initializer &data)
        {
            std::byte *_buf = buf;
            buf += sg_node_base<ptr_set_t>::__new(_buf).sizeOf();
            data_t::__new(buf,data);
            return (o_sgtree_node<data_t,ptr_set_t>&)(*_buf);
        }
        
        static std::size_t measure(Memspace &, Memspace &, const o_sgtree_node &other) {
            return other.sizeOf();
        }

        static std::size_t measure(const Initializer &data)
        {
            std::size_t size = sg_node_base<ptr_set_t>::sizeOf();
            size += data_t::measure(data);
            return size;
        }

        template <typename = typename std::enable_if<has_constant_size::value>::type>
        static std::size_t measure () 
        {
            std::size_t size = sg_node_base<ptr_set_t>::sizeOf();
            size += data_t::measure();
            return size;
        }
        
        template <class buf_t> static size_t safeSizeOf(buf_t buf)
        {
            std::size_t size = sg_node_base<ptr_set_t>::sizeOf();
            size += data_t::safeSizeOf(&buf[size]);
            return size;
        }
        
        void destroy(Memspace &memspace) const {
            m_data.destroy(memspace);
        }
        
        const data_t *operator->() const {
            return &m_data;
        }
                
    public :
        data_t m_data;
    };
DB0_PACKED_END

    template <class data_t,class data_comp_t>
    class o_sgtree_node_traits
    {
    public:
        using Initializer = typename data_t::Initializer;
        using node_ptr_t = typename v_object<o_sgtree_node<data_t> >::ptr_t;
        
        struct comp_t
        {
            data_comp_t _comp;
            comp_t(data_comp_t _comp = data_comp_t())
                : _comp(_comp)
            {
            }

            bool operator()(const node_ptr_t &n0,const node_ptr_t &n1) const {
                return _comp(n0->m_data, n1->m_data);
            }

            bool operator()(const Initializer &d0,const node_ptr_t &n1) const {
                return _comp(d0, n1->m_data);
            }		

            bool operator()(const node_ptr_t &n0,const Initializer &d1) const {
                return _comp(n0->m_data, d1);
            }

            bool operator()(const Initializer &d0,const Initializer &d1) const {
                return _comp(d0, d1);
            }
        };
    };
    
}
	