// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/collections/sgtree/v_sgtree.hpp>
#include <dbzero/core/collections/sgtree/intrusive_node.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    /**
     * v_map's intrusive node (extends sg_node_base)
     * KeyT, data_t - overlaid types (variable length allowed)
     */
DB0_PACKED_BEGIN
    template <class KeyT, class data_t> class DB0_PACKED_ATTR v_map_node
        : public o_ext<v_map_node<KeyT,data_t>, sg_node_base<>, 0, false >
    {
    protected :        
        using super_t = o_ext<v_map_node<KeyT,data_t>, sg_node_base<>, 0, false >;
        using self = v_map_node<KeyT, data_t>;
        friend super_t;

        template <typename T, typename... Args> v_map_node(const T &key_arg, Args&&... args)
            : super_t()
        {
            std::byte *at = self::beginOfDynamicArea();
            at += KeyT::__new(at, key_arg).sizeOf();
            data_t::__new(at, std::forward<Args>(args)...);
        }

    public :
        using KeyType = KeyT;
        using mapped_type = data_t;

        template <typename T, typename... Args> static size_t measure(const T &key_arg, Args&&... args)
        {
            size_t size = self::measureBase();
            size += KeyT::measure(key_arg);
            size += data_t::measure(std::forward<Args>(args)...);
            return size;
        }

        template <class buf_t> static size_t safeSizeOf(buf_t at) 
        {
            size_t size = self::safeBaseSize(at);
            size += KeyT::__const_ref(&at[size]).sizeOf();
            size += data_t::__const_ref(&at[size]).sizeOf();
            return size;
        }

        const KeyT &first() const 
        {
            const std::byte *at = self::beginOfDynamicArea();
            return KeyT::__const_ref(at);
        }

        const data_t &second() const 
        {
            const std::byte *at = self::beginOfDynamicArea();
            at += first().sizeOf();
            return data_t::__const_ref(at);
        }

        KeyT &first() 
        {
            std::byte *at = self::beginOfDynamicArea();
            return KeyT::__ref(at);
        }
            
        data_t &second() 
        {
            std::byte *at = self::beginOfDynamicArea();
            at += first().sizeOf();
            return data_t::__ref(at);
        }

        void destroy (db0::Memspace &memspace) const 
        {
            first().destroy(memspace);
            second().destroy(memspace);
        }
    };
DB0_PACKED_END

    template <typename KeyT, typename data_t, typename KeyCompT>
    class v_map_node_traits
    {
    public :
        using node_t = v_map_node<KeyT, data_t>;
        using node_ptr_t = typename v_object<node_t>::ptr_t;

        struct comp_t
        {
            KeyCompT m_key_comp;
            bool operator()(const node_ptr_t &n0, const node_ptr_t &n1) const {
                return m_key_comp(n0->first(), n1->first());
            }

            template <typename K> bool operator()(const K &k0, const node_ptr_t &n1) const {
                return m_key_comp(k0, n1->first());
            }

            template <typename K> bool operator()(const node_ptr_t &n0, const K &k1) const {
                return m_key_comp(n0->first(), k1);
            }
        };
    };

    template <typename KeyT, typename data_t, typename KeyCompT = std::less<KeyT> > 
    class v_map: public v_sgtree<intrusive_node<
            v_map_node<KeyT, data_t>, 
            typename v_map_node_traits<KeyT, data_t, KeyCompT>::comp_t> >
    {
    public :
        using node_t = intrusive_node<v_map_node<KeyT, data_t>, typename v_map_node_traits<KeyT, data_t, KeyCompT>::comp_t>;
        using super_t = v_sgtree<node_t>;
        using c_type = typename super_t::c_type;
        using ptr_t = typename super_t::ptr_t;
        using comp_t = typename v_map_node_traits<KeyT, data_t, KeyCompT>::comp_t;
        using KeyType = KeyT;
        using mapped_type = data_t;        

        /**
         * Create null instance
         */
        v_map() = default;

        /**
         * Create new V-Space instance
         */
        v_map(Memspace &memspace, comp_t _comp = comp_t())
            : super_t(memspace, _comp)
        {
        }

        /**
         * V-Space reference
         */
        v_map(const ptr_t &ptr, comp_t _comp = comp_t())
            : super_t(ptr, _comp)
        {
        }

        /**
         * Dereference v-space existing object
         */
        v_map(mptr _ptr,comp_t _comp = comp_t())
            : super_t(_ptr, _comp)
        {
        }        
    };

} 
