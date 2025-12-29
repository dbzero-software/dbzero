// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include "sgb_tree_node.hpp"
#include "sgb_tree_head.hpp"
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/collections/sgtree/v_sgtree.hpp>
#include <dbzero/core/intrusive/base_traits.hpp>
#include <dbzero/core/serialization/Types.hpp>

namespace db0

{

    template <typename ContainerT, typename ItemT, typename ItemCompT>
    class sgb_node_traits
    {
    public :
        using node_ptr_t = typename v_object<ContainerT>::ptr_t;
        
        struct comp_t
        {
            ItemCompT itemComp;
            comp_t(ItemCompT _comp = {})
                : itemComp(_comp)
            {
            }

            bool operator()(const node_ptr_t &lhs, const node_ptr_t &rhs) const {
                return itemComp(lhs->keyItem(), rhs->keyItem());
            }

            template <typename LhsT> bool operator()(const LhsT &lhs, const node_ptr_t &rhs) const {
                return itemComp(lhs, rhs->keyItem());
            }		

            template <typename RhsT> bool operator()(const node_ptr_t &lhs, const RhsT &rhs) const {
                return itemComp(lhs->keyItem(), rhs);
            }

            template <typename LhsT, typename std::enable_if<!std::is_same<LhsT, node_ptr_t>::value>::type>
            bool operator()(const LhsT &lhs, const ItemT &rhs) const {
                return itemComp(lhs, rhs);
            }

            template <typename RhsT, typename std::enable_if<!std::is_same<RhsT, node_ptr_t>::value>::type>
            bool operator()(const ItemT &lhs, const RhsT &rhs) const {
                return itemComp(lhs, rhs);
            }
        };
    };

    template <typename ContainerT, typename ItemT, typename ItemCompT, typename CompT, typename TreeHeaderT> 
    class SGB_IntrusiveNode : public v_object<ContainerT>
    {
    public :
        using super_t = v_object<ContainerT>;
        using c_type = ContainerT;
        using ptr_t = typename super_t::ptr_t;
        using comp_t = CompT;
        // type compliant with intrusive NodeTraits requirements
        using traits_t = base_traits_t<SGB_IntrusiveNode<c_type, ItemT, ItemCompT, comp_t, TreeHeaderT>, ptr_t>;
        using tree_base_t = v_object<o_sgb_tree_head<typename ContainerT::capacity_t, typename ContainerT::address_t, TreeHeaderT> >;

        SGB_IntrusiveNode() = default;
        
        template <typename... Args> SGB_IntrusiveNode(Memspace &memspace, Args&&... args)
            : super_t(memspace, std::forward<Args>(args)...)
        {
        }

        SGB_IntrusiveNode(const ptr_t &ptr)
            : super_t(ptr)
        {
        }

        inline operator ptr_t&() {
            return *this;
        }
        
        inline operator const ptr_t&() const {
            return *this;
        }
    };
    
    template <
        typename ItemType, 
        typename ItemCompType, 
        typename ItemEqualType,
        typename CapacityType = std::uint16_t, 
        typename AddressType = std::uint64_t,
        typename HeaderType = db0::o_null,
        typename TreeHeaderType = db0::o_null>
    class sgb_types
    {
    public :
        using ItemT = ItemType;
        using ItemCompT = ItemCompType;
        using ItemEqualT = ItemEqualType;
        using CapacityT = CapacityType;
        using AddressT = AddressType;
        using HeaderT = HeaderType;
        using TreeHeaderT = TreeHeaderType;
        using o_sgb_node_t = o_sgb_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT>;
        using node_traits = sgb_node_traits<o_sgb_node_t, ItemT, ItemCompT>;
        using ptr_set_t = sgb_tree_ptr_set<AddressT>;
        using NodeT = SGB_IntrusiveNode<o_sgb_node_t, ItemT, ItemCompT, typename node_traits::comp_t, TreeHeaderT>;
        using CompT = typename NodeT::comp_t;
        using NodeItemCompT = typename o_sgb_node_t::CompT;
        using NodeItemEqualT = typename o_sgb_node_t::EqualT;
        using HeapCompT = typename o_sgb_node_t::HeapCompT;
        
        using SG_TreeT = v_sgtree<NodeT, intrusive::detail::h_alpha_sqrt2_t>;
    };
    
}