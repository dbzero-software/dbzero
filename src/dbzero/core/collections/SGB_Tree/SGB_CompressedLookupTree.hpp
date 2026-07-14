// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

/**
* A compressed SGB lookup tree enhances on SGB lookup tree by allowing 
* storage of node-relative elements (compressed) to save on storage space
*/

#include "SGB_LookupTree.hpp"
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/metaprog/misc_utils.hpp>
#include <dbzero/core/memory/config.hpp>

namespace db0

{

    template <
        typename ItemT,
        typename KeyItemT,
        typename CapacityT, 
        typename AddressT, 
        typename ItemCompT,         
        typename ItemEqualT, 
        typename HeaderT,
        int D = 2>
DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_sgb_compressed_lookup_tree_node:
    public o_ext<
        o_sgb_compressed_lookup_tree_node<ItemT, KeyItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 
        o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 0, false>
    {
    protected:
        using ext_t = o_ext<
            o_sgb_compressed_lookup_tree_node<ItemT, KeyItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 
            o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 0, false>;
        using super_t = o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>;
        using base_t = typename super_t::super_t;

    public: 
        using iterator = typename super_t::iterator;
        using const_iterator = typename super_t::const_iterator;
        using HeapCompT = typename super_t::HeapCompT;
        
        o_sgb_compressed_lookup_tree_node(const KeyItemT &item, CapacityT capacity, const HeapCompT &comp)
            : ext_t(capacity)
        {
            // make sure at least 1 item can be appended
            assert(capacity >= this->measureSizeOf(1));
            // initialize header by compressing the key item (first item in the node)
            // need to append with base, otherwise is_sorted_flag would be erased
            base_t::append(comp, this->header().compressFirst(item));
            assert(this->size() == 1);
        }
        
        static std::size_t measure(const KeyItemT &, CapacityT capacity, const HeapCompT &) {
            return capacity;
        }

        /**
         * Note that type of the key item (uncompressed) is different from other items (compressed)
         */
        KeyItemT keyItem() const {
            return this->header().uncompress(super_t::keyItem());
        }
        
        iterator append(const HeapCompT &comp, const KeyItemT &item) {
            return super_t::append(comp, this->header().compress(item));
        }

        /**
         * Rebalance sorted node by moving items after "at" to the other node
         * and finally removing the "at" item
        */
        void rebalance_at(const_iterator at, o_sgb_compressed_lookup_tree_node &other, const HeapCompT &comp)
        {
            assert(this->is_sorted());
            if (this->is_reversed()) {
                rebalance_at<ReverseOperators<const_iterator> >(at, other, comp);
            } else {
                rebalance_at<Operators<const_iterator> >(at, other, comp);
            }
        }
        
    protected:
        
        template <typename op> void rebalance_at(const_iterator at, o_sgb_compressed_lookup_tree_node &other, 
            const HeapCompT &comp)
        {
            assert(this->is_sorted());
            auto len = op::sub(this->cend(), at);
            op::next(at);
            other.append_sorted<op>(*this, at, this->cend(), comp);
            // remove elements including "at"
            this->m_size -= len;
        }
        
        template <typename op_src> void append_sorted(const o_sgb_compressed_lookup_tree_node &from,
            const_iterator begin, const_iterator end, const HeapCompT &comp)
        {
            if (this->is_reversed()) {
                append_sorted<op_src, ReverseOperators<iterator> >(from, begin, end, comp);
            } else {
                append_sorted<op_src, Operators<iterator> >(from, begin, end, comp);
            }
        }

        template <typename op_src, typename op> void append_sorted(const o_sgb_compressed_lookup_tree_node &from,
            const_iterator begin, const_iterator end, const HeapCompT &comp)
        {            
            if (begin == end) {
                return;
            }
            
            const auto &from_head = from.header();
            auto &this_head = this->header();
            if (!this->is_sorted() || 
                comp.itemComp(this_head.compress(from_head.uncompress(*begin)), *this->find_max(comp))) 
            {
                // must append one-by-one
                while (begin != end) {
                    // change element's base (uncompress / compress)
                    super_t::append(comp, this_head.compress(from_head.uncompress(*begin)));
                    op_src::next(begin);
                }
            } else {
                // copy sorted elements
                auto len = op_src::sub(end, begin);
                auto out = this->end();
                while (begin != end) {
                    // change element's base
                    *out = this_head.compress(from_head.uncompress(*begin));
                    op_src::next(begin);
                    op::next(out);
                }                
                this->m_size += len;
            }
        }
    };
DB0_PACKED_END
    
    template <
        typename ItemType, 
        typename CompressedItemType,
        typename ItemCompType, 
        typename CompressedItemCompType, 
        typename ItemEqualType,
        typename CompressedItemEqualType,
        typename CapacityType = std::uint16_t, 
        typename AddressType = std::uint64_t,
        typename HeaderType = db0::o_null,
        typename TreeHeaderType = db0::o_null>
    class sgb_compressed_lookup_types
    {
    public :
        using ItemT = ItemType;
        using CompressedItemT = CompressedItemType;
        using ItemCompT = ItemCompType;
        using CompressedItemCompT = CompressedItemCompType;
        using ItemEqualT = ItemEqualType;
        using CompressedItemEqualT = CompressedItemEqualType;
        using CapacityT = CapacityType;
        using AddressT = AddressType;
        using HeaderT = HeaderType;
        using TreeHeaderT = TreeHeaderType;
        // Nodes hold compressed elements
        using o_sgb_node_t = o_sgb_compressed_lookup_tree_node<CompressedItemT, ItemT, CapacityT, AddressT, CompressedItemCompT, CompressedItemEqualT, HeaderT>;
        // SG-Tree compares uncompressed elements
        using node_traits = sgb_node_traits<o_sgb_node_t, ItemT, ItemCompT>;
        using ptr_set_t = sgb_tree_ptr_set<AddressT>;
        using NodeT = SGB_IntrusiveNode<o_sgb_node_t, ItemT, ItemCompT, typename node_traits::comp_t, TreeHeaderT>;
        using CompT = typename NodeT::comp_t;
        using NodeItemCompT = typename o_sgb_node_t::CompT;
        using NodeItemEqualT = typename o_sgb_node_t::EqualT;
        using HeapCompT = typename o_sgb_node_t::HeapCompT;
        
        using SG_TreeT = v_sgtree<NodeT, intrusive::detail::h_alpha_sqrt2_t>;
    };
    
    template <
        typename ItemT,
        typename CompressedItemT,
        typename CompressingHeaderT,
        typename ItemCompT = std::less<ItemT> ,
        typename CompressedItemCompT = std::less<CompressedItemT> ,
        typename ItemEqualT = std::equal_to<ItemT> ,
        typename CompressedItemEqualT = std::equal_to<CompressedItemT> ,
        typename TreeHeaderT = db0::o_null,
        typename CapacityT = std::uint16_t,
        typename AddressT = Address >
    class SGB_CompressedLookupTree: 
    protected SGB_LookupTreeBase<
        sgb_compressed_lookup_types<
            ItemT, 
            CompressedItemT, 
            ItemCompT, 
            CompressedItemCompT, 
            ItemEqualT, 
            CompressedItemEqualT, 
            CapacityT, 
            AddressT, 
            CompressingHeaderT,
            TreeHeaderT> >
    {
    protected:
        using super_t = SGB_LookupTreeBase<
            sgb_compressed_lookup_types<
                ItemT, 
                CompressedItemT, 
                ItemCompT, 
                CompressedItemCompT, 
                ItemEqualT, 
                CompressedItemEqualT, 
                CapacityT, 
                AddressT, 
                CompressingHeaderT,
                TreeHeaderT> >;
        using base_t = typename super_t::base_t;

    public:
        using sg_tree_const_iterator = typename super_t::sg_tree_const_iterator;
        using ItemIterator = typename super_t::ItemIterator;
        using ConstItemIterator = typename super_t::ConstItemIterator;
        using CompT = typename super_t::CompT;
        using NodeItemCompT = typename super_t::NodeItemCompT;
        using NodeItemEqualT = typename super_t::NodeItemEqualT;
        using const_iterator = typename super_t::const_iterator;

        // as null / invalid
        SGB_CompressedLookupTree() = default;
        
        SGB_CompressedLookupTree(Memspace &memspace, std::size_t node_capacity,
            AccessType access_type, const CompT &comp = {}, const NodeItemCompT &item_cmp = {}, const NodeItemEqualT &item_eq = {},
            unsigned int sort_thr = super_t::DEFAULT_SORT_THRESHOLD)
            : super_t(memspace, node_capacity, access_type, comp, item_cmp, item_eq, sort_thr)
        {
        }
        
        SGB_CompressedLookupTree(mptr ptr, std::size_t node_capacity,
            AccessType access_type, const CompT &comp = {}, const NodeItemCompT &item_cmp = {}, const NodeItemEqualT &item_eq = {},
            unsigned int sort_thr = super_t::DEFAULT_SORT_THRESHOLD)
            : super_t(ptr, node_capacity, access_type, comp, item_cmp, item_eq, sort_thr)
        {
        }

        void insert(const ItemT &item)
        {
            assert(this->m_access_type == AccessType::READ_WRITE);
            if (base_t::empty()) {
                super_t::emplace_to_empty(item);
                return;
            }
            
            ++base_t::modify().m_sgb_size;
            // Find the node by uncompressed key / item
            auto node = base_t::lower_equal_bound(item);
            if (node == base_t::end()) {
                node = base_t::begin();
            }
            
            insert_into(node, 0, item);
        }

        AddressT getAddress() const {
            return base_t::getAddress();
        }

        sg_tree_const_iterator cbegin_nodes() const {
            return base_t::begin();
        }

        sg_tree_const_iterator cend_nodes() const {
            return base_t::end();
        }

        bool empty() const {
            return super_t::empty();
        }

        std::size_t size() const {
            return super_t::size();
        }
        
        void commit() const {
            super_t::commit();
        }

        template <typename KeyT> ConstItemIterator findLower(const KeyT &key) const
        {
            auto node = base_t::lower_equal_bound(key);
            if (node == base_t::end()) {
                return { nullptr, sg_tree_const_iterator() };
            }
            
            // node will be sorted if needed (only if in READ/WRITE mode)
            if (this->m_access_type == AccessType::READ_WRITE) {                
                this->onNodeLookup(node);
            }
            
            if (node->header().canFit(key)) {
                // within the node look up by compressed key (only if able to fit)
                return { node->lower_equal_bound(node->header().compress(key), this->m_heap_comp), node };
            } else {
                // NOTE: since unable to fit key, it's larger than any item in this node
                return { node->find_max(this->m_heap_comp), node };
            }
        }

        /**
         * Note that return type is different from the base class
        */
        template <typename KeyT> std::optional<ItemT> lower_equal_bound(const KeyT &key) const
        {
            auto node = base_t::lower_equal_bound(key);
            if (node == base_t::end()) {
                return std::nullopt;
            }
            
            // NOTE: this check is to avoid sigsegv in case of data corruption
            if (node->empty())  {
                THROWF(db0::InternalException) << "Corrupted SGB_CompressedLookupTree node found at " << node.getAddress();
            }
            
            // node will be sorted if needed (only if opened as READ/WRITE)
            if (this->m_access_type == AccessType::READ_WRITE) {                
                this->onNodeLookup(node);
            }

            // within the node look up by compressed key
            if (node->header().canFit(key)) {
                auto item_ptr = node->lower_equal_bound(node->header().compress(key), this->m_heap_comp);
                assert(item_ptr);                
                // return uncompressed
                return node->header().uncompress(*item_ptr);
            } else {
                // return uncompressed
                return node->header().uncompress(*node->find_max(this->m_heap_comp));
            }
            
            return std::nullopt;
        }
           
        // Locate first element which is greater or equal to the key
        template <typename KeyT> std::optional<ItemT> upper_equal_bound(const KeyT &key) const
        {
            if (base_t::empty()) {
                return std::nullopt;
            }

            auto node = base_t::lower_equal_bound(key);
            if (node == base_t::end()) {
                // take the last node
                --node;                
            }
            
            // node will be sorted if needed (only if opened as READ/WRITE)
            if (this->m_access_type == AccessType::READ_WRITE) {                
                this->onNodeLookup(node);
            }
            // within the node look up by compressed key
            const CompressedItemT *item_ptr = nullptr;     
            if (node->header().canFit(key)) {
                item_ptr = node->upper_equal_bound(node->header().compress(key), this->m_heap_comp);
            }

            if (!item_ptr) {
                // pick first item from the next node otherwise
                ++node;                
                if (node == base_t::end()) {
                    return std::nullopt;
                }                
                item_ptr = node->find_min();
                assert(item_ptr);
            }
            
            // return uncompressed
            return node->header().uncompress(*item_ptr);
        }
        
        template <typename KeyT>
        const CompressedItemT *lower_equal_bound(const KeyT &key, sg_tree_const_iterator &node) const
        {
            node = base_t::lower_equal_bound(key);
            if (node == base_t::end()) {
                return nullptr;
            }
            
            // node will be sorted if needed (only if opened as READ/WRITE)
            if (this->m_access_type == AccessType::READ_WRITE) {
                this->onNodeLookup(node);
            }
                    
            if (node->header().canFit(key)) {
                // within the node look up by compressed key            
                return node->lower_equal_bound(node->header().compress(key), this->m_heap_comp);
            } else {
                return node->find_max(this->m_heap_comp);
            }
        }
        
        const TreeHeaderT &treeHeader() const {
            return base_t::getData()->treeHeader();
        }
        
        TreeHeaderT &modifyTreeHeader() {
            return base_t::modify().treeHeader();
        }

        /**
         * Iterate over all items, mostly useful for debugging purposes
        */
        void forAll(const std::function<void(const ItemT &)> &f) const 
        {
            for (auto node = base_t::begin(); node != base_t::end(); ++node) {
                for (auto item = node->cbegin(); item != node->cend(); ++item) {
                    f(node->header().uncompress(*item));
                }
            }
        }

        void detach() const {
            super_t::detach();
        }

        bool operator!() const {
            return super_t::operator!();
        }
        
        class uncompressed_const_iterator: protected super_t::const_iterator
        {            
        public:
            uncompressed_const_iterator(const const_iterator &iterator)
                : super_t::const_iterator(iterator)                
            {
            }
            
            bool is_end() const {
                return super_t::const_iterator::is_end();
            }

            uncompressed_const_iterator &operator++() 
            {
                super_t::const_iterator::operator++();
                return *this;
            }

            ItemT operator*() const {
                // return uncompressed item from the underlying iterator
                return this->m_tree_it->header().uncompress(super_t::const_iterator::operator*());
            }
        };
        
        // Begin sorted iteration over all items (uncompressed)
        uncompressed_const_iterator cbegin() const {
            return super_t::cbegin();
        }

    private:
        ItemCompT m_raw_item_comp;

        template <typename... Args> void insert_into(sg_tree_const_iterator &node, int recursion, const ItemT &item)
        {            
            // Split node if full or unable to fit item
            if (node->isFull() || ((node->size() > 2) && (recursion < 2) && !node->header().canFit(item))) {                
                // erase the max element and create the new node
                auto item_ptr = node.modify().find_middle(this->m_heap_comp);
                auto new_node = super_t::insert_equal(node->header().uncompress(*item_ptr), this->m_node_capacity, this->m_heap_comp);
                // rebalance the nodes around the middle item and remove the middle item from "node"
                node.modify().rebalance_at(item_ptr, new_node.modify(), this->m_heap_comp);
                // append to either of the nodes
                if (!this->m_raw_item_comp(item, new_node->keyItem())) {
                    insert_into(new_node, recursion + 1, item);
                    return;
                }                
            }
            
            if (!node->header().canFit(item)) {
                // must insert a new node to be able to fit the new item
                super_t::insert_equal(item, this->m_node_capacity, this->m_heap_comp);                
            } else {
                node.modify().append(this->m_heap_comp, item);
            }
        }

    };

}