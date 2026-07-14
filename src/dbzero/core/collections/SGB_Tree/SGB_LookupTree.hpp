// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "SGB_Tree.hpp"
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/utils/dary_heap.hpp>
#include <dbzero/core/utils/bisect.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <iostream>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
    enum class LookupHeaderFlags : std::uint16_t {
        sorted = 0x0001,    
        reversed = 0x0002
    };

    /**
     * Stores a per-node header details
    */
DB0_PACKED_BEGIN
    template <typename HeaderT> class DB0_PACKED_ATTR o_lookup_header: public o_fixed_ext<o_lookup_header<HeaderT>, HeaderT>
    {
    public:
        // counter of lookups since the last update
        std::uint16_t m_lookup_count = 0;
        FlagSet<LookupHeaderFlags> m_flags;
        
        // 0, 1 element set can be marked as sorted
        o_lookup_header()
            : m_flags { LookupHeaderFlags::sorted }
        {            
        }

        inline void reset() {
            m_lookup_count = 0;
            m_flags.set(LookupHeaderFlags::sorted, false);
        }
    };
DB0_PACKED_END

    template <
        typename ItemT, 
        typename CapacityT, 
        typename AddressT, 
        typename ItemCompT,         
        typename ItemEqualT, 
        typename HeaderT,
        int D = 2>
DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_sgb_lookup_tree_node:
    public o_ext<
        o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>,
        o_sgb_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, o_lookup_header<HeaderT>, D>, 0, false>
    {
    protected:
        using ext_t = o_ext<
            o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>,
            o_sgb_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, o_lookup_header<HeaderT>, D>, 0, false>;
        using super_t = o_sgb_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, o_lookup_header<HeaderT>, D>;

    public: 
        using iterator = typename super_t::iterator;
        using const_iterator = typename super_t::const_iterator;
        using HeapCompT = typename super_t::HeapCompT;
        
        o_sgb_lookup_tree_node(CapacityT capacity)
            : ext_t(capacity)
        {            
        }
        
        o_sgb_lookup_tree_node(const ItemT &item, CapacityT capacity, const HeapCompT &comp)
            : ext_t(item, capacity, comp)
        {
        }

        const_iterator cbegin() const
        {
            if (is_reversed()) {
                // reversed begin
                return super_t::cbegin() + this->maxItems() - 1;
            }
            return super_t::cbegin();
        }

        iterator begin() const {
            return const_cast<ItemT*>(this->cbegin());
        }

        const_iterator cend() const
        {
            if (is_reversed()) {
                return this->cbegin() - this->m_size;
            }
            return super_t::cend();            
        }
        
        iterator end() {
            return const_cast<ItemT*>(this->cend());
        }

        const_iterator clast() const
        {
            if (is_reversed()) {
                // reversed last
                return super_t::cbegin() - 1;
            } 
            return super_t::clast();            
        }
        
        iterator last() {
            return const_cast<ItemT*>(this->clast());
        }

        const ItemT &keyItem() const {
            // key item is the first heap item (or first sorted item)
            return *this->cbegin();
        }

        inline bool is_reversed() const {
            return this->header().m_flags[LookupHeaderFlags::reversed];
        }

        inline bool is_sorted() const {
            return this->header().m_flags[LookupHeaderFlags::sorted];
        }

        template <typename... Args> iterator append(const HeapCompT &comp, Args&&... args)
        {
            assert(!this->isFull());
            *(this->end()) = ItemT(std::forward<Args>(args)...);
            ++this->m_size;
            // heapify (as min heap), return pointer to the position of the item
            iterator result;
            if (this->is_reversed()) {
                result = dheap::rpush<D>(this->begin(), this->end(), comp);
            } else {
                result = dheap::push<D>(this->begin(), this->end(), comp);
            }
            // reset the lookup counter and sorted flag
            this->header().reset();
            return result;
        }

        /**
         * Erase item by key if it exists
         * 
         * @return true if item was erased
        */
        template <typename KeyT> 
        bool erase(const KeyT &key, const HeapCompT &comp)
        {            
            if (this->is_reversed()) {
                auto item_ptr = dheap::rfind<D>(this->begin(), this->end(), key, comp.itemEqual);
                if (!item_ptr) {
                    return false;
                }
                dheap::rerase<D>(this->begin(), this->end(), item_ptr, comp);
            } else {
                auto item_ptr = dheap::find<D>(this->begin(), this->end(), key, comp.itemEqual);
                if (!item_ptr) {
                    return false;
                }
                dheap::erase<D>(this->begin(), this->end(), item_ptr, comp);
            }
            --this->m_size;
            // reset the lookup counter and sorted flag
            this->header().reset();
            return true;
        }

        inline int step() const {
            return this->header().m_flags[LookupHeaderFlags::reversed] ? -1 : 1;
        }
        
        template <typename KeyT, typename CompT = HeapCompT>
        const_iterator lower_equal_bound(const KeyT &key, CompT comp) const
        {
            const_iterator result = nullptr;
            if (is_sorted()) {
                // if sorted, use binary search
                const_iterator it, end_ = this->cend();
                if (is_reversed()) {
                    it = bisect::rlower_equal(this->cbegin(), end_, key, comp.itemComp);
                } else {
                    it = bisect::lower_equal(this->cbegin(), end_, key, comp.itemComp);
                }
                if (it != end_) {
                    result = it;
                }
            } else {
                // must scan all items otherwise
                auto step_ = this->step();
                for (auto it = this->cbegin(); it != this->cend(); it += step_) {
                    if (!comp(*it, key)) {
                        if (!result || comp(*it, *result)) {
                            result = it;
                        }
                    }
                }
            }
            return result;
        }
        
        template <typename KeyT> 
        const_iterator upper_equal_bound(const KeyT &key, const HeapCompT &comp) const
        {
            const_iterator result = nullptr;
            if (is_sorted()) {
                // if sorted, use binary search
                const_iterator it, end_ = this->cend();
                if (is_reversed()) {
                    it = bisect::rupper_equal(this->cbegin(), end_, key, comp.itemComp);
                } else {
                    it = bisect::upper_equal(this->cbegin(), end_, key, comp.itemComp);
                }
                if (it != end_) {
                    result = it;
                }
            } else {
                // must scan all items otherwise
                auto step_ = this->step();
                for (auto it = this->cbegin(); it != this->cend(); it += step_) {
                    if (!comp(key, *it)) {
                        if (!result || comp(*result, *it)) {
                            result = it;
                        }
                    }
                }
            }
            return result;
        }

        void flipSort(const HeapCompT &comp) 
        {
            if (is_sorted()) {
                // already sorted
                return;
            }
            if (is_reversed()) {
                dheap::rsort<D>(this->begin(), this->end(), this->last(), comp);
            } else {
                dheap::sort<D>(this->begin(), this->end(), this->last(), comp);
            }
            // flip the reversed flag
            this->header().m_flags.set(LookupHeaderFlags::reversed, !is_reversed());
            this->header().m_flags.set(LookupHeaderFlags::sorted, true);
        }
        
        void rebalance(o_sgb_lookup_tree_node &other, const HeapCompT &comp)
        {
            if (this->size() == other.size()) {
                // already balanced
                return;
            }
            if (other.size() > this->size()) {
                other.rebalance(*this, comp);
                return;
            }
            
            if (!is_sorted()) {
                // sort the heap for better balancing
                this->flipSort(comp);
            }
            // pick the split point assuming that elements are already approximately sorted (heap sorted)
            int max_repeat = 2;
            auto step_ = this->step();
            while (max_repeat-- > 0 && this->size() > other.size()) {
                auto split_item = *(this->cbegin() + ((this->size() + other.size()) >> 1) * step_);
                auto it = this->begin(), end_ = this->end();
                while (it != end_) {
                    if (comp.itemComp(*it, split_item)) {
                        it += step_;
                    } else {
                        // prevents from leaving an empty node
                        if (this->size() > 1) {
                            other.append(comp, *it);
                            this->erase_existing(it, comp);
                        }
                        end_ -= step_;
                    }
                }
                // check if sufficiently balanced
                if (std::fabs((double)this->size() - (double)other.size()) / ((double)this->size() + (double)other.size()) < 0.25) {
                    break;
                }
            }
        }
        
        const_iterator find_middle(const HeapCompT &comp)
        {
            if (!is_sorted()) {
                this->flipSort(comp);
            }
            return this->begin() + (this->size() >> 1) * this->step();
        }
        
        const_iterator find_min() const {
            // First item is always minimum, either sorted or heap-sorted
            return this->cbegin();
        }
        
        const_iterator find_max(const HeapCompT &comp) const
        {
            if (is_sorted()) {
                return this->cend() - this->step();
            }
            return super_t::find_max(comp);
        }
        
        /**
         * Erase existing item, return true if the node is empty after the operation         
         * @return true if node is empty after the operation
        */
        bool erase_existing(unsigned int at, const HeapCompT &comp) {
            return this->erase_existing(this->itemAt(at), comp);
        }
        
        class const_sorting_iterator
        {    
        public:
            const_sorting_iterator() = default;
            const_sorting_iterator(const ItemT *ptr, const ItemT *end_ptr, const HeapCompT &comp,
                bool is_sorted, bool is_reversed)
                : m_ptr(is_sorted ? ptr : nullptr)
                , m_end_ptr(is_sorted ? end_ptr : nullptr)
                , m_is_sorted(is_sorted)
                , m_is_reversed(is_reversed)
                , m_step(is_reversed ? -1 : 1)
            {
                if (!is_sorted) {
                    if (is_reversed) {
                        // NOTE: pointers are reversed as well
                        assert(!(ptr <= end_ptr));
                        // copy items in reversed heap order
                        std::vector<ItemT> items;
                        items.reserve(std::distance(end_ptr, ptr));
                        while (ptr != end_ptr) {
                            items.push_back(*ptr);
                            --ptr;
                        }
                        m_it = { std::move(items), comp };
                    } else {
                        m_it = { ptr, end_ptr, comp };
                    }
                }
            }

            const_sorting_iterator &operator++()
            {
                assert(!is_end());
                if (m_is_sorted) {
                    m_ptr += m_step;                    
                } else {
                    assert(!!m_it);
                    ++m_it;
                }
                return *this;
            }

            // Check if the instance is valid
            bool operator!() const {
                return !(m_ptr || !!m_it);
            }
            
            bool is_end() const 
            {
                if (m_is_sorted) {
                    return m_ptr == m_end_ptr;
                } else {
                    return m_it.is_end();
                }
            }

            ItemT operator*() const
            {
                assert(!is_end());
                if (m_is_sorted) {
                    return *m_ptr;
                } else {
                    return *m_it;
                }            
            }

        private:
            typename super_t::const_sorting_iterator m_it;
            const ItemT *m_ptr = nullptr;
            const ItemT *m_end_ptr = nullptr;
            bool m_is_sorted = false;
            bool m_is_reversed = false;
            int m_step;
        };

        const_sorting_iterator cbegin_sorted(const HeapCompT &comp) const {
            return { this->cbegin(), this->cend(), comp, is_sorted(), is_reversed() };
        }

    private:

        /**
         * Erase existing item, return true if the node is empty after the operation         
         * @return true if node is empty after the operation
        */
        bool erase_existing(const_iterator item_ptr, const HeapCompT &comp)
        {
            if (is_reversed()) {
                dheap::rerase<D>(this->begin(), this->end(), const_cast<iterator>(item_ptr), comp);
            } else {
                dheap::erase<D>(this->begin(), this->end(), const_cast<iterator>(item_ptr), comp);
            }
            --this->m_size;
            this->header().reset();
            return this->m_size == 0;
        }
    };
DB0_PACKED_END

    template <
        typename ItemType, 
        typename ItemCompType, 
        typename ItemEqualType,
        typename CapacityType = std::uint16_t, 
        typename AddressType = std::uint64_t,
        typename HeaderType = db0::o_null,
        typename TreeHeaderType = db0::o_null>
    class sgb_lookup_types
    {
    public :
        using ItemT = ItemType;
        using ItemCompT = ItemCompType;
        using ItemEqualT = ItemEqualType;
        using CapacityT = CapacityType;
        using AddressT = AddressType;
        using HeaderT = HeaderType;
        using TreeHeaderT = TreeHeaderType;
        using o_sgb_node_t = o_sgb_lookup_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT>;
        using node_traits = sgb_node_traits<o_sgb_node_t, ItemT, ItemCompT>;
        using ptr_set_t = sgb_tree_ptr_set<AddressT>;
        using NodeT = SGB_IntrusiveNode<o_sgb_node_t, ItemT, ItemCompT, typename node_traits::comp_t, TreeHeaderT>;
        using CompT = typename NodeT::comp_t;
        using NodeItemCompT = typename o_sgb_node_t::CompT;
        using NodeItemEqualT = typename o_sgb_node_t::EqualT;
        using HeapCompT = typename o_sgb_node_t::HeapCompT;
                
        using SG_TreeT = v_sgtree<NodeT, intrusive::detail::h_alpha_sqrt2_t>;
    };

    /**
     * SGB_Tree extension which improves on lookup performance by selectively sorting nodes and using bisect search
    */
    template <typename TypesT> class SGB_LookupTreeBase: protected SGB_TreeBase<TypesT>
    {
    protected:
        using super_t = SGB_TreeBase<TypesT>;
        using base_t = typename super_t::super_t;

    public:
        using ItemT = typename TypesT::ItemT;
        using CompT = typename TypesT::CompT;
        using CapacityT = typename TypesT::CapacityT;
        using AddressT = typename TypesT::AddressT;
        using sg_tree_const_iterator = typename super_t::sg_tree_const_iterator;
        using ItemIterator = typename super_t::ItemIterator;
        using ConstItemIterator = typename super_t::ConstItemIterator;
        using NodeItemCompT = typename TypesT::NodeItemCompT;
        using NodeItemEqualT = typename TypesT::NodeItemEqualT;
        using HeapCompT = typename TypesT::HeapCompT;
        static constexpr unsigned int DEFAULT_SORT_THRESHOLD = 3;

        // as null / invalid
        SGB_LookupTreeBase() = default;

        SGB_LookupTreeBase(Memspace &memspace, std::size_t node_capacity, 
            AccessType access_type, const CompT &comp = {}, const NodeItemCompT item_cmp = {}, const NodeItemEqualT item_eq = {},
            unsigned int sort_thr = DEFAULT_SORT_THRESHOLD)
            : super_t(memspace, node_capacity, comp, item_cmp, item_eq)
            , m_sort_threshold(sort_thr)
            , m_access_type(access_type)
        {
        }
        
        SGB_LookupTreeBase(mptr ptr, std::size_t node_capacity,
            AccessType access_type, const CompT &comp = {}, const NodeItemCompT item_cmp = {}, const NodeItemEqualT item_eq = {},
            unsigned int sort_thr = DEFAULT_SORT_THRESHOLD)
            : super_t(ptr, node_capacity, comp, item_cmp, item_eq)
            , m_sort_threshold(sort_thr)
            , m_access_type(access_type)
        {
        }

        void insert(const ItemT &item)
        {
            assert(m_access_type == AccessType::READ_WRITE);
            this->emplace(item);
        }

        template <typename... Args> ItemIterator emplace(Args&&... args) 
        {
            assert(m_access_type == AccessType::READ_WRITE);
            return super_t::emplace(std::forward<Args>(args)...);
        }
        
        template <typename KeyT> ConstItemIterator lower_equal_bound(const KeyT &key) const 
        {
            auto node = base_t::lower_equal_bound(key);
            if (node == super_t::end()) {
                return { nullptr, sg_tree_const_iterator() };
            }

            // node will be sorted if needed (only if in READ/WRITE mode)
            if (m_access_type == AccessType::READ_WRITE) {                
                this->onNodeLookup(node);
            }
            return { node->lower_equal_bound(key, this->m_heap_comp), node };
        }
        
        AddressT getAddress() const {
            return base_t::getAddress();
        }

        sg_tree_const_iterator cbegin_nodes() const {
            return super_t::cbegin_nodes();
        }
        
        sg_tree_const_iterator cend_nodes() const {
            return super_t::cend_nodes();
        }
        
    protected:
        const unsigned int m_sort_threshold = 0;
        const AccessType m_access_type = {};
        
        void onNodeLookup(sg_tree_const_iterator &node) const
        {
            assert(m_access_type == AccessType::READ_WRITE);
            if (node->is_sorted()) {
                // already sorted
                return;
            }
            if (++node.modify().header().m_lookup_count < m_sort_threshold) {
                // not enough lookups
                return;
            }
            node.modify().flipSort(this->m_heap_comp);
        }    
    };
    
    template <
        typename ItemT, 
        typename ItemCompT = std::less<ItemT> ,
        typename ItemEqualT = std::equal_to<ItemT> ,
        typename CapacityT = std::uint16_t,
        typename AddressT = std::uint64_t,
        typename HeaderT = o_null,
        typename TreeHeaderT = o_null>
    class SGB_LookupTree: 
    public SGB_LookupTreeBase<sgb_lookup_types<ItemT, ItemCompT, ItemEqualT, CapacityT, AddressT, HeaderT, TreeHeaderT> >
    {   
    protected:
        using super_t = SGB_LookupTreeBase<sgb_lookup_types<ItemT, ItemCompT, ItemEqualT, CapacityT, AddressT, HeaderT, TreeHeaderT> >;
    public:
        using CompT = typename super_t::CompT;

        SGB_LookupTree(Memspace &memspace, std::size_t node_capacity, AccessType access_type, 
            const CompT &comp = {}, const ItemCompT &item_cmp = {}, const ItemEqualT &item_eq = {}, 
            unsigned int sort_thr = super_t::DEFAULT_SORT_THRESHOLD)
            : super_t(memspace, node_capacity, access_type, comp, item_cmp, item_eq, sort_thr)
        {
        }
        
        SGB_LookupTree(mptr ptr, std::size_t node_capacity, AccessType access_type, 
            const CompT &comp = {}, const ItemCompT &item_cmp = {}, const ItemEqualT &item_eq = {}, 
            unsigned int sort_thr = super_t::DEFAULT_SORT_THRESHOLD)
            : super_t(ptr, node_capacity, access_type, comp, item_cmp, item_eq, sort_thr)
        {
        }
    };
    
}