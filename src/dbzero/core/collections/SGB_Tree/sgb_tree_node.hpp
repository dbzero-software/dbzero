// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include <dbzero/core/serialization/Base.hpp>
#include <dbzero/core/collections/CompT.hpp>
#include "sgb_tree_head.hpp"
#include <dbzero/core/utils/dary_heap.hpp>
#include <iostream>
#include <cmath>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
    /**
     * The general SGB_Tree node type
     * D - is the underlying heap D-arity (2 by default)
     * D greater than 2 helps reduce the number of full-page writes in transactions
    */
    template <
        typename ItemT, 
        typename CapacityT, 
        typename AddressT, 
        typename ItemCompT,         
        typename ItemEqualT, 
        typename HeaderT,
        int D = 2>
DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_sgb_tree_node: 
    public o_base<o_sgb_tree_node<ItemT, CapacityT, AddressT, ItemCompT, ItemEqualT, HeaderT, D>, 0, false>
    {
    protected:
        using super_t = o_base<o_sgb_tree_node, 0, false>;
        
    public:
        using capacity_t = CapacityT;
        using address_t = AddressT;
        using Initializer = ItemT;
        using iterator = ItemT *;
        using const_iterator = const ItemT *;
        using CompT = ItemCompT;
        using EqualT = ItemEqualT;
        
        // tree pointers (possibly relative to slab)
        sgb_tree_ptr_set<AddressT> ptr_set;
        // total number of available (allocated) bytes
        CapacityT m_capacity;
        // actual number of stored elements
        CapacityT m_size = 0;

        // Reverses items for the min heap
        struct HeapCompT
        {
            ItemCompT itemComp;
            ItemEqualT itemEqual;
            
            HeapCompT(const ItemCompT &itemComp = ItemCompT(), const ItemEqualT &itemEqual = ItemEqualT())
                : itemComp(itemComp)
                , itemEqual(itemEqual)
            {
            }

            template <typename LhsT, typename RhsT> bool operator()(const LhsT &lhs, const RhsT &rhs) const {
                return itemComp(rhs, lhs);
            }
        };
        
        // Notice: header stored as variable-length to allow 0-bytes type (default)
        inline HeaderT &header() {
            return this->getDynFirst(HeaderT::type());
        }

        inline const HeaderT &header() const {
            return this->getDynFirst(HeaderT::type());
        }
        
        /// Must be initialized with an item
        o_sgb_tree_node(const ItemT &item, CapacityT capacity, const HeapCompT &comp = {})
            : o_sgb_tree_node(capacity)
        {
            this->append(comp, item);
        }

        static std::size_t measure(CapacityT capacity) {
            return capacity;
        }

        static std::size_t measure(const ItemT &, CapacityT capacity, const HeapCompT & = {}) {
            return capacity;
        }

        inline ItemT &at(unsigned int index)
        {
            // items stored in the dynamic area
            assert(index < m_size);
            return begin()[index];            
        }

        inline const ItemT &at(unsigned int index) const 
        {
            // items stored in the dynamic area
            assert(index < m_size);
            return cbegin()[index];
        }

        ItemT &operator[](unsigned int index) {
            return this->at(index);
        }

        const ItemT &operator[](unsigned int index) const {
            return this->at(index);
        }
        
        /**
         * Check if there's not sufficient space to store a new item
        */
        bool isFull() const {
            return (m_capacity - this->usedCapacity()) < sizeof(ItemT);
        }

        unsigned int maxItems() const {
            return (m_capacity - sizeof(o_sgb_tree_node) - HeaderT::sizeOf()) / sizeof(ItemT);
        }

        CapacityT size() const {
            return m_size;
        }

        bool empty() const {
            return m_size == 0;
        }
        
        std::size_t sizeOf() const {
            // size of the object equals the capacity
            return m_capacity;
        }

        template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        { 
            auto buf = at;
            buf += o_sgb_tree_node::__const_ref(at).m_capacity;
            return buf - at;
        }

        template <typename... Args> iterator append(const HeapCompT &comp, Args&&... args)
        {
            assert(!isFull());
            (*this)[m_size++] = ItemT(std::forward<Args>(args)...);
            // heapify (as min heap), return pointer to the position of the item
            return dheap::push<D>(begin(), end(), comp);
        }
        
        /**
         * Erase item by key if it exists
         * 
         * @return true if item was erased
        */
        template <typename KeyT> bool erase(const KeyT &key, const HeapCompT &comp)
        {
            auto item_ptr = dheap::find<D>(begin(), begin() + m_size, key, comp.itemEqual);
            if (!item_ptr) {
                return false;
            }
            dheap::erase<D>(begin(), end(), item_ptr, comp);
            --m_size;
            return true;
        }

        bool erase_existing(unsigned int item_index, const HeapCompT &comp) {
            return this->erase_existing(itemAt(item_index), comp);
        }
                        
        inline unsigned int indexOf(const_iterator item_ptr) const
        {
            assert(item_ptr);
            assert(item_ptr >= this->cbegin() && item_ptr < this->cend());
            return item_ptr - this->cbegin();
        }

        inline const_iterator itemAt(unsigned int index) const {
            return cbegin() + index;
        }

        const ItemT &keyItem() const {
            // key item is the first heap item (max)
            return (*this)[0];
        }
        
        /**
         * Compare it this node's key is equal specific value
        */
        template <typename KeyT> bool keyEqual(const KeyT &key, const HeapCompT &comp) const {
            return !comp(key, keyItem()) && !comp(keyItem(), key);
        }

        inline const_iterator cbegin() const {
            return reinterpret_cast<const ItemT *>(this->beginOfDynamicArea() + HeaderT::sizeOf());
        }

        iterator begin() const {
            return const_cast<ItemT*>(cbegin());
        }

        const_iterator cend() const {
            return cbegin() + m_size;
        }
        
        iterator end() {
            return const_cast<ItemT*>(cend());
        }

        inline const_iterator clast() const {
            return cbegin() + maxItems();
        }

        iterator last() const {
            return const_cast<ItemT*>(clast());
        }

        /**
         * const_sorting_iterator uses additional memory to sort items on-the-fly
         * from the heap order to the sorted order
        */
        class const_sorting_iterator
        {    
        public:
            // as null / invalid
            const_sorting_iterator() = default;
            const_sorting_iterator(const ItemT *ptr, const ItemT *end_ptr, const HeapCompT &comp)
                : m_items(ptr, end_ptr)
                , m_ptr(m_items.data())
                , m_end_ptr(m_items.data() + m_items.size())
                , m_comp(comp)
            {
                assert(ptr <= end_ptr);
            }
            
            const_sorting_iterator(const const_sorting_iterator &other)
                : m_items(other.m_items)
                // rebase items
                , m_ptr(m_items.data() + (other.m_ptr - other.m_items.data()))
                , m_end_ptr(m_items.data() + (other.m_end_ptr - other.m_items.data()))
                , m_comp(other.m_comp)
            {
            }
            
            const_sorting_iterator(const_sorting_iterator &&other) {
                (*this) = std::move(other);
            }

            const_sorting_iterator(std::vector<ItemT> &&items, const HeapCompT &comp)
                : m_items(std::move(items))
                , m_ptr(m_items.data())
                , m_end_ptr(m_items.data() + m_items.size())
                , m_comp(comp)
            {                
            }
            
            const_sorting_iterator &operator++()
            {
                assert(!is_end());
                dheap::pop<D>(m_ptr, m_end_ptr, m_comp);
                --m_end_ptr;
                return *this;
            }

            // Check if the instance is valid
            bool operator!() const {
                return !m_ptr || !m_end_ptr;
            }

            bool is_end() const {
                return m_ptr == m_end_ptr;
            }

            ItemT operator*() const {
                return *m_ptr;
            }

            const ItemT *get_ptr() const {
                return m_ptr;
            }

            const ItemT *operator->() const {
                return m_ptr;
            }

            const_sorting_iterator &operator=(const const_sorting_iterator &other)
            {
                if (this != &other) {
                    m_items = other.m_items;
                    // rebase items
                    m_ptr = m_items.data() + (other.m_ptr - other.m_items.data());
                    m_end_ptr = m_items.data() + (other.m_end_ptr - other.m_items.data());
                    m_comp = other.m_comp;
                }
                return *this;
            }

            const_sorting_iterator &operator=(const_sorting_iterator &&other)
            {
                if (this != &other) {
                    auto ptr_diff = other.m_ptr - other.m_items.data();
                    auto end_ptr_diff = other.m_end_ptr - other.m_items.data();
                    m_items = std::move(other.m_items);
                    // rebase items
                    m_ptr = m_items.data() + ptr_diff;
                    m_end_ptr = m_items.data() + end_ptr_diff;
                    m_comp = other.m_comp;
                }
                return *this;
            }
            
        private:
            std::vector<ItemT> m_items;
            ItemT *m_ptr = nullptr;
            ItemT *m_end_ptr = nullptr;
            HeapCompT m_comp;
        };
        
        const_sorting_iterator cbegin_sorted(const HeapCompT &comp) const {
            return { cbegin(), cend(), comp };
        }

        const_iterator find_max(const HeapCompT &comp) const {
            // min item because we use the inverted comparator
            return dheap::find_min<D>(cbegin(), cend(), comp);
        }
                
        const_iterator find_min() const {
            // first item is the min item
            return cbegin();
        }

        /**
         * Remove specific element from this node and replace it with 'item'
         * 
         * @param item item to be replaced
         * @return pair of newly inserted item (first) and removed item (second)
        */
        std::pair<iterator, ItemT> replace(iterator item, const ItemT &new_item, const HeapCompT &comp)
        {
            auto result = *item;
            *item = new_item;
            return { dheap::push<D>(begin(), item + 1, comp), result };
        }

        template <typename KeyT> const_iterator lower_equal_bound(const KeyT &key, const HeapCompT &comp) const
        {
            const_iterator result = nullptr;
            // must iterate over all items
            for (auto it = cbegin(), _end = cend(); it != _end; ++it) {
                if (!comp(*it, key)) {
                    if (!result || comp(*it, *result)) {
                        result = it;
                    }
                }
            }
            assert(!result || (result >= cbegin() && result < cend()));
            return result;
        }

        template <typename KeyT> const_iterator find_equal(const KeyT &key, const HeapCompT &comp) const {
            return dheap::find<D>(cbegin(), cbegin() + m_size, key, comp.itemEqual);
        }

        template <typename KeyT> const_iterator upper_equal_bound(const KeyT &key, const HeapCompT &comp) const
        {
            const_iterator result = nullptr;
            // must iterate over all items, some minor optimizations possible - e.g. starting from min in ascending order
            for (auto it = cbegin(), _end = cend(); it != _end; ++it) {
                if (!comp(key, *it)) {
                    if (!result || comp(*result, *it)) {
                        result = it;
                    }                    
                }
            }
            assert(!result || (result >= cbegin() && result < cend()));
            return result;
        }
        
        /**
         * Try finding the lower-equal element and the surrounding (prev/next) ones
        */
        template <typename KeyT> const_iterator lower_equal_window(const KeyT &key, const_iterator &prev, 
            const_iterator &next, const HeapCompT &comp) const
        {
            const_iterator result = nullptr;
            prev = next = nullptr;
            // must iterate over all items
            for (auto it = cbegin(); it != cend(); ++it) {
                if (comp(*it, key)) {
                    if (!next || comp(*next, *it)) {
                        next = it;
                    }
                } else {
                    if (!result || comp(*it, *result)) {
                        prev = result;
                        result = it;
                    } else if (!prev || comp(*it, *prev)) {
                        prev = it;
                    }
                }
            }
            return result;
        }

        /**
         * Rebalance the 2 nodes so that they hold roughly the same number of items
         * the 'other' node should have less items than 'this' node          
        */
        void rebalance(o_sgb_tree_node &other, const HeapCompT &comp)
        {
            if (size() == other.size()) {
                // already balanced
                return;
            }
            if (other.size() > size()) {
                other.rebalance(*this, comp);
                return;
            }
            
            assert(size() > other.size());
            assert(size() > 0 );
            assert(other.size() > 0);
            // pick the split point assuming that elements are already approximately sorted (heap sorted)
            int iter_max = 2;
            while (iter_max-- > 0 && size() > other.size()) {
                auto split_item = cbegin()[(size() + other.size()) / 2];
                auto it = begin(), end_ = end();
                while (it != end_) {
                    if (comp.itemComp(*it, split_item)) {
                        ++it;
                    } else {
                        if (size() > 1) {
                            other.append(comp, *it);
                            this->erase_existing(it, comp);
                            // must not be empty after removing a single item
                            assert(!this->empty());
                        }
                        --end_;
                    }
                }
                if (std::fabs((double)size() - (double)other.size()) / ((double)size() + (double)other.size()) < 0.25) {
                    break;
                }
            }
        }

        /**
         * Calculate size for the given number of items
        */
        static std::size_t measureSizeOf(unsigned int item_count) {
            return sizeof(o_sgb_tree_node) + HeaderT::sizeOf() + item_count * sizeof(ItemT);
        }

    protected:

        o_sgb_tree_node(CapacityT capacity)
            : m_capacity(capacity)
        {
            // initialize header with default arguments
            this->arrangeMembers()
                (HeaderT::type());
        }

    private:

        /**
         * Erase existing item, return true if the node is empty after the operation
         * 
         * @return true if node is empty after the operation
        */
        bool erase_existing(const_iterator item_ptr, const HeapCompT &comp)
        {
            assert(item_ptr);
            assert(item_ptr >= this->cbegin() && item_ptr < this->cend());
            dheap::erase<D>(begin(), end(), const_cast<iterator>(item_ptr), comp);
            --m_size;
            return m_size == 0;
        }
        
        std::size_t usedCapacity() const {
            return sizeof(o_sgb_tree_node) + HeaderT::sizeOf() + m_size * sizeof(ItemT);
        }
    };
DB0_PACKED_END

}