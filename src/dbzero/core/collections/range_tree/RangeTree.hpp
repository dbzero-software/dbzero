// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <algorithm>
#include <optional>
#include "RangeTreeBlock.hpp"
#include "RT_NullBlock.hpp"
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
DB0_PACKED_BEGIN
    template <typename KeyT, typename ValueT> struct DB0_PACKED_ATTR RT_ItemT
    {
        using BlockT = RangeTreeBlock<KeyT, ValueT>;
        using ItemT = typename BlockT::ItemT;
        // the first item (key + value) in the range
        ItemT m_first_item;
        // pointer to block instance
        typename BlockT::PtrT m_block_ptr;

        bool operator<(const RT_ItemT &other) const
        {
            if (m_first_item.m_key == other.m_first_item.m_key) {
                return m_first_item.m_value < other.m_first_item.m_value;
            }
            return m_first_item.m_key < other.m_first_item.m_key;
        }

        struct CompT
        {
            bool operator()(const RT_ItemT &a, const RT_ItemT &b) const
            {
                return a < b;
            }

            bool operator()(const ItemT &a, const ItemT &b) const
            {
                if (a.m_key == b.m_key) {
                    return a.m_value < b.m_value;
                }
                return a.m_key < b.m_key;
            }
        };
        
    };
DB0_PACKED_END
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_range_tree: public o_fixed_versioned<o_range_tree>
    {
        std::uint32_t m_max_block_size;
        // address of the underlying v_bindex
        Address m_rt_index_addr = {};
        // pointer to a single null-keys block instance
        Address m_rt_null_block_addr = {};
        // the total number of elements in the tree
        std::uint64_t m_size = 0;

        o_range_tree(std::uint32_t max_block_size)
            : m_max_block_size(max_block_size)
        {
        }

        o_range_tree(const o_range_tree &other)
            : m_max_block_size(other.m_max_block_size)
            , m_size(other.m_size)
        {
        }
    };
DB0_PACKED_END
    
    /**
     * @tparam KeyT the fixed size ordinal type (e.g. numeric), keys don't need to be unique
     * @tparam ValueT the value type, elements inside blocks are sorted by this value
    */
    template <typename KeyT, typename ValueT> class RangeTree: public v_object<o_range_tree>
    {
        using super_t = v_object<o_range_tree>;
        using RT_Item = RT_ItemT<KeyT, ValueT>;
        using RT_Index = v_bindex<RT_Item>;
    public:
        using BlockT = RangeTreeBlock<KeyT, ValueT>;
        using NullBlockT = RT_NullBlock<ValueT>;
        using ItemT = typename BlockT::ItemT;
        using iterator = typename RT_Index::iterator;
        using const_iterator = typename RT_Index::const_iterator;
        using CallbackT = std::function<void(ValueT)>;
        
        RangeTree(Memspace &memspace, std::uint32_t max_block_size = 256 * 1024)
            : super_t(memspace, max_block_size)
            , m_index(memspace)            
        {
            modify().m_rt_index_addr = m_index.getAddress();
        }

        RangeTree(mptr ptr)
            : super_t(ptr)
            , m_index(this->myPtr((*this)->m_rt_index_addr))            
        {
        }
        
        RangeTree(Memspace &memspace, const RangeTree &other)
            : super_t(memspace, *other.getData())
            , m_index(memspace, other.m_index)
        {
            modify().m_rt_index_addr = m_index.getAddress();
            if (other->m_rt_null_block_addr.isValid()) {
                NullBlockT null_block(other.myPtr(other->m_rt_null_block_addr));
                NullBlockT new_null_block(memspace, null_block);
                this->modify().m_rt_null_block_addr = new_null_block.getAddress();
            }
        }
        
        /**
         * Insert 1 or more elements in a single bulk operation
         * @tparam IteratorT random access iterator to items
         * @param add_callback_ptr optional callback to be called for each new added element
        */
        template <typename IteratorT> void bulkInsert(IteratorT begin, IteratorT end,
            CallbackT *add_callback_ptr = nullptr)
        {
            using CompT = typename ItemT::HeapCompT;
            // heapify the elements (min heap)
            std::make_heap(begin, end, CompT());
            while (begin != end) {
                auto range = getRange(*begin);
                
                for (;;) {
                    auto _end = end;
                    // calculate the remaining capacity in the block
                    auto block_capacity = 0;
                    if (range->size() < (*this)->m_max_block_size) {
                        block_capacity = (*this)->m_max_block_size - range->size();
                    }

                    // split bounded range if full
                    if (!range.isUnbound() && block_capacity == 0) {
                        range = splitRange(std::move(range));
                        continue;
                    }
                    
                    while (block_capacity > 0 && begin != end && range.canInsert(*begin)) {
                        std::pop_heap(begin, end, CompT());
                        --end;
                        --block_capacity;
                    }
                    if (end != _end) {
                        auto diff = range.bulkInsert(end, _end, add_callback_ptr);
                        if (diff > 0) {
                            this->modify().m_size += diff;
                        }
                    }
                    if (!range.isUnbound() || begin == end) {
                        break;
                    }
                    // in case of unbound ranges (i.e. the last range) append a new one and continue
                    range = insertRange(*begin);
                }
            }
        }
        
        /**
         * Erase 1 or more elements in a single bulk operation
         * @tparam IteratorT random access iterator to items
         * @param erase_callback_ptr optional callback to be called for each erased element
        */
        template <typename IteratorT> void bulkErase(IteratorT begin, IteratorT end,
            CallbackT *erase_callback_ptr = nullptr)
        {
            using CompT = typename ItemT::HeapCompT;
            if (m_index.empty()) {
                // nothing to erase
                return;
            }

            // heapify the elements (min heap by key)
            std::make_heap(begin, end, CompT());
            while (begin != end) {
                auto range = getExistingRange(*begin);
                auto _end = end;
                while (begin != end && range.inUpperBound(*begin)) {
                    std::pop_heap(begin, end, CompT());
                    --end;
                    if (!range.inLowerBound(*begin)) {
                        // ignore the element out of bounds
                        *(end + 1) = *_end;
                        --_end;
                    }
                }
                
                // erase from current range and continue with the next one
                if (end != _end) {
                    auto diff = range.bulkErase(end, _end, erase_callback_ptr);
                    if (diff > 0) {
                        this->modify().m_size -= diff;
                    }
                    if (range.empty()) {
                        // remove the empty range
                        range.erase();
                    }
                }
            }
        }

        /**
         * Insert 1 or more null elements (null key)
         * @param add_callback_ptr optional callback to be called for each new added element
        */
        template <typename IteratorT> void bulkInsertNull(IteratorT begin, IteratorT end,
            CallbackT *add_callback_ptr = nullptr)
        {
            // create the null block at the first null insert
            if (!(*this)->m_rt_null_block_addr.isValid()) {
                NullBlockT null_block(this->getMemspace());
                this->modify().m_rt_null_block_addr = null_block.getAddress();
            }

            auto null_block_ptr = getNullBlock();
            assert(null_block_ptr);

            // insert values into the null block directly
            auto diff = null_block_ptr->bulkInsertUnique(begin, end, add_callback_ptr).first;
            if (diff > 0) {
                this->modify().m_size += diff;
            }
        }

        template <typename IteratorT> void bulkEraseNull(IteratorT begin, IteratorT end,
            CallbackT *erase_callback_ptr = nullptr)
        {
            // exist if null block doesn't exist
            if (!(*this)->m_rt_null_block_addr.isValid()) {
                return;
            }

            auto null_block_ptr = getNullBlock();
            assert(null_block_ptr);
            
            // erase values from the null block directly
            auto diff = null_block_ptr->bulkErase(
                begin, end, static_cast<const ValueT*>(nullptr),  erase_callback_ptr
            );
            if (diff > 0) {
                this->modify().m_size -= diff;
            }
        }

        class RangeIterator
        {
        public:
            // default / empty iterator instance
            RangeIterator(bool asc)
                : m_asc(asc)
            {
            }

            RangeIterator(RT_Index &index, const iterator &it, const iterator &begin,
                const iterator &end, bool is_first, bool asc)
                : m_index_ptr(&index)
                , m_it(it)
                , m_next_it(it)
                , m_begin(begin)
                , m_end(end)
                , m_is_first(is_first)
                , m_asc(asc)
            {
                next(m_next_it);
                if (m_it != m_end) {
                    m_bounds.first = (*m_it).m_first_item;
                }
                if (m_next_it != m_end) {
                    m_bounds.second = (*m_next_it).m_first_item;
                }
            }

            inline bool inLowerBound(ItemT item) const
            {
                assert(m_asc);
                return !m_bounds.first || !item.ltByKey(*m_bounds.first);
            }

            inline bool inUpperBound(ItemT item) const
            {
                assert(m_asc);
                return !m_bounds.second || (item.ltByKey(*m_bounds.second));
            }

            bool inBounds(ItemT item) const {
                // the second condition is to allow multiple range with identical elements
                return inLowerBound(item) && inUpperBound(item);
            }

            bool canInsert(ItemT item) const
            {
                assert(m_asc);
                return (m_is_first || !m_bounds.first || !(*m_bounds.first).gtByKey(item)) && (!m_bounds.second || (*m_bounds.second).gtByKey(item));
            }
            
            std::pair<std::optional<KeyT>, std::optional<KeyT> > getKeyRange() const 
            {
                return {
                    m_bounds.first ? std::optional<KeyT>((*m_bounds.first).m_key) : std::nullopt,
                    m_bounds.second ? std::optional<KeyT>((*m_bounds.second).m_key) : std::nullopt
                };
            }

            BlockT &operator*()
            {
                if (!m_block) {
                    m_block = std::make_unique<BlockT>((*m_it).m_block_ptr(m_index_ptr->getMemspace()));
                }
                return *m_block;
            }

            BlockT *operator->() {
                return &**this;
            }
            
            bool isUnbound() const
            {
                assert(m_asc);
                return m_next_it == m_end;
            }
            
            void operator=(RangeIterator &&other)
            {                
                assert(m_index_ptr == other.m_index_ptr);
                assert(m_begin == other.m_begin);
                assert(m_end == other.m_end);
                assert(m_asc == other.m_asc);
                m_it = other.m_it;
                m_next_it = other.m_next_it;
                m_block = std::move(other.m_block);
                m_bounds = other.m_bounds;
            }
            
            void next()
            {
                m_it = m_next_it;
                if (m_it == m_end) {
                    m_block.reset();
                    m_bounds = {};
                    return;
                }

                m_is_first = false;
                m_bounds.first = (*m_it).m_first_item;
                next(m_next_it);
                if (m_next_it == m_end) {
                    m_bounds.second = {};
                } else {
                    m_bounds.second = (*m_next_it).m_first_item;
                }
                m_block.reset();
            }

            bool isEnd() const {
                return m_it == m_end;
            }

            std::pair<std::optional<ItemT>, std::optional<ItemT>> getBounds() const {
                return m_bounds;
            }

            // split current block
            BlockT split()
            {
                // collect block items into memory
                std::vector<ItemT> items;
                for (auto it = m_block->begin(), end = m_block->end(); it != end; ++it) {
                    items.push_back(*it);
                }
                if (items.empty()) {
                    return {};
                }

                // make heap by-key
                typename ItemT::HeapCompT comp;
                std::make_heap(items.begin(), items.end(), comp);
                // select middle element as the split point
                auto split_pt = *(items.begin() + items.size() / 2);
                // erase items < split_pt from the original block
                std::function<bool(ItemT)> selector = [comp, split_pt](ItemT item) {
                    return comp(split_pt, item);
                };

                (*this)->bulkErase(selector);
                auto it = items.begin(), end = items.end();
                while (it != end) {
                    if (!comp(split_pt, *it)) {
                        --end;
                        std::swap(*it, *end);
                    } else {
                        ++it;
                    }                    
                }

                // create new block & populate with remaining items
                BlockT new_block(m_index_ptr->getMemspace());
                new_block.bulkInsert(items.begin(), end);
                return new_block;
            }

            /**
             * @return number of elements added
             * @param add_callback_ptr optional callback to be called for each added element
            */
            template <typename iterator_t> std::size_t bulkInsert(iterator_t begin_item, iterator_t end_item,
                CallbackT *add_callback_ptr = nullptr)
            {
                if (m_is_first) {
                    // check if the first item needs to be updated (which is needed only for the 1st range)
                    typename RT_Item::CompT comp;
                    auto first_item = (*m_it).m_first_item;
                    for (auto it = begin_item, end = end_item; it != end; ++it) {
                        // must compare as RT_Item
                        if (comp(*it, first_item)) {
                            first_item = *it;
                        }
                    }

                    // update the 1st item
                    if (first_item != (*m_it).m_first_item) {
                        m_it.modifyItem().m_first_item = first_item;
                    }
                }
                
                // Forwards a value to the add item callback                
                std::function<void(ItemT)> add_item_callback = [&](ItemT item) {
                    (*add_callback_ptr)(item.m_value);                    
                };                
                
                std::function<void(ItemT)> *add_item_callback_ptr = (add_callback_ptr ? &add_item_callback : nullptr);
                return (*this)->bulkInsertUnique(begin_item, end_item, add_item_callback_ptr).second;
            }
            
            /**
             * Erase existing elements, ignore non-existing ones
             * @return number of erased elements
            */
            template <typename iterator_t> std::size_t bulkErase(iterator_t begin_item, iterator_t end_item,
                CallbackT *erase_callback_ptr = nullptr)
            {
                // Forwards a value to the erase item callback
                std::function<void(ItemT)> erase_item_callback = [&](ItemT item) {
                    (*erase_callback_ptr)(item.m_value);
                };
                
                std::function<void(ItemT)> *erase_item_callback_ptr = (erase_callback_ptr ? &erase_item_callback : nullptr);
                return (*this)->bulkErase(begin_item, end_item, static_cast<const ItemT*>(nullptr),
                    erase_item_callback_ptr);
            }

            // Erase the empty range, iterator gets invalidated
            void erase()
            {
                // destroy the range associated block
                m_block->destroy();
                m_block = nullptr;
                m_index_ptr->erase(m_it);
            }
            
            bool empty() const {
                return !m_block || m_block->empty();
            }

        private:
            RT_Index *m_index_ptr = nullptr;
            iterator m_it;
            iterator m_next_it;
            const iterator m_begin;
            const iterator m_end;
            std::unique_ptr<BlockT> m_block;   
            std::pair<std::optional<ItemT>, std::optional<ItemT>> m_bounds;
            // flag indicating if we're at the first range
            bool m_is_first;
            const bool m_asc;

            void next(iterator &it)
            {
                if (it == m_end) {
                    return;
                }
                if (m_asc) {
                    ++it;
                } else {
                    if (it == m_begin) {
                        it = m_end;
                    } else {
                        --it;
                    }
                }
            }
        };
        
        /**
         * Get the first / last range iterator
         * null block not included
        */
        RangeIterator beginRange(bool asc = true) const
        {
            auto &index = const_cast<RT_Index&>(m_index);
            if (asc) {
                return { index, index.begin(), index.begin(), index.end(), true, asc };
            } else {
                auto last = index.end();
                if (last != index.begin()) {
                    --last;
                }
                return { index, last, index.begin(), index.end(), true, asc };
            }
        }

        /**
         * Get the lower bound iterator (or begin if no bound defined)
        */
        RangeIterator lowerBound(const KeyT &key, bool key_inclusive) const
        {
            auto &index = const_cast<RT_Index&>(m_index);
            auto it = index.findLowerEqualBound(RT_Item { ItemT { key, ValueT() } });
            // less than all other stored keys
            if (it == index.end()) {
                it = index.begin();
            }
            return { const_cast<RT_Index&>(m_index), it, index.begin(), index.end(), it == index.begin(), true };
        }

        /**
         * Get the null block if it exists
        */
        std::unique_ptr<NullBlockT> getNullBlock() const
        {
            if (!(*this)->m_rt_null_block_addr.isValid()) {
                return nullptr;
            }
            return std::make_unique<NullBlockT>(this->myPtr((*this)->m_rt_null_block_addr));
        }

        /**
         * Get the number of existing ranges
        */
        std::size_t getRangeCount() const {
            return m_index.size();
        }

        /**
         * Builder class allows taking advantage of batch operations
        */
        class Builder
        {
        public:
            Builder() = default;

            // construct with pre-populated null items
            Builder(std::unordered_set<ValueT> &&remove_null_items, std::unordered_set<ValueT> &&add_null_items)
                : m_remove_null_items(std::move(remove_null_items))
                , m_add_null_items(std::move(add_null_items))                
            {
            }

            ~Builder()
            {
                assert(m_add_items.empty() && m_add_null_items.empty() 
                    && m_remove_items.empty() && m_remove_null_items.empty()
                    && "RangeTree::Builder::flush() or close() must be called before destruction");
            }

            void add(KeyT key, ValueT value) {
                m_add_items.insert(ItemT {key, value});
            }

            void remove(KeyT key, ValueT value)
            {
                // if element is in "to add" list then simply remove it from there
                if (m_add_items.erase(ItemT {key, value})) {
                    return;
                }
                m_remove_items.insert(ItemT {key, value});
            }

            void addNull(ValueT value) {
                m_add_null_items.insert(value);
            }

            void removeNull(ValueT value)
            {
                // if element is in "to add" list then simply remove it from there
                if (m_add_null_items.erase(value)) {
                    return;
                }
                m_remove_null_items.insert(value);
            }

            /**
             * @param add_callback_ptr optional callback to be called for each new added element
             * @param erase_callback_ptr optional callback to be called for each erased element
            */
            void flush(RangeTree &range_tree, CallbackT *add_callback_ptr = nullptr,
                CallbackT *erase_callback_ptr = nullptr)
            {
                // erase items first
                if (!m_remove_items.empty()) {
                    std::vector<ItemT> items;
                    std::copy(m_remove_items.begin(), m_remove_items.end(), std::back_inserter(items));
                    range_tree.bulkErase(items.begin(), items.end(), erase_callback_ptr);
                    m_remove_items.clear();
                }
                // ... and null items
                if (!m_remove_null_items.empty()) {
                    range_tree.bulkEraseNull(m_remove_null_items.begin(), m_remove_null_items.end(), erase_callback_ptr);
                    m_remove_null_items.clear();
                }
                if (!m_add_items.empty()) {
                    std::vector<ItemT> items;
                    std::copy(m_add_items.begin(), m_add_items.end(), std::back_inserter(items));                    
                    range_tree.bulkInsert(items.begin(), items.end(), add_callback_ptr);
                    m_add_items.clear();
                }
                if (!m_add_null_items.empty()) {
                    range_tree.bulkInsertNull(m_add_null_items.begin(), m_add_null_items.end(), add_callback_ptr);
                    m_add_null_items.clear();
                }
            }

            // undo all operations without flushing
            void close() 
            {
                m_add_items.clear();
                m_remove_items.clear();
                m_add_null_items.clear();                
                m_remove_null_items.clear();
            }
            
            // releaseNullItems is only allowed when no other items are present
            std::unordered_set<ValueT> &&releaseAddNullItems() 
            {
                assert(m_add_items.empty());
                assert(m_remove_items.empty());

                return std::move(m_add_null_items);
            }

            std::unordered_set<ValueT> &&releaseRemoveNullItems()
            {
                assert(m_add_items.empty());
                assert(m_remove_items.empty());

                return std::move(m_remove_null_items);
            }

            bool empty() const
            {
                return m_add_items.empty() && m_remove_items.empty() && m_add_null_items.empty() 
                    && m_remove_null_items.empty();
            }

        private:
            // buffer with items to be removed
            std::unordered_set<ItemT, typename ItemT::Hash> m_remove_items;
            // buffer with items to be added
            std::unordered_set<ItemT, typename ItemT::Hash> m_add_items;
            // special buffer for items with null keys to be removed/added
            std::unordered_set<ValueT> m_remove_null_items;
            std::unordered_set<ValueT> m_add_null_items;            
        };
        
        std::unique_ptr<Builder> makeBuilder() const {
            return std::make_unique<Builder>();
        }
        
        std::uint64_t size() const {
            return (*this)->m_size;
        }
        
        bool operator==(const RangeTree &other) const
        {
            if (this->isNull() || other.isNull()) {
                return false;
            }
            return m_index == other.m_index;
        }

        // check if there're any non-null elements in the tree
        bool hasAnyNonNull() const {
            return !m_index.empty();
        }

        // perform operation "f" on all elements, e.g. to drop all references
        void forAll(std::function<void(ValueT)> f) const;
        
#ifndef NDEBUG
        void show()
        {
            auto it = m_index.begin();
            while (it != m_index.end()) {                
                auto block = (*it).m_block_ptr(this->getMemspace());                
                std::cout << "--- NEXT BLOCK --" << std::endl;                
                std::cout << "block size: " << block.size() << std::endl;
                std::cout << "max block size: " << (*this)->m_max_block_size << std::endl;
                for (auto block_it = block.begin(), block_end = block.end(); block_it != block_end; ++block_it) {
                    std::cout << "key: " << (*block_it).m_key << ", value: " << (*block_it).m_value << std::endl;
                }
                ++it;
            }
        }
#endif

        void detach() const
        {
            m_index.detach();
            super_t::detach();
        }
        
        void commit() const
        {
            m_index.commit();
            super_t::commit();
        }

    private:
        RT_Index m_index;

        // Find existing or create new range
        RangeIterator getRange(ItemT item)
        {
            if (m_index.empty()) {
                return insertRange(item);
            }

            auto it = m_index.findLowerEqualBound(RT_Item { item });
            if (it == m_index.end()) {
                it = --m_index.end();
            }

            // retrieve existing range
            return { m_index, it, m_index.begin(), m_index.end(), it == m_index.begin(), true };
        }

        RangeIterator getExistingRange(ItemT item)
        {
            assert(!m_index.empty());
            auto it = m_index.findLowerEqualBound(RT_Item { item });
            if (it == m_index.end()) {
                it = --m_index.end();
            }

            // retrieve existing range
            return { m_index, it, m_index.begin(), m_index.end(), it == m_index.begin(), true };
        }
        
        RangeIterator insertRange(ItemT item)
        {
            BlockT new_block(this->getMemspace());
            m_index.insert({ item, new_block });
            auto it = m_index.find(RT_Item { item });
            return { m_index, it, m_index.begin(), m_index.end(), it == m_index.begin(), true };
        }

        RangeIterator splitRange(RangeIterator &&range)
        {
            auto new_block = range.split();
            m_index.insert({ new_block.front(), new_block });
            auto it = m_index.find(RT_Item { *range.getBounds().first });
            return { m_index, it, m_index.begin(), m_index.end(), it == m_index.begin(), true };
        }
    };

    template <typename KeyT, typename ValueT>
    void RangeTree<KeyT, ValueT>::forAll(std::function<void(ValueT)> f) const
    {
        for (auto it = m_index.begin(), end = m_index.end(); it != end; ++it) {
            auto block = (*it).m_block_ptr(this->getMemspace());
            for (auto block_it = block.begin(), block_end = block.end(); block_it != block_end; ++block_it) {
                f((*block_it).m_value);
            }
        }
        // also iterate over null block if it exists
        if ((*this)->m_rt_null_block_addr.isValid()) {
            NullBlockT null_block(this->myPtr((*this)->m_rt_null_block_addr));
            for (auto it = null_block.begin(), end = null_block.end(); it != end; ++it) {
                f(*it);
            }
        }
    }

}