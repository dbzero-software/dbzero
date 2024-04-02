#pragma once

#include <cstdint>
#include <algorithm>
#include <optional>
#include "RangeTreeBlock.hpp"
#include "RT_NullBlock.hpp"
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/serialization/Types.hpp>

namespace db0

{
    
    template <typename KeyT, typename ValueT> struct [[gnu::packed]] RT_ItemT
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
    
    struct [[gnu::packed]] o_range_tree: public o_fixed<o_range_tree>
    {
        std::uint32_t m_max_block_size;
        // address of the underlygin v_bindex
        std::uint64_t m_rt_index_addr = 0;
        // pointer to a single null-keys block instance
        std::uint64_t m_rt_null_block_addr = 0;
        // the total number of elements in the tree
        std::uint64_t m_size = 0;

        o_range_tree(std::uint32_t max_block_size)
            : m_max_block_size(max_block_size)
        {
        }
    };
    
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
         * Insert 1 or more null elements (null key)
         * @param add_callback_ptr optional callback to be called for each new added element
        */
        template <typename IteratorT> void bulkInsertNull(IteratorT begin, IteratorT end,
            CallbackT *add_callback_ptr = nullptr)
        {
            // create the null block at the first null insert
            if ((*this)->m_rt_null_block_addr == 0) {
                NullBlockT null_block(this->getMemspace());
                this->modify().m_rt_null_block_addr = null_block.getAddress();
            }

            auto null_block_ptr = getNullBlock();
            assert(null_block_ptr);
            // insert values into the null block directly
            auto diff = null_block_ptr->bulkInsert(begin, end, add_callback_ptr).first;
            if (diff > 0) {
                this->modify().m_size += diff;
            }
        }

        class RangeIterator
        {
        public:
            RangeIterator(Memspace &memspace, const iterator &it, const iterator &begin, 
                const iterator &end, bool is_first, bool asc)
                : m_memspace(memspace)
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

            bool inBounds(ItemT item) const
            {
                assert(m_asc);
                // the second condition is to allow multiple range with identical element
                return (!m_bounds.first || !(item < *m_bounds.first)) && (!m_bounds.second || (item < *m_bounds.second));
            }

            bool canInsert(ItemT item) const
            {
                assert(m_asc);
                // the second condition is to allow multiple range with identical element
                return (m_is_first || !m_bounds.first || !(item < *m_bounds.first)) && (!m_bounds.second || (item < *m_bounds.second));
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
                    m_block = std::make_unique<BlockT>((*m_it).m_block_ptr(m_memspace));
                }
                return *m_block;
            }

            BlockT *operator->()
            {
                return &**this;
            }
            
            bool isUnbound() const
            {
                assert(m_asc);
                return m_next_it == m_end;
            }
            
            void operator=(RangeIterator &&other)
            {                
                assert(&m_memspace == &other.m_memspace);
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

            bool isEnd() const
            {
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
                BlockT new_block(m_memspace);
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
                    // check if the first item needs to be updated
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
                    
        private:
            Memspace &m_memspace;
            iterator m_it;
            iterator m_next_it;
            const iterator m_begin;
            const iterator m_end;
            std::unique_ptr<BlockT> m_block;            
            std::pair<std::optional<ItemT>, std::optional<ItemT>> m_bounds;
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
            if (asc) {
                return { this->getMemspace(), 
                    const_cast<RT_Index&>(m_index).begin(),
                    const_cast<RT_Index&>(m_index).begin(), 
                    const_cast<RT_Index&>(m_index).end(), true, asc };
            } else {
                auto last = const_cast<RT_Index&>(m_index).end();
                if (last != const_cast<RT_Index&>(m_index).begin()) {
                    --last;
                }
                return { this->getMemspace(),
                    last,
                    const_cast<RT_Index&>(m_index).begin(),
                    const_cast<RT_Index&>(m_index).end(), true, asc };
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
            return { this->getMemspace(), it, index.begin(), index.end(), it == index.begin(), true };
        }

        /**
         * Get the null block if it exists
        */
        std::unique_ptr<NullBlockT> getNullBlock() const
        {
            if ((*this)->m_rt_null_block_addr == 0) {
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

            ~Builder() {
                assert(m_items.empty() && m_null_items.empty() && "RangeTree::Builder::flush() or close() must be called before destruction");
            }

            void add(KeyT key, ValueT value) {
                m_items.push_back({key, value});
            }

            void addNull(ValueT value) {
                m_null_items.push_back(value);
            }

            /**
             * @param add_callback_ptr optional callback to be called for each new added element
            */
            void flush(RangeTree &range_tree, CallbackT *add_callback_ptr = nullptr)
            {                
                if (!m_items.empty()) {
                    range_tree.bulkInsert(m_items.begin(), m_items.end(), add_callback_ptr);
                    m_items.clear();
                }
                if (!m_null_items.empty()) {
                    range_tree.bulkInsertNull(m_null_items.begin(), m_null_items.end(), add_callback_ptr);
                    m_null_items.clear();
                }                
            }

            // undo all operations without flushing
            void close() 
            {
                m_items.clear();
                m_null_items.clear();                
            }

        private:
            std::vector<ItemT> m_items;
            // special buffer for items with null keys
            std::vector<ValueT> m_null_items;
        };
        
        std::unique_ptr<Builder> makeBuilder() const
        {
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
            return { this->getMemspace(), it, m_index.begin(), m_index.end(), it == m_index.begin(), true };
        }

        RangeIterator insertRange(ItemT item)
        {
            BlockT new_block(this->getMemspace());
            m_index.insert({ item, new_block });
            auto it = m_index.find(RT_Item { item });
            return { this->getMemspace(), it, m_index.begin(), m_index.end(), it == m_index.begin(), true };
        }

        RangeIterator splitRange(RangeIterator &&range)
        {
            auto new_block = range.split();
            m_index.insert({ new_block.front(), new_block });
            auto it = m_index.find(RT_Item { *range.getBounds().first });
            return { this->getMemspace(), it, m_index.begin(), m_index.end(), it == m_index.begin(), true };
        }
    };
    
}