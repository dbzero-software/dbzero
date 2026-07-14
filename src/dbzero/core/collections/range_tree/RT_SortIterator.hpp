// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "RangeTree.hpp"
#include "IndexBase.hpp"
#include "FastQueue.hpp"
#include <dbzero/core/collections/full_text/FT_IteratorBase.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/collections/full_text/FT_MemoryIndex.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/core/serialization/hash.hpp>
#include <dbzero/core/utils/shared_void.hpp>

namespace db0

{

    /**
     * The RT_SortIterator can iterate over a specific RangeTree + arbitraty full-text query iterator
     * and sort the results by the RangeTree key
     * @tparam KeyT type of the RangeTree key
     * @tparam ValueT type of the RangeTree value
    */
    template <typename KeyT, typename ValueT>
    class RT_SortIterator: public SortedIterator<ValueT>
    {
        using RT_TreeT = RangeTree<KeyT, ValueT>;
        using self_t = RT_SortIterator<KeyT, ValueT>;
        using super_t = SortedIterator<ValueT>;
    public:
        // Create joined with FT-iterator
        RT_SortIterator(const IndexBase &index, SharedPtrWrapper<RT_TreeT> tree_ptr, std::unique_ptr<FT_Iterator<ValueT> > &&it,
            bool asc = true, bool null_first = false)
            : RT_SortIterator(index, this->nextUID(), tree_ptr, true, std::move(it), asc, null_first, nullptr)
        {
        }
        
        // Create for sorting by additional criteria
        RT_SortIterator(const IndexBase &index, SharedPtrWrapper<RT_TreeT> tree_ptr, std::unique_ptr<SortedIterator<ValueT> > &&inner_it, 
            bool asc = true, bool null_first = false)
            : RT_SortIterator(index, this->nextUID(), tree_ptr, inner_it->hasFTQuery(), inner_it->beginFTQuery(), 
                asc, null_first, std::move(inner_it))
        {
        }
        
        // Create for sorting the entire range tree
        RT_SortIterator(const IndexBase &index, SharedPtrWrapper<RT_TreeT> tree_ptr, bool asc = true, bool null_first = false)
            : RT_SortIterator(index, this->nextUID(), tree_ptr, false, nullptr, asc, null_first, nullptr)
        {
        }

		bool isEnd() const override;

        // @param buf to ValueT type
        void next(void *buf = nullptr) override;
        
        const std::type_info &keyTypeId() const override;

        const std::type_info &typeId() const override;

        std::ostream &dump(std::ostream &os) const override;
        
        const FT_IteratorBase *find(std::uint64_t uid) const override;

        std::unique_ptr<FT_IteratorBase> begin() const override;

        std::unique_ptr<FT_Iterator<ValueT> > beginFTQuery() const override;

        bool hasFTQuery() const override;
        
        std::unique_ptr<SortedIterator<ValueT> > beginSorted(std::unique_ptr<FT_Iterator<ValueT> > = nullptr) const override;

        SortedIteratorType getSerialTypeId() const override;

        void getSignature(std::vector<std::byte> &) const override;
        
    protected:
        void serializeImpl(std::vector<std::byte> &) const override;
        
        double compareToImpl(const FT_IteratorBase &it) const override;
        
        double compareTo(const RT_SortIterator &it) const;

    private:        
        IndexBase m_index;
        using BlockItemT = typename RT_TreeT::BlockT::ItemT;
        SharedPtrWrapper<RT_TreeT> m_tree_ptr;
        typename RT_TreeT::RangeIterator m_tree_it;
        std::unique_ptr<FT_Iterator<ValueT> > m_query_it;
        const bool m_asc;
        const bool m_null_first = false;
        const bool m_has_query;
        // the iterator over null keys only
        std::unique_ptr<FT_Iterator<ValueT> > m_null_query_it;
        // the optional inner-sort iterator
        std::unique_ptr<SortedIterator<ValueT> > m_inner_it;
        std::unique_ptr<SortedIterator<ValueT> > m_sorted_it;
        // a flag indicating that m_sorted_it was created over the null block
        bool m_sorted_null_block = false;
        
        // Create AND-joined with FT-iterator
        RT_SortIterator(const IndexBase &index, std::uint64_t uid, SharedPtrWrapper<RT_TreeT> tree_ptr, bool has_query,
            std::unique_ptr<FT_Iterator<ValueT> > &&it, bool asc, bool null_first, std::unique_ptr<SortedIterator<ValueT> > &&inner_it)
            : super_t(uid)
            , m_index(index)
            , m_tree_ptr(tree_ptr)
            , m_tree_it([&](){
                if (m_tree_ptr)
                    return m_tree_ptr->beginRange(asc);
                return typename RT_TreeT::RangeIterator(asc);
            }())
            , m_query_it(std::move(it))
            , m_asc(asc)
            , m_null_first(null_first)
            , m_has_query(has_query)
            , m_null_query_it(beginNullBlockQuery())
            , m_inner_it(std::move(inner_it)) 
            , m_is_end(m_has_query && (!m_query_it || m_query_it->isEnd()))
        {            
            if (m_tree_it.isEnd()) {
                m_null_it = std::move(m_null_query_it);
                m_null_query_it = nullptr;
            }
            fetchNext();            
        }
        
        struct HeapItem
        {
            KeyT m_key;
            ValueT m_value;

            HeapItem() = default;

            inline HeapItem(const KeyT &key, const ValueT &value)
                : m_key(key)
                , m_value(value)
            {
            }
            
            // construct from block item
            inline HeapItem(const BlockItemT &item)
                : m_key(item.m_key)
                , m_value(item.m_value)
            {
            }
        };

        // heapified items buffer
        std::vector<HeapItem> m_items;
        // null-key block iterator
        std::unique_ptr<FT_Iterator<ValueT> > m_null_it;
        // a buffer to hold all look-ahead occurrences of identical keys (for sorting)
        // this is only relevant with complex sorting (i.e. m_inner_it != nullptr)
        std::vector<ValueT> m_sort_buffer;
        // the sort key associated with the sort buffer (if it's not empty)
        KeyT m_sort_key;
        // the look-ahead queue (up to 2 items + is null key flag)
        FastQueue<std::pair<HeapItem, bool>, 2> m_lh_queue;
        // fetch queue (non-final items)
        FastQueue<std::pair<HeapItem, bool>, 2> m_fetch_queue;
        bool m_is_end = false;

        struct MaxCompT
        {
            inline bool operator()(const HeapItem &a, const HeapItem &b) const
            {
                if (a.m_key == b.m_key) {
                    return a.m_value < b.m_value;
                }
                return a.m_key < b.m_key;                
            }
        };

        struct MinCompT
        {
            inline bool operator()(const HeapItem &a, const HeapItem &b) const
            {
                if (a.m_key == b.m_key) {
                    return b.m_value < a.m_value;
                }
                return b.m_key < a.m_key;                
            }
        };

        std::unique_ptr<FT_Iterator<ValueT> > beginNullBlockQuery();
        
        /**
         * Internal implementation without the look-ahead logic
         * @return false if no more items available
         */
        bool tryNextSorted(HeapItem &, bool &next_key_null);

        // Feed the next item into the look-ahead buffer (if anything available)
        void fetchNext();
    };
    
    template <typename KeyT, typename ValueT> bool RT_SortIterator<KeyT, ValueT>::isEnd() const {
        return m_lh_queue.empty();
    }
    
    template <typename KeyT, typename ValueT> std::unique_ptr<FT_Iterator<ValueT> >
    RT_SortIterator<KeyT, ValueT>::beginNullBlockQuery()
    {
        if (m_has_query && !m_query_it) {
            return nullptr;
        }

        if (!m_tree_ptr) {
            return nullptr;
        }

        auto null_block = m_tree_ptr->getNullBlock();
        if (null_block) {
            // NOTE: use UniqueKeys=false to allow multiple null keys
            FT_ANDIteratorFactory<ValueT, false> and_factory;
            if (m_has_query) {
                and_factory.add(m_query_it->beginTyped());
            }
            and_factory.add(null_block->makeIterator());
            auto result = and_factory.release(-1);
            if (!result->isEnd()) {
                return result;
            }
        }

        return nullptr;
    }

    template <typename KeyT, typename ValueT> void RT_SortIterator<KeyT, ValueT>::next(void *buf)
    {
        // pulls from the the look-ahead buffer and tries retrieving the next element        
        if (buf) {
            *static_cast<ValueT*>(buf) = m_lh_queue.head().first.m_value;
        }
        m_lh_queue.pop();        
        fetchNext();        
    }

    template <typename KeyT, typename ValueT> void RT_SortIterator<KeyT, ValueT>::fetchNext()
    {
        if (m_is_end) {
            return;
        }

        if (m_inner_it) {
            for (;;) {
                // pull from inner sorted iterator if available
                if (m_sorted_it) {
                    assert(!m_sorted_it->isEnd());
                    ValueT value;
                    m_sorted_it->next(&value);
                    m_lh_queue.push(std::make_pair(HeapItem(m_sort_key, value), false));
                    if (m_sorted_it->isEnd()) {
                        m_sorted_it = nullptr;
                        m_sort_buffer.clear();
                        if (m_sorted_null_block) {
                            // end the iterator after completing the null block
                            m_is_end = true;
                        }
                    }
                    return;
                }
                
                HeapItem next_item_1;
                bool next_key_null_1 = false;
                if (!tryNextSorted(next_item_1, next_key_null_1)) {
                    return;
                }
                
                if (next_key_null_1) {
                    // since the null area was reached, finish with combining the
                    // null block and the inner sorted iterator
                    m_sorted_it = m_inner_it->beginSorted(beginNullBlockQuery());
                    if (m_sorted_it->isEnd()) {
                        // edge case when null items cannot be joined with the inner iterator
                        m_sorted_it = nullptr;
                        m_is_end = true;
                        return;
                    }
                    m_sorted_null_block = true;
                    // continue with the sorted iterator
                    continue;
                }

                HeapItem next_item_2;
                bool next_key_null_2 = false;
                if (!tryNextSorted(next_item_2, next_key_null_2)) {
                    m_lh_queue.push(std::make_pair(next_item_1, next_key_null_1));
                    return;
                }

                // Collect identical keys into the sort buffer
                if (!next_key_null_2 && next_item_1.m_key == next_item_2.m_key) {
                    m_sort_buffer.push_back(next_item_1.m_value);                    
                    m_sort_buffer.push_back(next_item_2.m_value);
                    m_sort_key = next_item_1.m_key;
                    while (tryNextSorted(next_item_2, next_key_null_2)) {
                        if (!next_key_null_2 && next_item_1.m_key == next_item_2.m_key) {
                            m_sort_buffer.push_back(next_item_2.m_value);
                        } else {
                            // return item to fetch buffer
                            m_fetch_queue.push(std::make_pair(next_item_2, next_key_null_2));
                            break;
                        }
                    }

                    assert(m_sort_buffer.size() > 1);
                    std::sort(m_sort_buffer.begin(), m_sort_buffer.end());
                    // resolve sort order with the underlying sorted iterator
                    // FIXME: #opt for small buffers we should improve performance here
                    using MemoryIndexT = FT_MemoryIndex<ValueT>;
                    auto inner_query = std::make_unique<FT_IndexIterator<MemoryIndexT, ValueT>>(
                        MemoryIndexT(m_sort_buffer.data(), m_sort_buffer.data() + m_sort_buffer.size()), -1
                        );
                    
                    // sort the buffer with the inner iterator
                    m_sorted_it = m_inner_it->beginSorted(std::move(inner_query));
                    m_sorted_null_block = false;
                    // it might happen that values are not present in the inner iterator (need to be ignored)
                    if (m_sorted_it->isEnd()) {
                        m_sorted_it = nullptr;
                        m_sort_buffer.clear();
                    }

                } else {
                    // return item 2 to fetch buffer
                    m_fetch_queue.push(std::make_pair(next_item_2, next_key_null_2));
                    m_lh_queue.push(std::make_pair(next_item_1, next_key_null_1));
                    return;
                }
            }
        } else {
            HeapItem next_item;
            bool next_key_null = false;
            if (tryNextSorted(next_item, next_key_null)) {
                m_lh_queue.push(std::make_pair(next_item, next_key_null));
            }
        }
    }

    template <typename KeyT, typename ValueT>
    bool RT_SortIterator<KeyT, ValueT>::tryNextSorted(HeapItem &next_item, bool &next_key_null)
    {
        // pull from the fetch queue if not empty
        if (!m_fetch_queue.empty()) {
            next_item = m_fetch_queue.head().first;
            next_key_null = m_fetch_queue.head().second;            
            m_fetch_queue.pop();
            return true;
        }

        next_key_null = false;
        using RT_IteratorT = typename RT_TreeT::BlockT::FT_IteratorT;
        // pull null values first (if asc and nulls first policy has been set)
        if (m_null_query_it && m_null_first == m_asc) {
            m_null_it = std::move(m_null_query_it);
            m_null_query_it = nullptr;
        }

        for (;;) {
            // pull from heap if anything available there
            if (!m_items.empty()) {
                next_item = m_items.front();
                if (m_asc) {
                    std::pop_heap(m_items.begin(), m_items.end(), MinCompT());
                } else {
                    std::pop_heap(m_items.begin(), m_items.end(), MaxCompT());
                }
                m_items.pop_back();
                return true;
            }
            
            // pull directly from the null block iterator if available
            if (m_null_it) {
                m_null_it->next(&next_item.m_value);
                if (m_null_it->isEnd()) {
                    m_null_it = nullptr;
                }
                // set flat to true to indicate null key
                next_key_null = true;
                return true;
            }
            
            if (m_tree_it.isEnd()) {
                if (m_null_first != m_asc) {
                    m_null_it = std::move(m_null_query_it);
                    m_null_query_it = nullptr;
                    if (!m_null_it) {                        
                        return false;
                    }
                    // continue with null items
                    continue;
                } else {                    
                    return false;
                }
            }

            // ingest another range (block of data) by joining with the query iterator
            // NOTE: use UniqueKey = false to retrieve object multiple times if added under different keys
            FT_ANDIteratorFactory<ValueT, false> and_factory;
            if (m_has_query) {
                and_factory.add(m_query_it->beginTyped(-1));
            }

            auto rt_tree_it = m_tree_it->makeIterator();
            auto rt_tree_it_uid = rt_tree_it->getUID();
            and_factory.add(std::move(rt_tree_it));
            auto it = and_factory.release(-1);

            // find the range-tree iterator in the query tree (always available)
            auto inner_it = it->find(rt_tree_it_uid);
            // inner iterator may not exist (e.g. if no join was found)
            if (inner_it) {
                // cast to well known type
                const auto &rt_inner_it = *static_cast<const RT_IteratorT*>(inner_it);
                while (!it->isEnd()) {  
                    // retrieve current full item from the inner iterator (key + value)
                    m_items.push_back(*rt_inner_it.asNative());
                    it->next();
                }
                if (m_asc) {
                    std::make_heap(m_items.begin(), m_items.end(), MinCompT());
                } else {                
                    std::make_heap(m_items.begin(), m_items.end(), MaxCompT());
                }
            }
            
            // continue with the next range
            m_tree_it.next();
        }
    }

    template <typename KeyT, typename ValueT> std::ostream &RT_SortIterator<KeyT, ValueT>::dump(std::ostream &os) const {
        return os << "RT_SortIterator";        
    }

    template <typename KeyT, typename ValueT>
    const FT_IteratorBase *RT_SortIterator<KeyT, ValueT>::find(std::uint64_t uid) const
    {
        if (this->m_uid == uid) {
            return this;
        }
        if (m_query_it) {
            return m_query_it->find(uid);
        }
        return nullptr;
    }
    
    template <typename KeyT, typename ValueT> const std::type_info &RT_SortIterator<KeyT, ValueT>::keyTypeId() const {
        return typeid(ValueT);
    }

    template <typename KeyT, typename ValueT> const std::type_info &RT_SortIterator<KeyT, ValueT>::typeId() const {
        return typeid(self_t);
    }
    
    template <typename KeyT, typename ValueT>
    std::unique_ptr<FT_IteratorBase> RT_SortIterator<KeyT, ValueT>::begin() const
    {        
        if (m_has_query) {
            return std::make_unique<self_t>(m_index, m_tree_ptr, (m_query_it ? m_query_it->beginTyped() : nullptr), m_asc, m_null_first);
        } else {
            return std::make_unique<self_t>(m_index, m_tree_ptr, m_asc, m_null_first);
        }
    }

    template <typename KeyT, typename ValueT>
    bool RT_SortIterator<KeyT, ValueT>::hasFTQuery() const {
        return m_has_query;
    }

    template <typename KeyT, typename ValueT>
    std::unique_ptr<FT_Iterator<ValueT> > RT_SortIterator<KeyT, ValueT>::beginFTQuery() const
    {
        if (m_query_it) {
            return m_query_it->beginTyped();
        }
        return nullptr;    
    }

    template <typename KeyT, typename ValueT>
    std::unique_ptr<SortedIterator<ValueT> > RT_SortIterator<KeyT, ValueT>::beginSorted(
        std::unique_ptr<FT_Iterator<ValueT> > ft_query) const
    {
        std::unique_ptr<SortedIterator<ValueT> > nested_inner_it;
        if (m_inner_it) {
            nested_inner_it = m_inner_it->beginSorted();
        }
        if (ft_query) {
            // sort specific inner query
            return std::unique_ptr<self_t>(new self_t(m_index, this->m_uid, m_tree_ptr, true, ft_query->beginTyped(-1),
                m_asc, m_null_first, std::move(nested_inner_it)));
        } else {
            // create a clone of this iterator
            return std::unique_ptr<self_t>(new self_t(m_index, this->m_uid, m_tree_ptr, m_has_query,
                (m_query_it ? m_query_it->beginTyped(-1) : nullptr), m_asc, m_null_first, std::move(nested_inner_it)));
        }
    }

    template <typename KeyT, typename ValueT>
    SortedIteratorType RT_SortIterator<KeyT, ValueT>::getSerialTypeId() const {
        return SortedIteratorType::RT_Sort;
    }
    
    template <typename KeyT, typename ValueT>
    void RT_SortIterator<KeyT, ValueT>::serializeImpl(std::vector<std::byte> &v) const
    {
        db0::serial::write(v, db0::serial::typeId<KeyT>());
        db0::serial::write(v, db0::serial::typeId<ValueT>());        
        db0::serial::write(v, m_index.getMemspace().getUUID());
        db0::serial::write(v, m_index.getAddress());
        db0::serial::write<bool>(v, m_asc);
        db0::serial::write<bool>(v, m_null_first);
        db0::serial::write<bool>(v, m_inner_it != nullptr);
        if (m_inner_it) {            
            m_inner_it->serialize(v);
        } else {
            db0::serial::write<bool>(v, m_has_query);
            if (m_has_query) {                
                m_query_it->serialize(v);
            }
        }
    }
    
    template <typename KeyT, typename ValueT>
    double RT_SortIterator<KeyT, ValueT>::compareToImpl(const FT_IteratorBase &it) const
    {
        if (this->typeId() == it.typeId()) {
            return compareTo(reinterpret_cast<const self_t &>(it));
        }
        return 1.0;
    }
    
    template <typename KeyT, typename ValueT>
    double RT_SortIterator<KeyT, ValueT>::compareTo(const RT_SortIterator &other) const
    {
        if (m_has_query) {
            if (other.m_has_query) {
                return m_query_it->compareTo(*other.m_query_it);
            }
            return 1.0;            
        }
        return false;        
        return m_index.getAddress() == other.m_index.getAddress() ? 0.0 : 1.0;
    }

    template <typename KeyT, typename ValueT>
    void RT_SortIterator<KeyT, ValueT>::getSignature(std::vector<std::byte> &v) const
    {
        if (m_has_query) {
            m_query_it->getSignature(v);            
        } else {
            std::vector<std::byte> bytes;
            db0::serial::write(v, db0::serial::typeId<KeyT>());
            db0::serial::write(v, db0::serial::typeId<ValueT>());            
            db0::serial::write(v, m_index.getMemspace().getUUID());
            db0::serial::write(v, m_index.getAddress());
            // get signature as a hash from bytes
            db0::serial::sha256(bytes, v);
        }
    }
    
}