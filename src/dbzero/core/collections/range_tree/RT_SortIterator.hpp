#pragma once

#include "RangeTree.hpp"
#include <dbzero/core/collections/full_text/FT_IteratorBase.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/collections/full_text/FT_MemoryIndex.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Fixture.hpp>

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
    public:
        // Create joined with FT-iterator
        RT_SortIterator(const RT_TreeT &tree, std::unique_ptr<FT_Iterator<ValueT> > &&it, bool asc = true)
            : RT_SortIterator(tree, true, std::move(it), asc, nullptr)
        {
        }
        
        // Create for sorting by additional criteria
        RT_SortIterator(const RT_TreeT &tree, std::unique_ptr<SortedIterator<ValueT> > &&inner_it, bool asc = true)
            : RT_SortIterator(tree, inner_it->hasFTQuery(), inner_it->beginFTQuery(), asc, std::move(inner_it))
        {
        }

        // Create for sorting the entire range tree
        RT_SortIterator(const RT_TreeT &tree, bool asc = true)
            : RT_SortIterator(tree, false, nullptr, asc, nullptr)
        {
        }

		bool isEnd() const override;

        void next(void *buf = nullptr) override;
        
        const std::type_info &keyTypeId() const override;

        const std::type_info &typeId() const override;

        std::ostream &dump(std::ostream &os) const override;

        bool equal(const FT_IteratorBase &it) const override;

        const FT_IteratorBase *find(const FT_IteratorBase &it) const override;

        std::unique_ptr<FT_IteratorBase> begin() const override;

        std::unique_ptr<FT_Iterator<ValueT> > beginFTQuery() const override;

        bool hasFTQuery() const override;
        
        std::unique_ptr<SortedIterator<ValueT> > beginSorted(std::unique_ptr<FT_Iterator<ValueT> > = nullptr) const override;

        SortedIteratorType getSerialTypeId() const override;

        void serialize(std::vector<std::byte> &) const override;
        
        static RT_SortIterator<KeyT, ValueT> deserialize(Snapshot &snapshot, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);

    private:
        using BlockItemT = typename RT_TreeT::BlockT::ItemT;
        RT_TreeT m_tree;
        typename RT_TreeT::RangeIterator m_tree_it;
        std::unique_ptr<FT_Iterator<ValueT> > m_query_it;
        const bool m_asc;
        const bool m_has_query;
        // the iterator over null keys only
        std::unique_ptr<FT_Iterator<ValueT> > m_null_query_it;
        // the optional inner-sort iterator
        std::unique_ptr<SortedIterator<ValueT> > m_inner_it;
        std::unique_ptr<SortedIterator<ValueT> > m_sorted_it;
        // a flag indicating that m_sorted_it was created over the null block
        bool m_sorted_null_block = false;

        // Create AND-joined with FT-iterator
        RT_SortIterator(const RT_TreeT &tree, bool has_query, std::unique_ptr<FT_Iterator<ValueT> > &&it, bool asc,
            std::unique_ptr<SortedIterator<ValueT> > &&inner_it)
            : m_tree(tree)
            , m_tree_it(m_tree.beginRange(asc))
            , m_query_it(std::move(it))
            , m_asc(asc)
            , m_has_query(has_query)
            , m_null_query_it(beginNullBlockQuery())
            , m_inner_it(std::move(inner_it))
            // force end if they underlying query is end / empty
            , m_force_end(m_has_query && (!m_query_it || m_query_it->isEnd()))
        {
            if (m_tree_it.isEnd()) {
                m_null_it = std::move(m_null_query_it);
                m_null_query_it = nullptr;
            }
        }

        struct HeapItem
        {
            KeyT m_key;
            ValueT m_value;

            HeapItem() = default;

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
        // the look-ahead item
        bool m_has_lh_item = false;
        // flag indicating if the look-ahead item is null-key
        bool m_lh_is_null = false;
        HeapItem m_lh_item;
        bool m_force_end = false;

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
         * @return true if a null key was fetched
         */
        bool _next(ValueT *value_buf, KeyT *key_buf = nullptr);

        /**
         * Pulls the next item and maintain the look-ahead buffer
         * @return true if a null key was fetched
         */        
        bool nextItem(HeapItem &next_item);

        // checks end condition without the look-ahead logic
        inline bool _isEndNoLH() const {
            return m_items.empty() && ((m_has_query && !m_query_it) || m_tree_it.isEnd()) && !m_null_it && !m_sorted_it;
        }

        /**
         * Internal and inlined version of isEnd
        */
        inline bool _isEnd() const {
            return (this->_isEndNoLH() && !m_has_lh_item) || m_force_end;
        }
    };
    
    template <typename KeyT, typename ValueT> bool RT_SortIterator<KeyT, ValueT>::isEnd() const
    {
        return this->_isEnd();
    }
    
    template <typename KeyT, typename ValueT> std::unique_ptr<FT_Iterator<ValueT> >
    RT_SortIterator<KeyT, ValueT>::beginNullBlockQuery()
    {
        if (m_has_query && !m_query_it) {
            return nullptr;
        }

        auto null_block = m_tree.getNullBlock();
        if (null_block) {
            FT_ANDIteratorFactory<ValueT> and_factory;
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
        assert(!this->_isEnd());
        if (m_inner_it) {
            for (;;) {
                // pull from inner sorted iterator if available
                if (m_sorted_it) {
                    m_sorted_it->next(buf);
                    if (m_sorted_it->isEnd()) {
                        m_sorted_it = nullptr;
                        m_sort_buffer.clear();
                        if (m_sorted_null_block) {
                            // end the iterator after completing the null block
                            m_force_end = true;
                        }
                    }
                    return;
                }

                HeapItem next_item;
                bool next_key_null = nextItem(next_item);
                if (buf) {
                    *static_cast<ValueT*>(buf) = next_item.m_value;
                }

                if (next_key_null) {
                    // since the null are was reached, finished with combining the 
                    // null block and the inner sorted iterator
                    m_sorted_it = m_inner_it->beginSorted(beginNullBlockQuery());
                    if (m_sorted_it->isEnd()) {
                        // edge case when null items cannot be joined with the inner iterator
                        m_sorted_it = nullptr;
                        m_force_end = true;
                        return;
                    }
                    m_sorted_null_block = true;
                    // continue with the sorted iterator
                    continue;
                }

                // collect identical keys into the sort buffer
                if (m_has_lh_item && m_lh_item.m_key == next_item.m_key) {
                    while (m_has_lh_item && m_lh_item.m_key == next_item.m_key && !m_lh_is_null) {
                        m_sort_buffer.push_back(next_item.m_value);
                        next_key_null = nextItem(next_item);
                    }
                    m_sort_buffer.push_back(next_item.m_value);
                } else {                
                    // no look-ahead items, just return
                    return;
                }
                
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
                // it might happen that values are not present in the inner iterator
                if (m_sorted_it->isEnd()) {
                    m_sorted_it = nullptr;
                    m_sort_buffer.clear();
                }                
            }
        } else {
            this->_next(static_cast<ValueT*>(buf));
        }
    }

    template <typename KeyT, typename ValueT>
    bool RT_SortIterator<KeyT, ValueT>::nextItem(HeapItem &next_item)
    {
        assert(!this->_isEnd());
        if (!m_has_lh_item) {            
            // feed the 1st lh-item
            m_lh_is_null = this->_next(&m_lh_item.m_value, &m_lh_item.m_key);
            m_has_lh_item = true;
        }

        // pull item from the lh-buffer
        next_item = m_lh_item;
        bool result = m_lh_is_null;
        // feed the lh-buffer
        if (this->_isEndNoLH()) {
            m_has_lh_item = false;
            assert(this->_isEnd());
        } else {
            m_lh_is_null = this->_next(&m_lh_item.m_value, &m_lh_item.m_key);
        }
        return result;
    }

    template <typename KeyT, typename ValueT>
    bool RT_SortIterator<KeyT, ValueT>::_next(ValueT *value_buf, KeyT *key_buf)
    {
        assert(!isEnd());
        using RT_IteratorT = typename RT_TreeT::BlockT::FT_IteratorT;
        for (;;) {
            // pull from heap if anything available there
            if (!m_items.empty()) {
                if (value_buf) {
                    *value_buf = m_items.front().m_value;
                }
                if (key_buf) {
                    *key_buf = m_items.front().m_key;
                }
                if (m_asc) {
                    std::pop_heap(m_items.begin(), m_items.end(), MinCompT());
                } else {
                    std::pop_heap(m_items.begin(), m_items.end(), MaxCompT());
                }
                m_items.pop_back();
                return false;
            }
            
            // pull directly from the null block iterator if available
            if (m_null_it) {
                m_null_it->next(value_buf);
                if (m_null_it->isEnd()) {
                    m_null_it = nullptr;
                }
                // return true to indicate null key
                return true;
            }
            
            // ingest another range (block of data) by joining with the query iterator
            FT_ANDIteratorFactory<ValueT> and_factory;
            if (m_has_query) {
                and_factory.add(m_query_it->beginTyped(-1));
            }
            and_factory.add(m_tree_it->makeIterator());
            auto it = and_factory.release(-1);

            // find the range-tree iterator in the query tree (always available)
            auto inner_it = it->find(*m_tree_it->makeIterator());
            // no results if no inner it
            if (inner_it) {
                assert(inner_it);
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
            
            m_tree_it.next();
            // initialize the null-key block query as the last step
            if (m_tree_it.isEnd()) {
                m_null_it = std::move(m_null_query_it);
                m_null_query_it = nullptr;
            }
        }
    }
    
    template <typename KeyT, typename ValueT> std::ostream &RT_SortIterator<KeyT, ValueT>::dump(std::ostream &os) const
    {
        return os << "RT_SortIterator";        
    }

    template <typename KeyT, typename ValueT> bool RT_SortIterator<KeyT, ValueT>::equal(const FT_IteratorBase &it) const
    {
        if (this->typeId() != it.typeId()) {        
            return false;
        }
        // const auto &other = static_cast<const RT_SortIterator<KeyT, ValueT> &>(it);
        throw std::runtime_error("Not implemented");        
    }
    
    template <typename KeyT, typename ValueT>
    const FT_IteratorBase *RT_SortIterator<KeyT, ValueT>::find(const FT_IteratorBase &it) const
    {
        if (this->equal(it)) {
            return this;
        }
        if (!m_query_it) {
            return nullptr;
        }
        return m_query_it->find(it);
    }
    
    template <typename KeyT, typename ValueT> const std::type_info &RT_SortIterator<KeyT, ValueT>::keyTypeId() const
    {
        return typeid(ValueT);
    }

    template <typename KeyT, typename ValueT> const std::type_info &RT_SortIterator<KeyT, ValueT>::typeId() const
    {
        return typeid(self_t);
    }

    template <typename KeyT, typename ValueT>
    std::unique_ptr<FT_IteratorBase> RT_SortIterator<KeyT, ValueT>::begin() const
    {
        if (m_has_query) {
            return std::make_unique<self_t>(m_tree, (m_query_it ? m_query_it->beginTyped() : nullptr), m_asc);
        } else {
            return std::make_unique<self_t>(m_tree, m_asc);
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
        std::unique_ptr<SortedIterator<std::uint64_t> > nested_inner_it;
        if (m_inner_it) {
            nested_inner_it = m_inner_it->beginSorted();
        }
        if (ft_query) {
            // sort specific inner query
            return std::unique_ptr<self_t>(new self_t(m_tree, true, ft_query->beginTyped(-1),
                m_asc, std::move(nested_inner_it)));
        } else {
            // create a clone of this iterator
            return std::unique_ptr<self_t>(new self_t(m_tree, m_has_query, (m_query_it ? m_query_it->beginTyped(-1) : nullptr),
                m_asc, std::move(nested_inner_it)));
        }
    }

    template <typename KeyT, typename ValueT>
    SortedIteratorType RT_SortIterator<KeyT, ValueT>::getSerialTypeId() const
    {
        return SortedIteratorType::RT_Sort;
    }
    
    template <typename KeyT, typename ValueT>
    void RT_SortIterator<KeyT, ValueT>::serialize(std::vector<std::byte> &v) const
    {
        db0::serial::write(v, db0::serial::typeId<KeyT>());
        db0::serial::write(v, db0::serial::typeId<ValueT>());
        db0::serial::write(v, m_tree.getMemspace().getUUID());
        db0::serial::write(v, m_tree.getAddress());
        db0::serial::write<bool>(v, m_asc);
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
    RT_SortIterator<KeyT, ValueT> RT_SortIterator<KeyT, ValueT>::deserialize(Snapshot &snapshot, 
        std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
    {
        using TypeOfTypeId = decltype(db0::serial::typeId<KeyT>());
        if (db0::serial::read<TypeOfTypeId>(iter, end) != db0::serial::typeId<KeyT>()) {
            THROWF(db0::InputException) << "Deserialize: invalid key type";
        }
        if (db0::serial::read<TypeOfTypeId>(iter, end) != db0::serial::typeId<ValueT>()) {
            THROWF(db0::InputException) << "Deserialize: invalid value type";
        }
        
        auto fixture = snapshot.getFixture(db0::serial::read<std::uint64_t>(iter, end));
        std::uint64_t addr = db0::serial::read<std::uint64_t>(iter, end);
        bool asc = db0::serial::read<bool>(iter, end);
        bool has_inner = db0::serial::read<bool>(iter, end);
        if (has_inner) {
            auto inner_it = db0::serial::deserializeSortedIterator<ValueT>(snapshot, iter, end);
            return { RT_TreeT(fixture->myPtr(addr)), std::move(inner_it), asc };
        } else {            
            bool has_query = db0::serial::read<bool>(iter, end);
            if (has_query) {
                auto query_it = db0::serial::deserializeFT_Iterator<ValueT>(snapshot, iter, end);
                return { RT_TreeT(fixture->myPtr(addr)), std::move(query_it), asc };
            } else {
                return { RT_TreeT(fixture->myPtr(addr)), asc };
            }
        }
    }

}