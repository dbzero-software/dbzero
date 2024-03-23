#pragma once

#include "RangeTree.hpp"
#include <dbzero/core/collections/full_text/FT_IteratorBase.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>

namespace db0

{

    /**
     * The RT_RangeIterator can be used to apply a range filter on top of a standard FT_Iterator
     * The RT_RangeIterator implements the FT_IteratorBase interface only so it can't be joined with other iterators
     * @tparam KeyT type of the RangeTree key
     * @tparam ValueT type of the RangeTree value
    */
    template <typename KeyT, typename ValueT> class RT_RangeIterator: public FT_IteratorBase
    {
        using RT_TreeT = RangeTree<KeyT, ValueT>;
        using self_t = RT_RangeIterator<KeyT, ValueT>;
    public:
        // Create to range-filter results of a specific FT-iterator (e.g. tag query)
        RT_RangeIterator(const RT_TreeT &tree, std::unique_ptr<FT_Iterator<ValueT> > &&it, std::optional<KeyT> min,
            bool min_inclusive, std::optional<KeyT> max, bool max_inclusive)
            : RT_RangeIterator(tree, true, std::move(it), min, min_inclusive, max, max_inclusive)
        {
        }
        
        // Create range-only filter
        RT_RangeIterator(const RT_TreeT &tree, std::optional<KeyT> min = {},
            bool min_inclusive = false, std::optional<KeyT> max = {}, bool max_inclusive = false)
            : RT_RangeIterator(tree, false, nullptr, min, min_inclusive, max, max_inclusive)
        {
        }

		bool isEnd() const override;

        void next(void *buf = nullptr) override;

        const std::type_info &keyTypeId() const override;

        const std::type_info &typeId() const override;

        std::ostream &dump(std::ostream &os) const override;

        bool equal(const FT_IteratorBase &it) const override;

        std::unique_ptr<FT_IteratorBase> begin() const override;
        
        const FT_IteratorBase *find(const FT_IteratorBase &it) const override;

    private:
        using ItemT = typename RT_TreeT::ItemT;
        using RT_IteratorT = typename RT_TreeT::BlockT::FT_IteratorT;
        RT_TreeT m_tree;        
        typename RT_TreeT::RangeIterator m_tree_it;
        const bool m_has_query;
        std::unique_ptr<FT_Iterator<ValueT> > m_query_it;
        std::optional<KeyT> m_min;
        const bool m_min_inclusive;
        std::optional<KeyT> m_max;
        const bool m_max_inclusive;

        // Create to range-filter results of a specific FT-iterator (e.g. tag query)
        RT_RangeIterator(const RT_TreeT &tree, bool has_query, std::unique_ptr<FT_Iterator<ValueT> > &&it, std::optional<KeyT> min,
            bool min_inclusive, std::optional<KeyT> max, bool max_inclusive)
            : m_tree(tree)
            , m_tree_it((min ? tree.lowerBound(*min, min_inclusive) : tree.beginRange()))
            , m_has_query(has_query)
            , m_query_it(std::move(it))
            , m_min(min)
            , m_min_inclusive(min_inclusive)
            , m_max(max)
            , m_max_inclusive(max_inclusive)
        {
            if (!m_tree_it.isEnd()) {
                m_range_it = m_tree_it->makeIterator();
                m_native_it_ptr = reinterpret_cast<RT_IteratorT *>(m_range_it.get());
                assert(!m_range_it->isEnd());
                m_has_lh_item = _next(m_lh_item);
            }
        }
        
        inline bool inRange(const KeyT &key) const
        {
            return (!m_min || (m_min_inclusive ? key >= *m_min : key > *m_min)) &&
                (!m_max || (m_max_inclusive ? key <= *m_max : key < *m_max));
        }
        
        // the range currently being iterated over
        std::unique_ptr<FT_IteratorBase> m_range_it;
        // the underlying native iterator (valid when m_range_it is not null)
        RT_IteratorT *m_native_it_ptr = nullptr;
        // the look-ahead item
        bool m_has_lh_item = false;
        ItemT m_lh_item;

        bool _next(ItemT &next_item);
    };
    
    template <typename KeyT, typename ValueT> const std::type_info &RT_RangeIterator<KeyT, ValueT>::keyTypeId() const
    {
        // note that ValueT is the actual full-text iteration key
        return typeid(ValueT);
    }

    template <typename KeyT, typename ValueT> const std::type_info &RT_RangeIterator<KeyT, ValueT>::typeId() const
    {
        return typeid(self_t);
    }

    template <typename KeyT, typename ValueT> std::ostream &RT_RangeIterator<KeyT, ValueT>::dump(std::ostream &os) const
    {
        return os << "RT_RangeIterator";
    }
    
    template <typename KeyT, typename ValueT> bool RT_RangeIterator<KeyT, ValueT>::isEnd() const
    {
        return !m_has_lh_item;
    }
    
    template <typename KeyT, typename ValueT> bool RT_RangeIterator<KeyT, ValueT>::equal(const FT_IteratorBase &it) const
    {
        if (typeid(it) != typeid(self_t)) {
            return false;
        }
        // const auto &other = static_cast<const self_t &>(it);
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT, typename ValueT>
    const FT_IteratorBase *RT_RangeIterator<KeyT, ValueT>::find(const FT_IteratorBase &it) const
    {
        if (this->equal(it)) {
            return this;
        }
        if (!m_query_it) {
            return nullptr;
        }
        return m_query_it->find(it);
    }

    template <typename KeyT, typename ValueT>
    void RT_RangeIterator<KeyT, ValueT>::next(void *buf)
    {
        assert(m_has_lh_item);
        *reinterpret_cast<ValueT *>(buf) = m_lh_item.m_value;
        m_has_lh_item = _next(m_lh_item);
    }

    template <typename KeyT, typename ValueT>
    bool RT_RangeIterator<KeyT, ValueT>::_next(ItemT &item)
    {
        while (m_range_it) {
            assert(!m_range_it->isEnd());
            item = *m_native_it_ptr->asNative();
            m_range_it->next();
            if (m_range_it->isEnd()) {
                m_tree_it.next();
                if (m_tree_it.isEnd()) {
                    m_range_it = nullptr;
                    m_native_it_ptr = nullptr;
                } else {
                    m_range_it = m_tree_it->makeIterator();
                    m_native_it_ptr = reinterpret_cast<RT_IteratorT *>(m_range_it.get());
                    assert(!m_range_it->isEnd());
                }
            }
            if (inRange(item.m_key)) {
                return true;
            }
        }
        return false;
    }

    template <typename KeyT, typename ValueT>
    std::unique_ptr<FT_IteratorBase> RT_RangeIterator<KeyT, ValueT>::begin() const
    {        
        return std::unique_ptr<FT_IteratorBase>(new self_t(m_tree, m_has_query, (m_query_it ? m_query_it->beginTyped() : nullptr), 
            m_min, m_min_inclusive, m_max, m_max_inclusive));
    }

}
