#pragma once

#include <optional>
#include "RangeTree.hpp"
#include <dbzero/core/collections/full_text/IteratorFactory.hpp>
#include "RT_RangeIterator.hpp"
#include "RT_Range.hpp"
#include "RT_FTIterator.hpp"

namespace db0

{

    /**
     * RangeTree ItertorFactory specialization
    */
    template <typename KeyT, typename ValueT> class RangeIteratorFactory: public IteratorFactory<ValueT>
    {
    public:
        using RT_TreeT = RangeTree<KeyT, ValueT>;

        RangeIteratorFactory(const RT_TreeT &tree, std::optional<KeyT> min = {},
            bool min_inclusive = false, std::optional<KeyT> max = {}, bool max_inclusive = false)
            : m_tree(tree)
            , m_range { min, min_inclusive, max, max_inclusive }
        {
        }

        std::unique_ptr<FT_IteratorBase> createBaseIterator() override;

        std::unique_ptr<FT_Iterator<ValueT> > createFTIterator() override;

    private:
        RT_TreeT m_tree;
        RT_Range<KeyT> m_range;
    };

    template <typename KeyT, typename ValueT> std::unique_ptr<FT_IteratorBase>
    RangeIteratorFactory<KeyT, ValueT>::createBaseIterator()
    {
        return std::make_unique<RT_RangeIterator<KeyT, ValueT>>(m_tree, m_range.m_min, m_range.m_min_inclusive,
            m_range.m_max, m_range.m_max_inclusive);
    }

    template <typename KeyT, typename ValueT> std::unique_ptr<FT_Iterator<ValueT> >
    RangeIteratorFactory<KeyT, ValueT>::createFTIterator()
    {        
        return std::make_unique<RT_FTIterator<KeyT, ValueT>>(m_tree, m_range.m_min, m_range.m_min_inclusive,
            m_range.m_max, m_range.m_max_inclusive);
    }
    
}
