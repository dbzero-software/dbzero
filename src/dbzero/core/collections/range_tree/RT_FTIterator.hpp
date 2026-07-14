// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "RangeTree.hpp"
#include "RT_Range.hpp"
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/FT_ORXIterator.hpp>

namespace db0

{

    /**
     * The RT_FTIterator implements a range subquery over the RangeTree index
     * It implements the full FT_Iterator interface and thefore can be joined with other FT-iterators
     * It might be slower than the RT_RageIterator for very large collections but is more versatile
     * @tparam KeyT type of the RangeTree key
     * @tparam ValueT type of the RangeTree value
    */
    template <typename KeyT, typename ValueT> class RT_FTIterator: public FT_JoinORXIterator<ValueT>
    {
        using RT_TreeT = RangeTree<KeyT, ValueT>;
        using self_t = RT_FTIterator<KeyT, ValueT>;
        using super_t = FT_JoinORXIterator<ValueT>;
    public:
        // Create to range-filter results of a specific FT-iterator (e.g. tag query)
        RT_FTIterator(const IndexBase &index, SharedPtrWrapper<RT_TreeT> tree_ptr, std::optional<KeyT> min,
            bool min_inclusive, std::optional<KeyT> max, bool max_inclusive, bool null_first)
            : super_t(makeQuery(tree_ptr, min, min_inclusive, max, max_inclusive, null_first), -1, false)
            , m_fixture_uuid(index.getMemspace().getUUID())
            , m_index_addr(index.getAddress())
            , m_range { min, min_inclusive, max, max_inclusive }
        {
        }
        
        FTIteratorType getSerialTypeId() const override;
        
        void serialize(std::vector<std::byte> &) const override;
        
    private:
        const std::uint64_t m_fixture_uuid = 0;
        const Address m_index_addr = {};
        const RT_Range<KeyT> m_range;

        std::list<std::unique_ptr<FT_Iterator<ValueT> > > makeQuery(SharedPtrWrapper<RT_TreeT> tree_ptr, std::optional<KeyT> min,
            bool min_inclusive, std::optional<KeyT> max, bool max_inclusive, bool null_first) const;
    };
    
    template <typename KeyT, typename ValueT>
    std::list<std::unique_ptr<FT_Iterator<ValueT> > > RT_FTIterator<KeyT, ValueT>::makeQuery(SharedPtrWrapper<RT_TreeT> tree_ptr,
        std::optional<KeyT> min, bool min_inclusive, std::optional<KeyT> max, bool max_inclusive, bool null_first) const
    {
        auto fullInclusion = [&](KeyT r_min, KeyT r_max) -> bool {
            if (min && (r_min < *min)) {
                return false;
            }
            if (max && r_max > *max) {
                return false;
            }
            return true;
        };

        auto nullInclusion = [&]() -> bool {            
            return (null_first && !min) || (!null_first && !max);
        };
        
        // underlying index is empty
        if (!tree_ptr) {
            return {};
        }

        std::list<std::unique_ptr<FT_Iterator<ValueT> > > query;
        auto it = [&]() { if (min) return tree_ptr->lowerBound(*min, min_inclusive); else return tree_ptr->beginRange(); }();
        while (!it.isEnd()) {
            auto bounds = it.getKeyRange();
            if (bounds.first && bounds.second && fullInclusion(*bounds.first, *bounds.second)) {
                // take full iterator                
                query.push_back(it->makeIterator());                
            } else {
                // take sub-range iterator
                query.push_back(it->makeIterator({min, min_inclusive, max, max_inclusive}));
            }
            it.next();
        }

        if (nullInclusion()) {
            auto null_block_ptr = tree_ptr->getNullBlock();
            if (null_block_ptr) {
                // add null iterator
                query.push_back(null_block_ptr->makeIterator());
            }
        }

        return query;
    }
    
    template <typename KeyT, typename ValueT>
    FTIteratorType RT_FTIterator<KeyT, ValueT>::getSerialTypeId() const {
        return FTIteratorType::RangeTree;
    }

    template <typename KeyT, typename ValueT>
    void RT_FTIterator<KeyT, ValueT>::serialize(std::vector<std::byte> &v) const
    {
        db0::serial::write(v, db0::serial::typeId<KeyT>());
        db0::serial::write(v, db0::serial::typeId<ValueT>());
        db0::serial::write(v, m_fixture_uuid);
        db0::serial::write(v, m_index_addr);
        m_range.serialize(v);
    }
    
}