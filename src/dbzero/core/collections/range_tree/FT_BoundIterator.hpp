// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "BlockItem.hpp"

namespace db0

{
    
    /**
     * FT_Iterator wrapper for key + value pairs with the range filtering capability
     * @tparam KeyT type of the RangeTree key
     * @tparam ValueT type of the RangeTree value
     * @tparam IndexT type of the underlying data collection
    */
    template <typename KeyT, typename ValueT, typename IndexT> class FT_BoundIterator:
    public FT_IndexIterator<IndexT, ValueT>
    {
    private:
        using self_t = FT_BoundIterator<KeyT, ValueT, IndexT>;
        using super_t = FT_IndexIterator<IndexT, ValueT>;

    public:
        using RangeT = RT_Range<KeyT>;
        FT_BoundIterator(const IndexT &index, int direction, const RangeT &key_range)
            : super_t(index, direction)
            , m_key_range(key_range)
        {
            fix(direction);
        }

		void operator++() override;

		void operator--() override;

		bool join(ValueT join_key, int direction) override;

		void joinBound(ValueT join_key) override;

		std::pair<ValueT, bool> peek(ValueT join_key) const override;		
		
        std::unique_ptr<FT_Iterator<ValueT> > beginTyped(int direction = -1) const override;
        
    private:
        const RangeT m_key_range;

        /**
         * Fix the current iterator's position such that is stays within the key range
         * Note that this operation may render the iterator invalid (end)
        */
        void fix(int direction);
        void fixInc();
        void fixDec();
    };

    template <typename KeyT, typename ValueT, typename IndexT>
    void FT_BoundIterator<KeyT, ValueT, IndexT>::fixInc()
    {        
        while (!super_t::isEnd() && !m_key_range.contains((*super_t::m_iterator).m_key)) {
            super_t::operator++();
        }
    }

    template <typename KeyT, typename ValueT, typename IndexT>
    void FT_BoundIterator<KeyT, ValueT, IndexT>::fixDec()
    {
        while (!super_t::isEnd() && !m_key_range.contains((*super_t::m_iterator).m_key)) {
            super_t::operator--();
        }
    }

    template <typename KeyT, typename ValueT, typename IndexT>
    void FT_BoundIterator<KeyT, ValueT, IndexT>::fix(int direction)
    {
        if (direction > 0) {
            fixInc();
        } else {
            fixDec();
        }
    }

    template <typename KeyT, typename ValueT, typename IndexT>
    void FT_BoundIterator<KeyT, ValueT, IndexT>::operator++()
    {
        assert(!super_t::isEnd());
        super_t::operator++();
        fixInc();
    }

    template <typename KeyT, typename ValueT, typename IndexT>
    void FT_BoundIterator<KeyT, ValueT, IndexT>::operator--()
    {
        assert(!super_t::isEnd());
        super_t::operator--();
        fixDec();
    }

    template <typename KeyT, typename ValueT, typename IndexT>
    bool FT_BoundIterator<KeyT, ValueT, IndexT>::join(ValueT join_key, int direction)
    {
        assert(!super_t::isEnd());
        if (!super_t::join(join_key, direction)) {
            return false;
        }

        fix(direction);
        return !super_t::isEnd();
    }

    template <typename KeyT, typename ValueT, typename IndexT>
    void FT_BoundIterator<KeyT, ValueT, IndexT>::joinBound(ValueT join_key)
    {
        super_t::joinBound(join_key);
        fix(super_t::m_direction);      
    }

    template <typename KeyT, typename ValueT, typename IndexT>
    std::pair<ValueT, bool> FT_BoundIterator<KeyT, ValueT, IndexT>::peek(ValueT join_key) const
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT, typename ValueT, typename IndexT>
    std::unique_ptr<FT_Iterator<ValueT> > FT_BoundIterator<KeyT, ValueT, IndexT>::beginTyped(int direction) const
    {
        return std::unique_ptr<FT_Iterator<ValueT> >(new self_t(super_t::m_data, direction, m_key_range));
    }
    
}