// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/full_text/FT_IndexIterator.hpp>
#include "RT_Range.hpp"
#include "BlockItem.hpp"
#include "FT_BoundIterator.hpp"

namespace db0

{

    template <typename KeyT, typename ValueT> class RangeTreeBlock:
    public v_bindex<BlockItemT<KeyT, ValueT>, Address, typename BlockItemT<KeyT, ValueT>::CompT>
    {
        using super_t = v_bindex<BlockItemT<KeyT, ValueT>, Address, typename BlockItemT<KeyT, ValueT>::CompT>;
    public:
        using PtrT = db0_ptr<RangeTreeBlock<KeyT, ValueT>>;
        using ItemT = BlockItemT<KeyT, ValueT>;
        using RangeT = RT_Range<KeyT>;
        using IndexT = super_t;
        using FT_IteratorT = FT_IndexIterator<super_t, ValueT>;
        
        RangeTreeBlock() = default;
        RangeTreeBlock(Memspace &memspace)
            : super_t(memspace)
        {
        }

        RangeTreeBlock(mptr ptr)
            : super_t(ptr)
        {
        }

        ItemT front() const
        {
            assert(!this->empty());
            return *this->begin();
        }
        
        std::unique_ptr<FT_IteratorT> makeIterator() const {
            return std::make_unique<FT_IteratorT>(*this, -1);
        }
        
        // Construct iterator with an additonal range filtering
        std::unique_ptr<FT_IteratorT> makeIterator(const RangeT &key_range) const {
            return std::make_unique<FT_BoundIterator<KeyT, ValueT, super_t>>(*this, -1, key_range);
        }
    };
    
}
