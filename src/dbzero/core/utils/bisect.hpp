// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "operators.hpp"

namespace db0::bisect 

{
    
    template <typename random_iterator, typename KeyT, typename CompT, typename op = Operators<random_iterator> >
    random_iterator lower_equal(random_iterator begin, random_iterator end, const KeyT &key, CompT less)
    {
        if (begin == end) {
            return end;
        }
        auto low = begin, high = end;
        // NOTE: high is past-the-end element
        while (op::sub(high, low) > 1) {
            auto mid = op::add(low, (op::sub(high, low) >> 1));
            if (less(key, *mid)) {
                high = mid;
            } else if (less(*mid, key)) {
                low = mid;
            } else {
                // key matched
                return mid;
            }
        }
        if (less(key, *low)) {
            return end;
        }
        return low;
    }
    
    template <typename random_iterator, typename KeyT, typename CompT>
    random_iterator rlower_equal(random_iterator begin, random_iterator end, const KeyT &key, CompT less)
    {
        using op = ReverseOperators<random_iterator>;
        return lower_equal<random_iterator, KeyT, CompT, op>(begin, end, key, less);
    }

    template <typename random_iterator, typename KeyT, typename CompT, typename op = Operators<random_iterator> >
    random_iterator upper_equal(random_iterator begin, random_iterator end, const KeyT &key, CompT less)
    {
        if (begin == end) {
            return end;
        }
        auto low = begin, high = end;
        while (op::sub(high, low) > 1) {
            auto mid = op::add(low, (op::sub(high, low) >> 1));
            if (less(*mid, key)) {
                low = mid;
            } else if (less(key, *mid)) {
                high = mid;
            } else {
                // key matched
                return mid;
            }
        }
        if (less(*low, key)) {
            low = op::add(low, 1);
        }
        assert(low == end || !less(*low, key));
        return low;
    }
    
    template <typename random_iterator, typename KeyT, typename CompT>
    random_iterator rupper_equal(random_iterator begin, random_iterator end, const KeyT &key, CompT less)
    {
        using op = ReverseOperators<random_iterator>;
        return upper_equal<random_iterator, KeyT, CompT, op>(begin, end, key, less);
    }

}
