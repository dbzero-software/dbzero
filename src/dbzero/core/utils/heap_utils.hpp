// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once 

/**
 *  Heap algorithms, compatible with STL-style heap
 */

#include <iterator>

namespace db0

{

    /**
     *  Update for change in front element.
     *  @Note This operation has cost O(log n). No rebalancing happens.
     */
    template<typename RandomIt, typename Compare = std::less<typename std::iterator_traits<RandomIt>::value_type>>
    void update_heap_front(RandomIt first, RandomIt last, Compare comp = Compare())
    {
        std::size_t size = std::distance(first, last);
        if(size < 2) {
            return;
        }
        std::size_t cpos = 0;
        std::size_t lpos = 1;
        std::size_t rpos = 2;
        std::size_t npos;
        RandomIt curr, next;
        do {
            if(rpos >= size) {
                rpos = lpos;
            }
            if(comp(*(first + lpos), *(first + rpos))) {
                npos = rpos;
            } else {
                npos = lpos;
            }
            curr = first + cpos;
            next = first + npos;
            if(comp(*curr, *next)) {
                std::swap(*curr, *next);
                cpos = npos;
            } else {
                break;
            }
            rpos = (cpos + 1) << 1;
            lpos = rpos - 1;
        }
        while(lpos < size);
    }
    
}