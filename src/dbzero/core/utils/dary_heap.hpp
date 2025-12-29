// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

/// This package contains a D-ary heap routines
#pragma once

#include "operators.hpp"
#include <algorithm>

namespace db0::dheap

{
        
    /**
     * Push the (end - 1) element into the D-ary max heap
     * 
     * @return the position of the pushed element after fixing the heap
     */
    template <int D, typename random_iterator, typename CompT, typename op = Operators<random_iterator> >
    random_iterator push(random_iterator begin, random_iterator end, CompT less)
    {
        // FIXME: for now only works for D = 2 (binary)
        static_assert(D == 2);
        if (end == begin) {
            return begin;
        }
        auto item = op::add(end, -1);
        while (item != begin) {
            auto parent = op::add(begin, ((op::sub(item, begin) - 1) >> 1));
            if (less(*parent, *item)) {
                std::swap(*parent, *item);
                item = parent;
            } else {                
                return item;
            }
        } 
        return begin;    
    }

    /**
     * Moves the largest to the end (max heap)
    */
    template <int D, typename random_iterator, typename CompT, typename op = Operators<random_iterator> > 
    void pop(random_iterator begin, random_iterator end, CompT less)
    {
        // FIXME: for now only works for D = 2 (binary)
        static_assert(D == 2);
        std::pop_heap(begin, end, less);
    }

    /**
     * Finds the min element in the D-ary max heap
    */
    template <int D, typename random_iterator, typename CompT, typename op = Operators<random_iterator> > 
    random_iterator find_min(random_iterator begin, random_iterator end, CompT less)
    {
        assert(end != begin);
        // it's sufficient to check the leaf nodes only
        auto leaf_count = (op::sub(end, begin) + 1) >> 1;
        auto min_item = op::add(end, -1);
        --leaf_count;
        for (auto item = op::add(min_item, -1); leaf_count > 0; op::prev(item), --leaf_count) {
            if (less(*item, *min_item)) {
                min_item = item;
            }            
        }
        assert(end != min_item);
        return min_item;
    }
    
    /**
     * Find element in the D-ary max heap (use linear search)
     * 
     * @return element's iterator or npos (nullptr by default) if not found
    */
    template <int D, typename random_iterator, typename KeyT, typename EqualT, typename op = Operators<random_iterator> > 
    random_iterator find(random_iterator begin, random_iterator end, const KeyT &key, EqualT equal, random_iterator npos = nullptr)
    {
        for (auto item = begin; item != end; op::next(item)) {
            if (equal(*item, key)) {
                return item;
            }
        }
        return npos;
    }
    
    template <int D, typename random_iterator, typename CompT, typename op = Operators<random_iterator> > 
    void fixheap_down(random_iterator begin, random_iterator elem, random_iterator end, CompT less)
    {
        if (op::sub(end, begin) < 2) {
            return;
        }

        for (;;) {
            auto max_elem = elem;
            // left child
            auto l = op::add(begin, (op::sub(elem, begin) << 1) + 1);
            // right child
            auto r = op::add(l, 1);
            if (op::sub(end, l) > 0 && less(*elem, *l))
                max_elem = l;
            if (op::sub(end, r) > 0 && less(*max_elem, *r))
                max_elem = r;
            if (max_elem == elem)
                return;
            std::swap(*elem, *max_elem);
            elem = max_elem;
        }
    }

    /**
     * Remove specific element from the D-ary max heap
    */
    template <int D, typename random_iterator, typename CompT, typename op = Operators<random_iterator> > 
    void erase(random_iterator begin, random_iterator end, random_iterator elem, CompT less)
    {
        // FIXME: for now only works for D = 2 (binary)
        static_assert(D == 2);
        assert(op::sub(elem, begin) >= 0 && op::sub(end, elem) > 0);
        op::prev(end);
        if (end == begin) {
            return;
        }
        bool is_gt = less(*elem, *end);
        *elem = *end;
        if (is_gt) {
            push<D, random_iterator, CompT, op>(begin, op::add(elem, 1), less);
        } else {
            fixheap_down<D, random_iterator, CompT, op>(begin, elem, end, less);
        }
    }

    template <int D, typename random_iterator, typename CompT, typename op = Operators<random_iterator> >
    void sort(random_iterator begin, random_iterator end, random_iterator out, CompT less) {
        if (op::sub(end, begin) < 2) {
            return;
        }
        if (out == end) {
            op::prev(end);
            while (begin != end) {
                std::swap(*begin, *end);
                fixheap_down<D, random_iterator, CompT, op>(begin, begin, end, less);
                op::prev(end);
            }
        } else {
            op::prev(end);
            op::prev(out);
            for (;;) {
                *out = *begin;
                *begin = *end;
                fixheap_down<D, random_iterator, CompT, op>(begin, begin, end, less);
                op::prev(end);
                op::prev(out);
                if (end == begin) {
                    *out = *begin;
                    break;    
                }
            }
        }
    }
    
    // Functions using ReversedOperators as default
      template <int D, typename random_iterator, typename CompT, typename op = ReverseOperators<random_iterator> >
    random_iterator rpush(random_iterator begin, random_iterator end, CompT less) {
        return push<D, random_iterator, CompT, op>(begin, end, less);
    }

    template <int D, typename random_iterator, typename CompT, typename op = ReverseOperators<random_iterator> > 
    random_iterator rfind_min(random_iterator begin, random_iterator end, CompT less) {
        return find_min<D, random_iterator, CompT, op>(begin, end, less);
    }

    template <int D, typename random_iterator, typename KeyT, typename EqualT, typename op = ReverseOperators<random_iterator> > 
    random_iterator rfind(random_iterator begin, random_iterator end, const KeyT &key, EqualT equal, random_iterator npos = nullptr)
    {
        return find<D, random_iterator, KeyT, EqualT, op>(begin, end, key, equal, npos);
    }

    template <int D, typename random_iterator, typename CompT, typename op = ReverseOperators<random_iterator> > 
    void rerase(random_iterator begin, random_iterator end, random_iterator elem, CompT less) {
        erase<D, random_iterator, CompT, op>(begin, end, elem, less);
    }

    template <int D, typename random_iterator, typename CompT, typename op = ReverseOperators<random_iterator> >
    void rsort(random_iterator begin, random_iterator end, random_iterator out, CompT less) {
        sort<D, random_iterator, CompT, op>(begin, end, out, less);
    }
    
}