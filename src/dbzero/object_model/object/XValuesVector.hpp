// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/value/XValue.hpp>

namespace db0::object_model

{       

    /**
     * The XValuesVector organizes XValue elements for a temporary use by the ObjectInitializer
     * It's also capable of sorting and compacting lo-fi types
     */
    class XValuesVector: public std::vector<XValue>
    {
    public:
        XValuesVector(unsigned int sort_threshold = 32);
        
        // @param value - xvalue to add
        // @param mask - bitmask indicating which parts of the value are valid (relevant for lo-fi types)
        void push_back(const XValue &xvalue, std::uint64_t mask = 0);
        
        // Try pulling an existing initialization value from under a specific index
        bool tryGetAt(unsigned int at, std::pair<StorageClass, Value> &) const;
        // Remove all entries under the specified index, use mask for lo-fi types
        bool remove(unsigned int at, std::uint64_t mask = 0);
        void sortAndMerge();
        
        void clear();
        
    private:
        const unsigned int m_sort_threshold;
        // NOTE: masks only apply to the unsorted part of the vector
        std::vector<std::uint64_t> m_masks;
        unsigned int m_sorted_size = 0;

        // sort, deduplicate & merge values
        void sortValues();
    };

}   