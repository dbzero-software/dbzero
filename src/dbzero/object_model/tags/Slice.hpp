// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>

namespace db0::object_model

{
    
    // The slice definition
    struct SliceDef
    {
        const std::size_t m_start = 0;
        const std::size_t m_stop = MAX_STOP();
        const int m_step = 1;
        
        bool isDefault() const;
        
        bool operator==(const SliceDef &other) const {
            return m_start == other.m_start && m_stop == other.m_stop && m_step == other.m_step;
        }

        static constexpr std::size_t MAX_STOP() {
            return std::numeric_limits<std::size_t>::max();
        }
        
        SliceDef combineWith(const SliceDef &other) const;
    };
    
    class Slice
    {
    public:
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;
                
        Slice(BaseIterator *, const SliceDef & = {});

        bool isEnd() const;
        void next(void *buf = nullptr);

    private:
        const SliceDef m_slice_def;
        BaseIterator *m_iterator_ptr;
        // current postion in the iterator
        std::size_t m_pos = 0;
    };

}