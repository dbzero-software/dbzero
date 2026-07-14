// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Slice.hpp"
#include <limits>
#include <cassert>

namespace db0::object_model

{

    bool SliceDef::isDefault() const {
        return *this == SliceDef();
    }
    
    SliceDef SliceDef::combineWith(const SliceDef &other) const
    {
        if (other.isDefault()) {
            return *this;
        }
        if (isDefault()) {
            return other;
        }
        // multiple slicing is not supported
        THROWF(db0::InputException) 
            << "Cannot slice an already sliced iterable (Operation not supported)" << THROWF_END;
    }
    
    Slice::Slice(BaseIterator *base_iterator, const SliceDef &slice_def)
        : m_slice_def(slice_def)
        , m_iterator_ptr(base_iterator)        
    {        
        assert(m_slice_def.m_step > 0);
        if (m_slice_def.m_start > 0 && m_iterator_ptr && !m_iterator_ptr->isEnd()) {
            m_iterator_ptr->skip(m_slice_def.m_start);
            m_pos += m_slice_def.m_start;
            if (m_pos >= m_slice_def.m_stop) {
                m_iterator_ptr = nullptr;
            }
        }    
    }
    
    bool Slice::isEnd() const {
        return !m_iterator_ptr || m_iterator_ptr->isEnd();
    }
    
    void Slice::next(void *buf)
    {
        assert(!isEnd());
        m_iterator_ptr->next(buf);
        if (m_slice_def.m_step == 1) {
            ++m_pos;
        } else {
            m_iterator_ptr->skip(m_slice_def.m_step - 1);
            m_pos += m_slice_def.m_step;
        }
        if (m_pos >= m_slice_def.m_stop) {
            m_iterator_ptr = nullptr;
        }
    }
    
}