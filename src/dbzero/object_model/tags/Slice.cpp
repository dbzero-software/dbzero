#include "Slice.hpp"
#include <limits>
#include <cassert>

namespace db0::object_model

{

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
        /* FIXME:
        if (m_slice_def.m_step == 1) {
            ++m_pos;
        } else {
            m_iterator_ptr->skip(m_slice_def.m_step - 1);
            m_pos += m_slice_def.m_step;
        }
        if (m_pos >= m_slice_def.m_stop) {
            m_iterator_ptr = nullptr;
        }
        */
    }
    
}