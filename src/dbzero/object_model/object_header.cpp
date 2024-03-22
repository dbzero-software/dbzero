#include "object_header.hpp"
#include <dbzero/core/vspace/v_object.hpp>

namespace db0

{

    void o_object_header::incRef()
    {
        ++m_ref_count;
    }

    bool o_object_header::decRef()
    {
        assert(m_ref_count > 0);
        return --m_ref_count == 0;
    }
    
}