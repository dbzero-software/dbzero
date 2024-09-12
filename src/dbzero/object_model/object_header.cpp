#include "object_header.hpp"
#include <dbzero/core/vspace/v_object.hpp>
#include <limits>

namespace db0

{

    void o_object_header::incRef()
    {
        if (m_ref_count == std::numeric_limits<std::uint32_t>::max()) {
            THROWF(db0::InternalException) << "Too many references";
        }
        ++m_ref_count;
    }

    bool o_object_header::decRef()
    {
        assert(m_ref_count > 0);
        return --m_ref_count == 0;
    }
    
}