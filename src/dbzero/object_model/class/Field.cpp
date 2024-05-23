#include "Field.hpp"

namespace db0::object_model

{

    o_field::o_field(RC_LimitedStringPool &string_pool, const char *name)
        : m_name(string_pool.add(name))
    {
    }
    
}
