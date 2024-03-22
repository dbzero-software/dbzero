#include "Field.hpp"

namespace db0 { namespace object_model

{

    o_field::o_field(RC_LimitedStringPool &string_pool, const char *name, StorageClass storage_class, ClassPtr class_ptr)
        : m_name(string_pool.add(name))
        , m_storage_class(storage_class)
        , m_class_ptr(class_ptr)
    {
    }

} }
