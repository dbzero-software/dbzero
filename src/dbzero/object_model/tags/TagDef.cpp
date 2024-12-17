#include "TagDef.hpp"

namespace db0::object_model

{
    
    TagDef::TagDef(std::uint64_t fixture_uuid, std::uint64_t value, ObjectPtr obj_ptr)
        : m_fixture_uuid(fixture_uuid)
        , m_value(value)
        , m_object(obj_ptr)
    {
    }
    
    TagDef &TagDef::makeNew(void *at_ptr, std::uint64_t fixture_uuid, std::uint64_t value, ObjectPtr obj_ptr) {
        return *new (at_ptr) TagDef(fixture_uuid, value, obj_ptr);
    }
    
}