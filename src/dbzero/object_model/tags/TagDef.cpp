#include "TagDef.hpp"

namespace db0::object_model

{
    
    TagDef::TagDef(std::uint64_t fixture_uuid, Address address, ObjectPtr obj_ptr)
        : m_fixture_uuid(fixture_uuid)
        , m_address(address)
        , m_object(obj_ptr)
    {
    }
    
    TagDef &TagDef::makeNew(void *at_ptr, std::uint64_t fixture_uuid, Address address, ObjectPtr obj_ptr) {
        return *new (at_ptr) TagDef(fixture_uuid, address, obj_ptr);
    }

    bool TagDef::operator==(const TagDef &other) const {
        return this->m_address == other.m_address && this->m_fixture_uuid == other.m_fixture_uuid;
    }

    bool TagDef::operator!=(const TagDef &other) const {
        return !(*this == other);
    }
    
}
