#include "ObjectId.hpp"
#include <cstring>

DEFINE_ENUM_VALUES(db0::object_model::UUID_Options, "durable")

namespace db0::object_model

{

    void ObjectId::formatHex(char *buffer)
    {       
        sprintf(buffer, "0x%02x%016lx%016lx%08x", m_flags.value(), m_fixture_uuid, m_typed_addr.m_value, m_instance_id);
    }
    
    bool ObjectId::operator==(const ObjectId &other) const
    {        
        return (m_fixture_uuid == other.m_fixture_uuid) && (m_typed_addr == other.m_typed_addr) 
            && (m_instance_id == other.m_instance_id);
    }

    bool ObjectId::operator!=(const ObjectId &other) const
    {
        return !(*this == other);
    }

    bool ObjectId::operator<(const ObjectId &other) const
    {
        return m_fixture_uuid < other.m_fixture_uuid || (m_fixture_uuid == other.m_fixture_uuid && m_typed_addr < other.m_typed_addr) || 
            (m_fixture_uuid == other.m_fixture_uuid && m_typed_addr == other.m_typed_addr && m_instance_id < other.m_instance_id);
    }
    
    bool ObjectId::operator<=(const ObjectId &other) const
    {
        return *this < other || *this == other;
    }

    bool ObjectId::operator>(const ObjectId &other) const
    {
        return !(*this <= other);
    }

    bool ObjectId::operator>=(const ObjectId &other) const
    {
        return !(*this < other);
    }

}
