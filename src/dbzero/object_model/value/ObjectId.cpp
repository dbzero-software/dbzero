#include "ObjectId.hpp"
#include <cstring>
#include <dbzero/core/utils/base32.hpp>

namespace db0::object_model

{
                    
    ObjectId ObjectId::fromBase32(const char *buf)
    {
        if (strlen(buf) != encodedSize()) {
            THROWF(db0::InputException) << "Invalid UUID";
        }

        // allocate +1 byte since decoded content might be up to 1 byte larger
        std::array<std::uint8_t, rawSize() + 1> bytes;
        if (db0::base32_decode(buf, bytes.data()) < rawSize()) {
            THROWF(db0::InputException) << "Invalid UUID";
        }
        
        std::uint8_t *ptr = bytes.data();
        ObjectId obj;
        obj.m_fixture_uuid = *reinterpret_cast<std::uint64_t*>(ptr);
        ptr += sizeof(obj.m_fixture_uuid);
        obj.m_typed_addr.m_value = *reinterpret_cast<std::uint64_t*>(ptr);
        ptr += sizeof(obj.m_typed_addr.m_value);
        obj.m_instance_id = *reinterpret_cast<std::uint16_t*>(ptr);
        return obj;
    }
    
    void ObjectId::toBase32(char *buffer)
    {        
        std::array<std::uint8_t, rawSize()> bytes;
        std::memset(bytes.data(), 0, bytes.size());
        std::uint8_t *ptr = bytes.data();
        *reinterpret_cast<std::uint64_t*>(ptr) = m_fixture_uuid;
        ptr += sizeof(m_fixture_uuid);
        *reinterpret_cast<std::uint64_t*>(ptr) = m_typed_addr.m_value;
        ptr += sizeof(m_typed_addr.m_value);
        *reinterpret_cast<std::uint16_t*>(ptr) = m_instance_id;
        db0::base32_encode(bytes.data(), bytes.size(), buffer);
    }
    
    bool ObjectId::operator==(const ObjectId &other) const
    {        
        return (m_fixture_uuid == other.m_fixture_uuid) && (m_typed_addr == other.m_typed_addr) 
            && (m_instance_id == other.m_instance_id);
    }

    bool ObjectId::operator!=(const ObjectId &other) const {
        return !(*this == other);
    }

    bool ObjectId::operator<(const ObjectId &other) const
    {
        return m_fixture_uuid < other.m_fixture_uuid || (m_fixture_uuid == other.m_fixture_uuid && m_typed_addr < other.m_typed_addr) || 
            (m_fixture_uuid == other.m_fixture_uuid && m_typed_addr == other.m_typed_addr && m_instance_id < other.m_instance_id);
    }
    
    bool ObjectId::operator<=(const ObjectId &other) const {
        return *this < other || *this == other;
    }

    bool ObjectId::operator>(const ObjectId &other) const {
        return !(*this <= other);
    }

    bool ObjectId::operator>=(const ObjectId &other) const {
        return !(*this < other);
    }

}
