#include "ObjectId.hpp"
#include <cstring>
#include <dbzero/core/utils/base32.hpp>

namespace db0::object_model

{
                    
    ObjectId ObjectId::fromBase32(const char *buf)
    {
        if (strlen(buf) != encodedSize()) {
            THROWF(db0::InputException) << "UUID string is not valid: " << buf;
        }

        // allocate +1 byte since decoded content might be up to 1 byte larger
        std::array<std::uint8_t, rawSize() + 1> bytes;
        if (db0::base32_decode(buf, bytes.data()) < rawSize()) {
            THROWF(db0::InputException) << "UUID string is not valid (failed to decode): " << buf;
        }
        
        std::uint8_t *ptr = bytes.data();
        ObjectId obj;
        obj.m_fixture_uuid = *reinterpret_cast<std::uint64_t*>(ptr);
        ptr += sizeof(obj.m_fixture_uuid);
        obj.m_address = *reinterpret_cast<db0::UniqueAddress*>(ptr);
        ptr += sizeof(obj.m_address);
        obj.m_storage_class = *reinterpret_cast<db0::StorageClass*>(ptr);
        return obj;
    }
    
    void ObjectId::toBase32(char *buffer) const
    {        
        std::array<std::uint8_t, rawSize()> bytes;
        std::memset(bytes.data(), 0, bytes.size());
        std::uint8_t *ptr = bytes.data();
        *reinterpret_cast<std::uint64_t*>(ptr) = m_fixture_uuid;
        ptr += sizeof(m_fixture_uuid);
        *reinterpret_cast<db0::UniqueAddress*>(ptr) = m_address;
        ptr += sizeof(m_address);
        *reinterpret_cast<db0::StorageClass*>(ptr) = m_storage_class;
        db0::base32_encode(bytes.data(), bytes.size(), buffer);
    }
    
    bool ObjectId::operator==(const ObjectId &other) const
    {        
        return (m_fixture_uuid == other.m_fixture_uuid) && (m_address == other.m_address) 
            && (m_storage_class == other.m_storage_class);
    }

    bool ObjectId::operator!=(const ObjectId &other) const {
        return !(*this == other);
    }

    bool ObjectId::operator<(const ObjectId &other) const
    {
        return m_fixture_uuid < other.m_fixture_uuid || (m_fixture_uuid == other.m_fixture_uuid && m_address < other.m_address) || 
            (m_fixture_uuid == other.m_fixture_uuid && m_address == other.m_address && m_storage_class < other.m_storage_class);
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
    
    std::string ObjectId::toUUIDString() const
    {
        char buffer[encodedSize() + 1];
        toBase32(buffer);
        return std::string(buffer);
    }

}
