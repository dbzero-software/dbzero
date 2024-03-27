#include "ObjectId.hpp"
#include <cstring>
#include <dbzero/core/utils/base32.hpp>

namespace db0::object_model

{
        
    static constexpr std::size_t rawBufSize()
    {
        // aligned to 64 bits
        auto len = ObjectId::rawSize();
        if (len % 8 != 0) {
            len += 8 - len % 8;
        }
        return len;
    }
    
    static constexpr const char *base_32_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        
    ObjectId ObjectId::fromBase32(const char *buf)
    {
        throw std::runtime_error("Not implemented");
    }

    void ObjectId::formatBase32(char *buffer)
    {        
        std::array<std::uint8_t, rawBufSize()> bytes;
        std::memset(bytes.data(), 0, bytes.size());
        std::uint8_t *ptr = bytes.data();
        *reinterpret_cast<std::uint64_t*>(ptr) = m_fixture_uuid;
        ptr += sizeof(m_fixture_uuid);
        *reinterpret_cast<std::uint64_t*>(ptr) = m_typed_addr.m_value;
        ptr += sizeof(m_typed_addr.m_value);
        *reinterpret_cast<std::uint32_t*>(ptr) = m_instance_id;
        
        // process 5 bytes at a time (8 characters)
        auto end = buffer + encodedSize();
        for (unsigned int i = 0; i < sizeof(bytes); i += 5) {
            auto value = *reinterpret_cast<std::uint64_t*>(bytes.data() + i);
            for (unsigned int j = 0; j < 8 && buffer < end; ++j, value >>= 6) {
                *buffer++ = base_32_chars[value & 0x1F];
            }
        }
        // null-terminate
        *buffer = 0;
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
