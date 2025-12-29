// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectId.hpp"
#include <cstring>
#include <dbzero/core/utils/base32.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/serialization/packed_int.hpp>

namespace db0::object_model

{
    
    std::function<void()> ObjectId::m_throw_func = []() {
        THROWF(db0::InputException) << "Invalid UUID";
    };
    
    bool ObjectId::operator!() const {
        return !m_fixture_uuid || !m_address.isValid() || m_storage_class == db0::StorageClass::UNDEFINED;
    }
    
    ObjectId ObjectId::tryFromBase32(const char *buf)
    {        
        if (strlen(buf) > maxEncodedSize()) {
            return {};
        }

        // allocate +1 byte since decoded content might be up to 1 byte larger
        std::array<std::byte, maxSize() + 1> bytes;
        auto size = db0::base32_decode(buf, reinterpret_cast<std::uint8_t*>(bytes.data()));   
        if (size < minSize()) {
            return {};
        }
        
        auto at = bytes.data(), end = bytes.data() + size;
        // read with bounds validation        
        auto fixture_uuid = db0::serial::readSimple<std::uint64_t>(at, end, m_throw_func); 
        auto address = UniqueAddress::fromValue(db0::serial::read<packed_int64>(at, end, m_throw_func));
        auto storage_class = db0::serial::read<db0::packed_int32>(at, end, m_throw_func).value();
        // NOTE: due to encoding we may be left with 1 extra byte
        if ((end - at) > 1) {
            return {};
        }
        return { fixture_uuid, address, static_cast<db0::StorageClass>(storage_class) };
    }

    ObjectId ObjectId::fromBase32(const char *buf)
    {        
        auto result = tryFromBase32(buf);
        if (!result) {
            THROWF(db0::InputException) << "Invalid UUID: " << buf;
        }
        return result;    
    }

    std::size_t ObjectId::toBase32(char *buffer) const
    {
        std::array<std::byte, maxSize()> bytes;
        std::memset(bytes.data(), 0, bytes.size());
        auto at = bytes.data();
        db0::serial::writeSimple(at, m_fixture_uuid);
        db0::serial::write<db0::packed_int64>(at, m_address.getValue());        
        // NOTICE: store as packed int to allow more bytes in the future if needed
        db0::serial::write<db0::packed_int32>(at, static_cast<std::uint32_t>(m_storage_class));
        // encode actual bytes
        return db0::base32_encode(reinterpret_cast<const std::uint8_t*>(bytes.data()), at - bytes.data(), buffer);
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
        char buffer[maxEncodedSize() + 1];
        auto size = toBase32(buffer);
        return std::string(buffer, size);
    }
    
}
