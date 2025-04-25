#pragma once

#include "StorageClass.hpp"
#include "TypedAddress.hpp"
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    struct ObjectId
    {    
        std::uint64_t m_fixture_uuid;
        // NOTE: unique address combines memory offset + instance ID
        db0::UniqueAddress m_address;
        db0::StorageClass m_storage_class;
        
        // encodes with base-32 characters (no format prefix / suffix)
        // the buffer must be at least 'encodedSize' + 1 bytes long
        void toBase32(char *buf) const;
        static ObjectId fromBase32(const char *buf);
        
        bool operator==(const ObjectId &other) const;

        bool operator!=(const ObjectId &other) const;

        bool operator<(const ObjectId &other) const;

        bool operator<=(const ObjectId &other) const;

        bool operator>(const ObjectId &other) const;

        bool operator>=(const ObjectId &other) const;

        static constexpr std::size_t rawSize() {
            return sizeof(m_fixture_uuid) + sizeof(m_address) + sizeof(m_storage_class);
        }
        
        static constexpr std::size_t encodedSize() {
            return (rawSize() * 8 - 1) / 5 + 1;
        }

        std::string toUUIDString() const;
    };

}

