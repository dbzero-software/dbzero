#pragma once

#include "StorageClass.hpp"
#include "TypedAddress.hpp"
#include <dbzero/core/utils/FlagSet.hpp>

namespace db0::object_model

{

    struct ObjectId
    {    
        std::uint64_t m_fixture_uuid;
        TypedAddress m_typed_addr;
        std::uint32_t m_instance_id;
        
        // encodes with base-32 characters (no format prefix / suffix)
        // the buffer must be at least 'encodedSize' + 1 bytes long
        void formatBase32(char *buf);
        static ObjectId fromBase32(const char *buf);
                
        bool operator==(const ObjectId &other) const;

        bool operator!=(const ObjectId &other) const;

        bool operator<(const ObjectId &other) const;

        bool operator<=(const ObjectId &other) const;

        bool operator>(const ObjectId &other) const;

        bool operator>=(const ObjectId &other) const;

        static constexpr std::size_t rawSize() {
            return sizeof(m_fixture_uuid) + sizeof(m_typed_addr) + sizeof(m_instance_id);
        }

        static constexpr std::size_t encodedSize() {
            return (rawSize() * 8 - 1) / 5 + 1;
        }
    };

}

