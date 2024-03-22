#pragma once

#include "StorageClass.hpp"
#include "TypedAddress.hpp"
#include <dbzero/core/utils/FlagSet.hpp>

namespace db0::object_model

{

    enum class UUID_Options : std::uint8_t
    {
        // durable flag indicates that when creating UUID the ref-count was incremented
        durable = 0b00000001
    };

    struct ObjectId
    {                        
        std::uint64_t m_fixture_uuid;
        TypedAddress m_typed_addr;
        std::uint32_t m_instance_id;
        db0::FlagSet<UUID_Options> m_flags;
        
        // buf must be at least 46 bytes long
        void formatHex(char *buf);
        
        bool operator==(const ObjectId &other) const;

        bool operator!=(const ObjectId &other) const;

        bool operator<(const ObjectId &other) const;

        bool operator<=(const ObjectId &other) const;

        bool operator>(const ObjectId &other) const;

        bool operator>=(const ObjectId &other) const;
    };

}

DECLARE_ENUM_VALUES(db0::object_model::UUID_Options, 1)
