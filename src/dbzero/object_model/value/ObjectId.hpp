// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "StorageClass.hpp"
#include "TypedAddress.hpp"
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{

    class ObjectId
    {    
    public:
        std::uint64_t m_fixture_uuid = 0;
        // NOTE: unique address combines memory offset + instance ID
        db0::UniqueAddress m_address;
        // NOTE: encoded storage class member's size may vary between 1 and 5 bytes
        db0::StorageClass m_storage_class = db0::StorageClass::UNDEFINED;
        
        // encodes with base-32 characters (no format prefix / suffix)
        // the buffer must be at least 'maxEncodedSize' + 1 bytes long
        std::size_t toBase32(char *buf) const;
        
        static ObjectId tryFromBase32(const char *buf);
        static ObjectId fromBase32(const char *buf);
        
        bool operator==(const ObjectId &other) const;

        bool operator!=(const ObjectId &other) const;

        bool operator<(const ObjectId &other) const;

        bool operator<=(const ObjectId &other) const;

        bool operator>(const ObjectId &other) const;

        bool operator>=(const ObjectId &other) const;

        static constexpr std::size_t minSize() {
            return sizeof(m_fixture_uuid) + 2;
        }

        static constexpr std::size_t maxSize() {
            return sizeof(m_fixture_uuid) + sizeof(m_address) + sizeof(std::uint32_t) + 2;
        }

        static constexpr std::size_t maxEncodedSize() {
            return (maxSize() * 8 - 1) / 5 + 1;
        }
        
        std::string toUUIDString() const;

        bool operator!() const;

    private:
        static std::function<void()> m_throw_func;
    };
    
}

