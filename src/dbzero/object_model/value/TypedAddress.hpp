#pragma once

#include <cstdint>
#include <cassert>
#include <dbzero/core/memory/Allocator.hpp>
#include "StorageClass.hpp"

namespace db0::object_model

{

    // A struct that combines StorageClass (14bit) + address (50bits) in a single 64bit value
    struct [[gnu::packed]] TypedAddress
    {
        std::uint64_t m_value;

        TypedAddress() = default;

        inline TypedAddress(std::uint64_t value)
            : m_value(value)
        {
        }
        
        inline TypedAddress(StorageClass type, std::uint64_t address)
            : m_value((static_cast<std::uint64_t>(type) << 50) | db0::getPhysicalAddress(address))
        {            
        }

        inline StorageClass getType() const {
            return static_cast<StorageClass>(m_value >> 50);
        }

        // NOTE: physical address is returned
        inline std::uint64_t getAddress() const {
            return db0::getPhysicalAddress(m_value);
        }
        
        // Cast operator (to physical address)
        inline operator std::uint64_t() const {
            return getAddress();
        }

        void setAddress(std::uint64_t address);
        void setType(StorageClass type);

        bool operator==(const TypedAddress &other) const;
        bool operator<(const TypedAddress &other) const;
    };
    
}