// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/memory/Allocator.hpp>
#include <cassert>
#include <cstdint>

namespace db0

{

    class MS_Address
    {
    public:
        static constexpr std::uint64_t LOCAL_ADDRESS_BITS = 24;
        static constexpr std::uint64_t SLOT_ID_BITS = 40;
        static constexpr std::uint64_t LOCAL_ADDRESS_MASK = (1ull << LOCAL_ADDRESS_BITS) - 1;
        // the last valid slot ID is SLOT_ID_COUNT - 1, the slot ID of all 1s is reserved for invalid address
        static constexpr std::uint64_t SLOT_ID_COUNT = 1ull << SLOT_ID_BITS;
        
        static MS_Address &from(std::uint64_t &address);

        static const MS_Address &from(const std::uint64_t &address);

        // Encode as external address
        static std::uint64_t encode(Allocator::SlotId slot_id, std::uint64_t local_address);

        Allocator::SlotId slot_id() const;

        std::uint64_t local_address() const;
    private:
        std::uint64_t m_address;
    };

    inline MS_Address &MS_Address::from(std::uint64_t &address)
    {
        return reinterpret_cast<MS_Address &>(address);
    }

    inline const MS_Address &MS_Address::from(const std::uint64_t &address)
    {
        return reinterpret_cast<const MS_Address &>(address);
    }

    inline std::uint64_t MS_Address::encode(Allocator::SlotId slot_id, std::uint64_t local_address)
    {
        assert(slot_id < SLOT_ID_COUNT);
        assert((local_address & LOCAL_ADDRESS_MASK) == local_address);
        return (static_cast<std::uint64_t>(slot_id) << LOCAL_ADDRESS_BITS) | local_address;
    }

    inline Allocator::SlotId MS_Address::slot_id() const
    {
        return m_address >> LOCAL_ADDRESS_BITS;
    }
    
    inline std::uint64_t MS_Address::local_address() const
    {
        return m_address & LOCAL_ADDRESS_MASK;
    }

}
