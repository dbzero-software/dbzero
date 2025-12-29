// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Allocator.hpp"

namespace db0

{

    /**
     * The Allocator implementation that can only allocate a one specific address
    */
    class OneShotAllocator: public Allocator
    {
    public:
        OneShotAllocator(Address addr, std::size_t size);
        
        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;
        
        void commit() const override;

        void detach() const override;

    private:
        const Address m_addr;
        const std::size_t m_size;
        bool m_allocated = false;
    };

}
