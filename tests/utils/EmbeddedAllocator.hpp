// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <functional>
#include <optional>
#include <dbzero/core/memory/Allocator.hpp>

namespace db0

{

    /**
     * EmbeddedAllocator implementation for testing purposes.
    */
    class EmbeddedAllocator: public Allocator
    {
    public:
        using AllocCallbackT = std::function<void(std::size_t, std::uint32_t, bool, std::optional<Address>)>;
        EmbeddedAllocator() = default;
        
        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t, 
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;

        void commit() const override;

        void detach() const override;

        // size, slot_num, aligned, address (result)
        void setAllocCallback(AllocCallbackT callback);

    private:
        unsigned int m_count = 0;
        std::unordered_map<Address, std::size_t> m_allocations;
        AllocCallbackT m_alloc_callback;
    };
    
}