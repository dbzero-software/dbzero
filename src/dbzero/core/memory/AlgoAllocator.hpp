// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include "Allocator.hpp"

namespace db0

{

    class AlgoAllocator: public Allocator
    {
    public:
        // a function defining the address pool over the 0 - 2^32 integer space
        using AddressPoolF = std::function<Address(unsigned int)>;
        // the reverse address pool function
        using ReverseAddressPoolF = std::function<unsigned int(Address)>;

        AlgoAllocator(AddressPoolF f, ReverseAddressPoolF rf, std::size_t alloc_size);

        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;
        
        void commit() const override;

        void detach() const override;

        /**
         * Set or update the max address assigned by the allocator
        */
        void setMaxAddress(Address max_address);

        /**
         * Reset the allocator to the initial state (as if no allocation was done)
        */
        void reset();
        
        /**
         * Get the first assigned i.e. the root address
        */
        Address getRootAddress() const;
        
    private:
        AddressPoolF m_address_pool_f;
        ReverseAddressPoolF m_reverse_address_pool_f;
        const std::size_t m_alloc_size;
        unsigned int m_next_i = 0;
    };

}