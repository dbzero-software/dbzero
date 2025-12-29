// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Allocator.hpp"
#include <vector>
#include <memory>
#include <optional>

namespace db0

{
    
    class SlabAllocator;

    /**
     * Implementation of the Allocator interface which also supports slot_num parameter
    */
    class SlotAllocator: public Allocator
    {
    public:
        SlotAllocator(std::shared_ptr<Allocator> allocator);

        // initialize slot-specific allocator
        void setSlot(std::uint32_t slot_num, std::shared_ptr<SlabAllocator> slot_allocator);
        
        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        // Unique allocations are not supported because of the limited slot's address space
        std::optional<UniqueAddress> tryAllocUnique(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;
        std::size_t getAllocSize(Address, unsigned char realm_id) const override;

        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;
        bool isAllocated(Address, unsigned char realm_id, std::size_t *size_of_result = nullptr) const override;
        
        void commit() const override;

        void detach() const override;
        
        bool inRange(Address) const override;

        std::shared_ptr<Allocator> getAllocator() const { return m_allocator; }

        SlabAllocator &getSlot(std::uint32_t slot_num) const;
        
        std::pair<Address, std::optional<Address> > getRange(std::uint32_t slot_num = 0) const override;
        
    private:
        std::shared_ptr<Allocator> m_allocator;
        Allocator *m_allocator_ptr;
        std::vector<std::shared_ptr<SlabAllocator> > m_slots;

        Allocator &select(std::uint32_t slot_num);
    };

}   