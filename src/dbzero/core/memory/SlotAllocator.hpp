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
            bool aligned = false) override;

        // Unique allocations are not supported because of the limited slot's address space
        std::optional<UniqueAddress> tryAllocUnique(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address) const override;
                
        void commit() const override;

        void detach() const override;
        
        bool inRange(Address) const override;

        std::shared_ptr<Allocator> getAllocator() const { return m_allocator; }

        SlabAllocator &getSlot(std::uint32_t slot_num) const;
        
    private:
        std::shared_ptr<Allocator> m_allocator;
        Allocator *m_allocator_ptr;
        std::vector<std::shared_ptr<SlabAllocator> > m_slots;

        Allocator &select(std::uint32_t slot_num);
    };

}   