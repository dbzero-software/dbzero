#pragma once

#include "Allocator.hpp"
#include <vector>
#include <memory>
#include <optional>

namespace db0

{
    
    /**
     * Implementation of the Allocator interface which also supports slot_num parameter
    */
    class SlotAllocator: public Allocator
    {
    public:
        SlotAllocator(std::shared_ptr<Allocator> allocator);

        // initialize slot-specific allocator
        void setSlot(std::uint32_t slot_num, std::shared_ptr<Allocator> slot_allocator);

        std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t slot_num = 0) override;

        void free(std::uint64_t address) override;

        std::size_t getAllocSize(std::uint64_t address) const override;
                
        void commit() override;

        void detach() override;
        
        std::shared_ptr<Allocator> getAllocator() const { return m_allocator; }

        const Allocator &getSlot(std::uint32_t slot_num) const;

    private:
        std::shared_ptr<Allocator> m_allocator;
        Allocator *m_allocator_ptr;
        std::vector<std::shared_ptr<Allocator> > m_slots;

        Allocator &select(std::uint32_t slot_num);
    };

}   