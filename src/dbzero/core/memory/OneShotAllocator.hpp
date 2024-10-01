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
        OneShotAllocator(std::uint64_t addr, std::size_t size);
        
        std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false, bool unique = false) override;
        
        void free(std::uint64_t address) override;

        std::size_t getAllocSize(std::uint64_t address) const override;
        
        void commit() const override;

        void detach() const override;

    private:
        const std::uint64_t m_addr;
        const std::size_t m_size;
        bool m_allocated = false;
    };

}
