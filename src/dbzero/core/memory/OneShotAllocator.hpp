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
            bool aligned = false) override;

        std::optional<UniqueAddress> tryAllocUnique(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false) override;

        void free(Address) override;

        std::size_t getAllocSize(Address) const override;

        bool isAllocated(Address) const override;
        
        void commit() const override;

        void detach() const override;

    private:
        const Address m_addr;
        const std::size_t m_size;
        bool m_allocated = false;
    };

}
