#pragma once

#include <functional>
#include "Allocator.hpp"

namespace db0

{

    class AlgoAllocator: public Allocator
    {
    public:
        // a function defining the address pool over the 0 - 2^32 integer space
        using AddressPoolF = std::function<std::uint64_t(unsigned int)>;
        // the reverse address pool function
        using ReverseAddressPoolF = std::function<unsigned int(std::uint64_t)>;

        AlgoAllocator(AddressPoolF f, ReverseAddressPoolF rf, std::size_t alloc_size);

        std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false) override;
        
        void free(std::uint64_t address) override;

        std::size_t getAllocSize(std::uint64_t address) const override;
        
        void commit() override;

        void detach() override;

        /**
         * Set or update the max address assigned by the allocator
        */
        void setMaxAddress(std::uint64_t max_address);

        /**
         * Reset the allocator to the initial state (as if no allocation was done)
        */
        void reset();

        /**
         * Get the first assigned i.e. the root address
        */
        std::uint64_t getRootAddress() const;

    private:
        AddressPoolF m_address_pool_f;
        ReverseAddressPoolF m_reverse_address_pool_f;
        const std::size_t m_alloc_size;
        unsigned int m_next_i = 0;
    };

}