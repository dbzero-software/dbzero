#pragma once

#include <cstdint>
#include "Types.hpp"

namespace db0

{

    // The container for storing DP micro-updates, i.e. mutated ranges
    class [[gnu::packed]] o_mu_store: public o_base<o_mu_store>
    {
    protected:
        using super_t = o_base<o_mu_store>;
        friend super_t;
        // @param max_bytes the container's capacity and size in bytes
        o_mu_store(std::size_t max_bytes);

    public:
        /**
         * @param offset must be 12 bits or less
         * @param size must be 12 bits or less
         * @return true if append was successful, otherwise capacity is exceeded
         */
        bool tryAppend(std::uint16_t offset, std::uint16_t size);
        
        static std::size_t measure(std::size_t max_bytes);
        std::size_t sizeOf() const;

    private:
        std::uint16_t m_capacity;

        // Compact 2x 12 bit values into 24 bit container
        inline void compact(std::uint16_t offset, std::uint16_t size, std::array<std::uint8_t, 3> &result)
        {
            result[0] = (offset >> 4) & 0xFF;
            result[1] = ((offset & 0xF) << 4) | ((size >> 8) & 0xF);
            result[2] = size & 0xFF;
        }
    };

}