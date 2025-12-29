// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstddef>
#include <array>
#include <limits>
#include "Types.hpp"
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN

    // The container for storing DP micro-updates, i.e. mutated ranges
    class DB0_PACKED_ATTR o_mu_store: public o_base<o_mu_store>
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

        // Append a special element marking the whole range as modified
        // note that subsequent appends until "clear" is called will complete with "false"
        // this state can also be obtained by reaching full capactity
        void appendFullRange();

        static std::size_t measure(std::size_t max_bytes);
        
        std::size_t sizeOf() const;

        class ConstIterator
        {
        public:            
            bool operator!=(const ConstIterator &) const;
            bool operator==(const ConstIterator &) const;

            ConstIterator &operator++();
            std::pair<std::uint16_t, std::uint16_t> operator*() const;
            
            std::size_t operator-(const ConstIterator &other) const;

        protected:
            friend o_mu_store;
            ConstIterator(const std::uint8_t *current);

            std::uint8_t *m_current;            
        };
        
        ConstIterator begin() const;
        ConstIterator end() const;

        std::size_t size() const;
        
        // Get capacity of the container (as the number of elements)
        std::size_t maxSize() const;

        // Sort elements and merge similar or overlapping ranges
        void compact();

        void clear();
        
        // Calculate the total size of all micro-updates
        // @return 0 if full range is set
        std::size_t getMUSize() const;

        // Check if the collection contains a special marker indicating full-range modification
        inline bool isFullRange() const {
            return m_size == std::numeric_limits<std::uint8_t>::max();
        }
        
        static constexpr std::size_t maxCapacity()
        {
            return std::min(
                static_cast<std::size_t>(std::numeric_limits<decltype(m_capacity)>::max()),
                static_cast<std::size_t>((std::numeric_limits<decltype(m_size)>::max() - 1) * 3 + sizeof(o_mu_store))
            );
        }
        
    private:
        // total capacity in bytes (sizeof)
        std::uint16_t m_capacity;
        // size as the number of elements
        std::uint8_t m_size = 0;
    };
    
    // Compress 2x 12 bit values into 24 bit container
    inline void compress(std::uint16_t offset, std::uint16_t size, std::array<std::uint8_t, 3> &result)
    {
        result[0] = (offset >> 4) & 0xFF;
        result[1] = ((offset & 0xF) << 4) | ((size >> 8) & 0xF);
        result[2] = size & 0xFF;
    }

DB0_PACKED_END
}    
