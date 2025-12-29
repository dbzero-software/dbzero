// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Allocator.hpp"
#include <dbzero/core/collections/bitset/FixedBitset.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    /**
     * Implements the Allocator interface over a FixedBitset.
     * only fixed size allocaions are allowed
    */
    template <typename BitSetT> class BitsetAllocator: public Allocator
    {
    public:
        /**
         * @param bitset the underlying bit container compatible with FixedBitset
         * @param base_address the begin / end address of the managed range (depends on direction)
         * @param alloc_size the allowed allocation size, typically equal data page size
         * @param direction either 1 or -1 (the direction in which the addresses are allocated)         
        */
        BitsetAllocator(BitSetT &&bitset, Address base_addr, std::size_t alloc_size, int direction);

        std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override;
        
        void free(Address) override;

        std::size_t getAllocSize(Address) const override;
        
        bool isAllocated(Address, std::size_t *size_of_result = nullptr) const override;
        
        void commit() const override;

        void detach() const override;
                
        /// Get the total number of allocations
        std::size_t getAllocCount() const;

        void clear();

        Address getBaseAddress() const {
            return m_base_addr;
        }
        
        /// Get total size of the area occupied by allocations (as the number of allocations)
        std::size_t span() const;

        /**
         * Enable dynamic bounds checking
         * 
         * @param bounds_fn the function to return the address threshold to not be exceeded
        */
        void setDynamicBounds(std::function<std::uint64_t()> bounds_fn);

        inline std::uint64_t addressOf(unsigned int index) const {
            return m_base_addr + (index * m_alloc_size) * m_direction - m_shift;
        }

    private:
        BitSetT m_bitset;
        const std::size_t m_alloc_size;
        const Address m_base_addr;
        const int m_direction;
        // allocation shift to account for the direction (0 for forward direction)
        const std::size_t m_shift;
        std::function<std::uint64_t()> m_bounds_fn;
        // pre-calculated span
        std::size_t m_span = 0;
        
        unsigned int indexOf(std::uint64_t address) const
        {
            if (m_direction > 0) {
                assert(m_shift == 0);
                return (address - m_base_addr) / m_alloc_size;
            } else {
                return (m_base_addr - address - m_shift) / m_alloc_size;
            }
        }

        std::size_t calculateSpan() const;
    };

    template <typename BitSetT> BitsetAllocator<BitSetT>::BitsetAllocator(BitSetT &&bitset, Address base_addr,
        std::size_t alloc_size, int direction)
        : m_bitset(std::move(bitset))
        , m_alloc_size(alloc_size)
        , m_base_addr(base_addr)
        , m_direction(direction)
        , m_shift(direction > 0 ? 0 : alloc_size)        
        , m_span(calculateSpan())        
    {
    }
    
    template <typename BitSetT> std::optional<Address>
    BitsetAllocator<BitSetT>::tryAlloc(std::size_t size, std::uint32_t slot_num, bool aligned, unsigned char, unsigned char)
    {
        assert(slot_num == 0);
        // all BitSetAllocator allocations are aligned        
        assert(size == m_alloc_size && "BitsetAllocator: invalid alloc size requested");
        auto index = m_bitset->firstIndexOf(false);
        if (index == m_bitset.npos) {
            return std::nullopt;
        }
        // validate dynamic bounds if set
        if (m_direction > 0) {
            assert(m_shift == 0);
            if (m_bounds_fn && m_base_addr.getOffset() + ((index + 1) * m_alloc_size) > m_bounds_fn()) {
                // address would exceed the bounds
                return std::nullopt;
            }
        } else {
            if (m_bounds_fn && addressOf(index) < m_bounds_fn()) {
                // address would exceed the bounds
                return std::nullopt;
            }
        }

        m_bitset.modify().set(index, true);
        m_span = calculateSpan();        
        return Address::fromOffset(addressOf(index));
    }

    template <typename BitSetT> void BitsetAllocator<BitSetT>::free(Address address)
    {
        if (address % m_alloc_size != 0) {
            // do not dealloc sub-addresses
            return;            
        }
        std::uint64_t index = indexOf(address);
        if (index >= m_bitset.npos || !m_bitset->get(index)) {
            THROWF(db0::BadAddressException) << "Invalid address: " << address;
        }
        m_bitset.modify().set(index, false);
        m_span = calculateSpan();
    }
    
    template <typename BitSetT> std::size_t BitsetAllocator<BitSetT>::getAllocSize(Address address) const
    {
        auto inner_offset = address % m_alloc_size;
        auto index = indexOf(address - inner_offset);
        if (index >= m_bitset.npos || !m_bitset->get(index)) {
            THROWF(db0::BadAddressException) << "BitsetAllocator " << this << " invalid address: " << address;
        }
        // handle inner offset to allow resolution of sub-addresses
        return m_alloc_size - inner_offset;
    }

    template <typename BitSetT>
    bool BitsetAllocator<BitSetT>::isAllocated(Address address, std::size_t *size_of_result) const
    {
        auto inner_offset = address % m_alloc_size;
        auto index = indexOf(address - inner_offset);
        if (index >= m_bitset.npos || !m_bitset->get(index)) {
            return false;
        }
        if (size_of_result) {
            *size_of_result = m_alloc_size - inner_offset;
        }
        return true;
    }

    template <typename BitSetT> std::size_t BitsetAllocator<BitSetT>::getAllocCount() const {
        return m_bitset->count(true);
    }

    template <typename BitSetT> void BitsetAllocator<BitSetT>::clear() {
        m_bitset.modify().reset();
    }

    template <typename BitSetT> std::size_t BitsetAllocator<BitSetT>::calculateSpan() const {
        return m_bitset->lastIndexOf(true) + 1;
    }

    template <typename BitSetT> void BitsetAllocator<BitSetT>::setDynamicBounds(std::function<std::uint64_t()> bounds_fn) {   
        m_bounds_fn = bounds_fn;
    }
    
    template <typename BitSetT> std::size_t BitsetAllocator<BitSetT>::span() const {
        return m_span;
    }
    
    template <typename BitSetT> void BitsetAllocator<BitSetT>::commit() const {
        m_bitset.commit();
    }

    template <typename BitSetT> void BitsetAllocator<BitSetT>::detach() const {
        m_bitset.detach();
    }

}