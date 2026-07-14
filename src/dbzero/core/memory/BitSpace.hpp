// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "utils.hpp"
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/memory/BitsetAllocator.hpp>

namespace db0

{

    /**
     * The BitSpace is a Memspace variant based on the BitsetAllocator
     * it's a backbone of the CRDT_Allocator
    */
    template <unsigned int BitN> class BitSpace: public Memspace
    {
    public:
        /**
         * @param prefix the underlying prefix
         * @param base_addr either the beginning or the end of the addressing space (depends on direction)
         * @param page_size allowed size of the single allocation
         * @param direction either 1 or -1 (the direction in which the addresses are allocated)
         * @param offset allows generating shortened relative addresses (adequate for 32bit representation)
        */
        BitSpace(std::shared_ptr<Prefix> prefix, Address base_addr, std::size_t page_size, int direction = 1);

        /**
         * Initialize a new bitspace over specific prefix
        */
        static void create(std::shared_ptr<Prefix>, Address base_addr, std::size_t page_size, int direction = 1);

        /// Get size (as the number of allocations) occupied by the allocated data
        std::size_t span() const {
            return m_bitset_allocator->span();
        }

        void clear();
        
        /// Return the size of the space occupied by the BitSpace itself
        static constexpr std::size_t sizeOf() {
            return BitSetT::sizeOf();
        }

        Address getBaseAddress() const {
            return m_bitset_allocator->getBaseAddress();
        }

        std::size_t getPageSize() const {
            return m_page_size;
        }

        void setDynamicBounds(std::function<std::uint64_t()> bounds_fn) {
            m_bitset_allocator->setDynamicBounds(bounds_fn);
        }

        void commit() const;

        void detach() const;

    private:
        using BitSetT = VFixedBitset<BitN>;
        using AllocatorT = BitsetAllocator<BitSetT>;
        const std::size_t m_page_size;
        // Convenience memspace (without an allocator) providing raw access to the underlying prefix
        Memspace m_internal_memspace;
        std::shared_ptr<AllocatorT> m_bitset_allocator;
        
        // begin / end address of the bitset
        static std::pair<Address, Address> bitsetAddr(Address base_addr, std::size_t page_size, int direction)
        {
            // align base address if necessary
            base_addr = alignWideRange(base_addr, BitSetT::sizeOf(), page_size, direction);
            if (direction > 0) {
                return { base_addr, base_addr + BitSetT::sizeOf() };
            } else {
                return { base_addr - BitSetT::sizeOf(), base_addr - BitSetT::sizeOf() };
            }
        }
    };
    
    template <unsigned int BitN> BitSpace<BitN>::BitSpace(std::shared_ptr<Prefix> prefix, Address base_addr,
        std::size_t page_size, int direction)
        : m_page_size(page_size)
        , m_internal_memspace(prefix, nullptr)
        // use page-aligned address as the first BitSpace allocated address
        , m_bitset_allocator(
            new AllocatorT(
                BitSetT(m_internal_memspace.myPtr(bitsetAddr(base_addr, page_size, direction).first)),
                alignAddress(bitsetAddr(base_addr, page_size, direction).second, page_size, direction),
                page_size,
                direction            
            )
        )
    {
        Memspace::init(prefix, m_bitset_allocator);
    }
    
    template <unsigned int BitN> void BitSpace<BitN>::clear() {
        m_bitset_allocator->clear();
    }
    
    template <unsigned int BitN> void BitSpace<BitN>::create(std::shared_ptr<Prefix> prefix, Address base_addr,
        std::size_t page_size, int direction)
    {
        Memspace memspace(prefix, nullptr);
        BitSetT::create(memspace, bitsetAddr(base_addr, page_size, direction).first);
    }
    
    template <unsigned int BitN> void BitSpace<BitN>::commit() const {
        // NOTE: we don't call Memspace::commit() to avoid unnecessary prefix commit
        m_bitset_allocator->commit();
    }
    
    template <unsigned int BitN> void BitSpace<BitN>::detach() const {
        m_bitset_allocator->detach();
    }    

} 