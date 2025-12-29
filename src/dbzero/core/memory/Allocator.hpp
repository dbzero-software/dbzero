// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <memory>
#include <cassert>
#include "Address.hpp"

namespace db0

{
    
    /**
     * The DB0 allocator interface
     * NOTE: allocators may return logical adddress which needs to be converted to physical one
    */
    class Allocator
    {
    public:        
        /**
         * @param the allocation size in bytes
         * @param slot_num optional slot number to allocate from (slot_num = 0 means any slot).
         * @param align a flag for page-aligned allocation
         * @param unique a flag for generating a unique, never repeating addresses
         * @param realm_id the realm ID to allocate from (where supported)
         * @param locality the locality (hint) to allocate from (where supported) (0 = any locality)
         * Note that slot functionality is implementation specific and may not be supported by all allocators.
         * We use slots in special cases where objects needs to be allocated from a limited narrow address range
        */
        virtual std::optional<Address> tryAlloc(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) = 0;
        
        // Try allocating a unique, never repeating address
        // NOTE: this functionality is only supported by some allocators
        // The default throwing implementation is provided
        virtual std::optional<UniqueAddress> tryAllocUnique(std::size_t size, std::uint32_t slot_num = 0,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0);
        
        /**
         * Free previously allocated address
         * @param address the address previously returned by alloc (the memory offset part)
        */
        virtual void free(Address) = 0;
        
        /**
         * Retrieve size of the range allocated under a specific address
         * 
         * @param address the address previously returned by alloc
         * @return the range size in bytes
        */
        virtual std::size_t getAllocSize(Address) const = 0;
        // getAllocSize with realm_id validatation (where unsupported simply forwards to the non-realm version)
        virtual std::size_t getAllocSize(Address, unsigned char realm_id) const;
        
        /**
         * Check if the address is a valid allocation address with this allocator
         * size_of retrieved on request (if size_of_result is not null)
         */
        virtual bool isAllocated(Address, std::size_t *size_of_result = nullptr) const = 0;
        // isAllocated version with realm_id validation
        virtual bool isAllocated(Address, unsigned char realm_id, std::size_t *size_of_result = nullptr) const;
        
        /**
         * Prepare the allocator for the next transaction
        */
        virtual void commit() const = 0;

        virtual void detach() const = 0;

        // Flush any pending deferred operations (e.g. deferred free)
        // the default empty implementation is provieded
        virtual void flush() const;
        
        /**
         * Allocate a new continuous range of a given size
         * 
         * @param size size (in bytes) of the range to be allocated
         * @param slot_num optional slot number to allocate from (slot_num = 0 means any slot).
         * @return the address of the range
        */
        Address alloc(std::size_t size, std::uint32_t slot_num = 0, bool aligned = false, 
            unsigned char realm_id = 0, unsigned char locality = 0);
        
        UniqueAddress allocUnique(std::size_t size, std::uint32_t slot_num = 0, bool aligned = false, 
            unsigned char realm_id = 0, unsigned char locality = 0);
        
        // Check if the address is within the range managed by the allocator
        // (only applicable to limited allocators - e.g. SlabAllocator)
        virtual bool inRange(Address) const;
        
        // Get range covered by the allocator or a specific slot
        // @return begin / end (which might be undefined for unlimited allocators)
        virtual std::pair<Address, std::optional<Address> > getRange(std::uint32_t slot_num = 0) const;
        
        // To be implemented where it makes sense
        virtual void close();
    };
    
}