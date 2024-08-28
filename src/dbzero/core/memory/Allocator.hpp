#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <memory>

namespace db0

{
    
    /**
     * The DB0 allocator interface
    */
    class Allocator
    {
    public:        
        /**
         * @param the allocation size in bytes
         * @param slot_num optional slot number to allocate from (slot_num = 0 means any slot).
         * @param align for page-aligned allocation
         * Note that slot functionality is implementation specific and may not be supported by all allocators.
         * We use slots in special cases where objects needs to be allocated from a limited narrow address range
        */
        virtual std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false) = 0;
        
        /**
         * Free previously allocated address
        */
        virtual void free(std::uint64_t address) = 0;

        /**
         * Retrieve size of the range allocated under a specific address
         * 
         * @param address the address previously returned by alloc
         * @return the range size in bytes
        */
        virtual std::size_t getAllocSize(std::uint64_t address) const = 0;
                
        /**
         * Prepare the allocator for the next transaction
        */
        virtual void commit() = 0;

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
        std::uint64_t alloc(std::size_t size, std::uint32_t slot_num = 0, bool aligned = false);
    };

}