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
         * Allocate a new continuous range of a given size
         * 
         * @param size size (in bytes) of the range to be allocated
         * @return the address of the range
        */
        std::uint64_t alloc(std::size_t size);
        
        virtual std::optional<std::uint64_t> tryAlloc(std::size_t size) = 0;
        
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
    };
    
}