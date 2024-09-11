#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <memory>
#include <cassert>

namespace db0

{
    
    // Converts allocator's logical address into a physical one (by removing high 14 bits)
    inline std::uint64_t getPhysicalAddress(std::uint64_t address) {
        return address & 0x0003'FFFF'FFFF'FFFF;
    }
    
    inline bool isPhysicalAddress(std::uint64_t address) {
        return (address & 0xFFFC'0000'0000'0000) == 0;
    }
    
    // extract the instance ID part from the physical address (high 14 bits)
    inline std::uint16_t getInstanceId(std::uint64_t address) {
        return address >> 50;
    }
    
    // combine physical address & instance ID to form a logical address
    std::uint64_t makeLogicalAddress(std::uint64_t address, std::uint16_t instance_id) 
    {
        assert(isPhysicalAddress(address));
        return (static_cast<std::uint64_t>(instance_id) << 50) | address;
    }
    
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
         * Note that slot functionality is implementation specific and may not be supported by all allocators.
         * We use slots in special cases where objects needs to be allocated from a limited narrow address range
        */
        virtual std::optional<std::uint64_t> tryAlloc(std::size_t size, std::uint32_t slot_num = 0, 
            bool aligned = false, bool unique = false) = 0;
        
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
        std::uint64_t alloc(std::size_t size, std::uint32_t slot_num = 0, bool aligned = false, 
            bool unique = false);
    };

}