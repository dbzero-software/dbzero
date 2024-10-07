#pragma once

#include <cstdint>
#include <memory>
#include <atomic>
#include "ResourceLock.hpp"
#include <dbzero/core/threading/ROWO_Mutex.hpp>
#include <dbzero/core/threading/Flags.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{

    /**
     * A DP_Lock holds a single or multiple data pages in a specific state (read)
     * mutable locks can process updates from the current transaction only and cannot be later mutated
     * If a DP_Lock has no owner object, it can be dragged on to the next transaction (to avoid CoWs)
     * and improve cache performance
     */
    class DP_Lock: public ResourceLock
    {
    public:
        /**         
         * @param address the resource address
         * @param size size of the resource
         * @param access_mode access flags
         * @param read_state_num the existing state number of the finalized transaction (or 0 if not available)
         * @param write_state_num the current transaction number (or 0 for read-only locks)
         * @param create_new flag indicating if this a newly created resource (e.g. newly appended data page)
        */
        DP_Lock(StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>, std::uint64_t read_state_num,
            std::uint64_t write_state_num, bool create_new = false);
        
        /**
         * Create a copied-on-write lock from an existing lock         
        */
        DP_Lock(const DP_Lock &, std::uint64_t write_state_num, FlagSet<AccessOptions>);
                
        /**
         * Flush data from local buffer and clear the 'dirty' flag
         * data is not flushed if not dirty.
         * Data is flushed into the current state of the associated storage view
        */
        void flush() override;
        
        /**
         * Update lock to a different state number
         * this can safely be done only for unused locks (cached only)
         * This operation will also upgrade the acccess mode to "write"
         * @param state_num the new state number
         * @param no_flush true to additionally assign the no_flush flag
         */
        void updateStateNum(std::uint64_t state_num, bool no_flush);
        
        std::uint64_t getStateNum() const;
        
        // Updates the local state number before merging atomic operation with active transaction
        void merge(std::uint64_t final_state_num);
        
    protected:
        // the updated state number or read-only state number
        std::uint64_t m_state_num;

        struct tag_derived {};
        DP_Lock(tag_derived, StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode,
            std::uint64_t read_state_num, std::uint64_t write_state_num , bool create_new);
    };
    
}