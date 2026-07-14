// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <memory>
#include <atomic>
#include "config.hpp"
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
         * @param cow_lock optional copy-on-write lock (previous version)
        */
        DP_Lock(StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>, StateNumType read_state_num,
            StateNumType write_state_num, std::shared_ptr<ResourceLock> cow_lock = nullptr);

        /**
         * Create a copied-on-write lock from an existing lock   
        */
        DP_Lock(std::shared_ptr<DP_Lock>, StateNumType write_state_num, FlagSet<AccessOptions>);
        
        bool tryFlush(FlushMethod) override;
        
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
         * !!! The internal buffer can be moved (underlying pointer change is allowed)
         * @param state_num the new state number
         * @param is_volatile flag indicating if the volatile lock (i.e. of the atomic operation) is being created
         */
        void updateStateNum(StateNumType state_num, bool is_volatile);
        
        std::uint64_t getStateNum() const;
        
        // Updates the local state number before merging atomic operation with the active transaction
        void merge(StateNumType final_state_num);
        
#ifndef NDEBUG
        bool isBoundaryLock() const override;
#endif
        
    protected:
        // the actual state number under which this lock is registered
        StateNumType m_state_num;
        
        struct tag_derived {};
        DP_Lock(tag_derived, StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode,
            StateNumType read_state_num, StateNumType write_state_num, std::shared_ptr<ResourceLock> cow_lock);
        
        bool _tryFlush(FlushMethod);        
    };
    
}