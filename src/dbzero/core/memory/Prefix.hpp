// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "config.hpp"
#include <dbzero/core/memory/MemLock.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <optional>

namespace db0

{

    class Allocator;
    class BaseStorage;
    class ProcessTimer;
    
    /**
     * The Prefix interface represents a single DB0 Prefix space
     * Prefix can either be mutable or immutable, commit can be performed on a mutable Prefix only
     * Prefix is associated with a specific state ID, which is mutated during commit
    */
    class Prefix
    {
    public:
        Prefix(std::string name);

        virtual ~Prefix() = default;

        /**
         * Allocates a new range with a specific allocator and maps it into memory of the current process
         * @param addr_ptr optional pointer to store the allocated address
        */
        MemLock allocRange(Allocator &, std::size_t size, std::uint64_t *addr_ptr = nullptr) const;

        /**
         * Maps a specific address range into memory of the current process
         *          
         * @param address the address to start from
         * @param size the range size (in bytes)
         * @return the memory lock object or exception thrown on invalid address / out of available range
        */
        virtual MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) = 0;
        
        virtual std::size_t getPageSize() const = 0;
        
        /**
         * Get current (or the last finalized) state number
         * The current and finalized state number may be different for read/write prefixes (on head transaction)
         */
        virtual StateNumType getStateNum(bool finalized = false) const = 0;
        
        /**
         * Commit all local changes made since last commit
         * 
         * @return the state ID after commit
        */
        virtual std::uint64_t commit(ProcessTimer * = nullptr) = 0;
        
        virtual void close(ProcessTimer *timer_ptr = nullptr) = 0;
        
        /**
         * Get last update timestamp
        */
        virtual std::uint64_t getLastUpdated() const = 0;

        virtual bool beginRefresh();

        /**
         * @return timestamp of the last update (or 0 if not updated)
         */
        virtual std::uint64_t completeRefresh();

        /**
         * Update read-only prefix data to the latest changes done by other processes
         * @return 0 if no changes have been applied, otherwise last update timestamp
        */
        virtual std::uint64_t refresh();

        virtual AccessType getAccessType() const = 0;

        virtual BaseStorage &getStorage() const = 0;

        const std::string &getName() const;

        /**
         * Get the read-only snapshot of the prefix (i.e. a view based on current state number)
         */
        virtual std::shared_ptr<Prefix> getSnapshot(std::optional<StateNumType> state_num = {}) const = 0;
        
        // Get approximate (may not be threading precise) volume of the underlying dirty locks
        virtual std::size_t getDirtySize() const = 0;
        
        // Begin atomic operation with this prefix
        virtual void beginAtomic();

        // End atomic operation with this prefix
        virtual void endAtomic();

        // Cancel/revert atomic operation with this prefix
        virtual void cancelAtomic();
        
        // Perform memory cleanups, e.g. by removing expired weak pointers
        // the default empty implementation is provided
        virtual void cleanup() const;

        // Try releasing a specific volume of dirty locks
        // @return number of bytes actually released
        virtual std::size_t flushDirty(std::size_t limit) = 0;

        // Get arbitrary prefix statistics
        virtual void getStats(std::function<void(const std::string &name, std::uint64_t value)>) const;
                        
    private:
        const std::string m_name;
    };

}