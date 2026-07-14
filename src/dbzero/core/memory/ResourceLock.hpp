// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <memory>
#include <atomic>
#include <cassert>
#include <functional>
#include <optional>
#include <limits>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/serialization/mu_store.hpp>
#include <dbzero/core/threading/ROWO_Mutex.hpp>
#include <dbzero/core/threading/Flags.hpp>
#include <dbzero/core/utils/FixedList.hpp>
#include "diff_utils.hpp"

namespace db0

{

    class DirtyCache;
    class BaseStorage;
    
    struct StorageContext
    {
        std::reference_wrapper<DirtyCache> m_cache_ref;
        std::reference_wrapper<BaseStorage> m_storage_ref;
    };
    
    enum class FlushMethod: std::uint8_t
    {
        // Flush using the diff-range calculation (if possible)
        diff = 0x01 ,
        // Flush using the full-DP write (always possible)
        full = 0x02
    };
        
    /**
     * A ResourceLock is the foundation class for DP_Lock and BoundaryLock implementations    
     * it supposed to hold a single or multiple data pages in a specific state (read)
     * mutable locks can process updates from the current transaction only and cannot be later mutated
     * If a DP_Lock has no owner object, it can be dragged on to the next transaction (to avoid CoWs)
     * and improve cache performance
     */
    class ResourceLock: public std::enable_shared_from_this<ResourceLock>
    {
    public:
        // the limit of forced diff-ranges per ResourceLock
        static constexpr std::size_t MAX_DIFF_RANGES = 64;

        /**
         * @param storage_context
         * @param address the starting address in the storage
         * @param size the size of the data in bytes
         * @param access_options the resource flags         
         * 
         * NOTE: even if the ResourceLock is created with AccessOptions::write
         * one is required to call setDirty to mark it as modified
         */
        ResourceLock(StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>,
            std::shared_ptr<ResourceLock> cow_lock = nullptr);
        
        // Copy-on-write constructor
        ResourceLock(std::shared_ptr<ResourceLock>, FlagSet<AccessOptions>);
        
        virtual ~ResourceLock();
        
        /**
         * Get the underlying buffer
        */
        inline void *getBuffer() const {
            return m_data.data();
        }

        inline void *getBuffer(std::uint64_t address) const
        {
            assert(address >= m_address && address < m_address + m_data.size());
            return static_cast<std::byte*>(getBuffer()) + address - m_address;
        }
        
        // Get the address within the ResourceLock's internal buffer
        std::uint64_t getAddressOf(const void *) const;
        
        // Try flushing using a specific method
        // @return true if the lock's data has been flushed (or not dirty)
        virtual bool tryFlush(FlushMethod) = 0;

        /**
         * Flush data from local buffer and clear the 'dirty' flag
         * data is not flushed if not dirty.
         * Data is flushed into the current state of the associated storage view
        */
        virtual void flush() = 0;

        /**
         * Clear the 'dirty' flag if it has been set, clear diffs
         * @return true if the flag was set
        */
        bool resetDirtyFlag();
                
        inline std::uint64_t getAddress() const {
            return m_address;
        }
        
        inline std::size_t size() const {
            return m_data.size();
        }

        inline bool isRecycled() const {
            return m_resource_flags & db0::RESOURCE_RECYCLED;
        }

        inline bool isCached() const {
            return !m_access_mode[AccessOptions::no_cache];
        }
                
        // Mark lock as dirty without range specification
        void setDirty();

        // Mark a specific range as forced-dirty
        // it will be assumed dirty even if the data is not changed
        // the range must fit within the lock's address range
        void setDirty(std::uint64_t begin, std::uint64_t end);

        // Sets the RESOURCE_FREEZE flag
        void freeze();

        

#ifndef NDEBUG
        bool isVolatile() const;
#endif        
        
        BaseStorage &getStorage() const {
            return m_context.m_storage_ref.get();
        }

        inline bool isDirty() const {
            return m_resource_flags & db0::RESOURCE_DIRTY;
        }

        // The "frozen" state prevents the resource from reusing in write operations
        inline bool isFrozen() const {
            return m_resource_flags & db0::RESOURCE_FREEZE;
        }

        // The operation checks if the lock is non-dirty and non-frozen (by inspecting the flags)
        inline bool allowReuse() const {
            return !(m_resource_flags & (db0::RESOURCE_FREEZE | db0::RESOURCE_DIRTY));
        }
        
        // Apply changes from the lock being merged (discarding changes in this lock)
        // operation required by the atomic merge
        void moveFrom(ResourceLock &);
        
        // clears the no_flush flag if it was set
        void resetNoFlush();
        // discard any changes done to this lock (to be called e.g. on rollback)
        void discard();
        
        // Check if the copy-on-write data is available
        // this member is used for debug & evaluation purposes
        bool hasCoWData() const;
                
        // Calculate the estimated upper bound of a memory footprint
        // NOTE: for mutable locks (i.e. from active transactiona the CoW buffer capacity is added)
        std::size_t usedMem() const;
        
        // retrieve diffs from the CoW buffer (if such exists)
        bool getDiffs(std::vector<std::uint16_t> &) const;
        
#ifndef NDEBUG
        // get total memory usage of all ResourceLock instances
        // @return total size in bytes / total count
        static std::pair<std::size_t, std::size_t> getTotalMemoryUsage();
        virtual bool isBoundaryLock() const = 0;
#endif
        
    protected:
        friend class CacheRecycler;
        friend class BoundaryLock;
        using list_t = db0::FixedList<std::shared_ptr<ResourceLock> >;
        using iterator = list_t::iterator;
        
        using ResourceDirtyMutexT = ROWO_Mutex<
            std::uint16_t,
            db0::RESOURCE_DIRTY,
            db0::RESOURCE_DIRTY,
            db0::RESOURCE_LOCK >;
        
        StorageContext m_context;
        const std::uint64_t m_address;
        mutable std::atomic<std::uint16_t> m_resource_flags = 0;
        FlagSet<AccessOptions> m_access_mode;
        
        mutable std::vector<std::byte> m_data;
        // CacheRecycler's iterator
        iterator m_recycle_it = 0;
        // immutable copy-on-write lock (i.e. previous version)
        std::shared_ptr<ResourceLock> m_cow_lock;
        // the internally managed CoW's buffer
        std::vector<std::byte> m_cow_data;
        // Optional diff-ranges (begin, end)
        // these ranges are relevant e.g. when tracking the "silent" mutations
        DiffRange m_diffs;
        // special indicator of the zero-fill buffer
        static const std::byte m_cow_zero;
        
        void setRecycled(bool is_recycled);
        
        bool addrPageAligned(BaseStorage &) const;
        
        const std::byte *getCowPtr() const;
        bool getDiffs(const void *buf, std::vector<std::uint16_t> &result) const;
                
#ifndef NDEBUG
        static std::atomic<std::size_t> rl_usage;
        static std::atomic<std::size_t> rl_count;
        static std::atomic<std::size_t> rl_op_count;
#endif
        
        std::size_t getPageSize() const;

    private:
        // init the dirty state of the lock
        bool initDirty();
    };

    std::ostream &showBytes(std::ostream &, const std::byte *, std::size_t);

}