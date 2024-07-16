#pragma once

#include <cstdint>
#include <memory>
#include <atomic>
#include <cassert>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/threading/ROWO_Mutex.hpp>
#include <dbzero/core/threading/Flags.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{

    class BaseStorage;
    
    /**
     * A BaseLock is the foundation class for ResourceLock and BoundaryLock implementations    
     * it supposed to hold a single or multiple data pages in a specific state (read)
     * mutable locks can process updates from the current transaction only and cannot be later mutated
     * If a ResourceLock has no owner object, it can be dragged on to the next transaction (to avoid CoWs)
     * and improve cache performance
     */
    class BaseLock
    {
    public:
        BaseLock(BaseStorage &storage, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>,
            bool create_new);
        BaseLock(const BaseLock &, FlagSet<AccessOptions>);
        virtual ~BaseLock();
        
        /**
         * Get the underlying buffer
        */
        inline void *getBuffer() const {
            return m_data.data();
        }

        inline void *getBuffer(std::uint64_t address) const {
            assert(address >= m_address && address < m_address + m_data.size());
            return static_cast<std::byte*>(getBuffer()) + address - m_address;
        }
        
        /**
         * Flush data from local buffer and clear the 'dirty' flag
         * data is not flushed if not dirty.
         * Data is flushed into the current state of the associated storage view
        */
        virtual void flush() = 0;

        /**
         * Clear the 'dirty' flag if it has been set
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
        
        inline void setDirty() {
            safeSetFlags(m_resource_flags, db0::RESOURCE_DIRTY);
        }
        
        bool isCached() const;
        
        BaseStorage &getStorage() const {
            return m_storage;
        }

        bool isDirty() const {
            return m_resource_flags & db0::RESOURCE_DIRTY;
        }
        
        void copyFrom(const BaseLock &);

        // clears the no_flush flag if it was set
        void resetNoFlush();

    protected:
        friend class CacheRecycler;
        using list_t = db0::FixedList<std::shared_ptr<BaseLock> >;
        using iterator = list_t::iterator;
        
        using ResourceDirtyMutexT = ROWO_Mutex<
            std::uint16_t, 
            db0::RESOURCE_DIRTY,
            db0::RESOURCE_DIRTY,
            db0::RESOURCE_LOCK >;
        
        // the updated state number or read-only state number
        BaseStorage &m_storage;
        const std::uint64_t m_address;
        mutable std::atomic<std::uint16_t> m_resource_flags = 0;
        FlagSet<AccessOptions> m_access_mode;
        
        mutable std::vector<std::byte> m_data;
        // CacheRecycler's iterator
        iterator m_recycle_it = 0;

        void setRecycled(bool is_recycled);

        bool addrPageAligned(BaseStorage &) const;
    };

}