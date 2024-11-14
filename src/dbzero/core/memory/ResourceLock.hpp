#pragma once

#include <cstdint>
#include <memory>
#include <atomic>
#include <cassert>
#include <functional>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/threading/ROWO_Mutex.hpp>
#include <dbzero/core/threading/Flags.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{

    class DirtyCache;
    class BaseStorage;
    
    struct StorageContext
    {
        std::reference_wrapper<DirtyCache> m_cache_ref;
        std::reference_wrapper<BaseStorage> m_storage_ref;
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
        ResourceLock(StorageContext, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>,
            bool create_new);
        ResourceLock(const ResourceLock &, FlagSet<AccessOptions>);        
        
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
        
        // Finalize lock initialization by checking dirty flag
        // and appending to dirty cache if necessary
        // this method cannot be called from the constructor because shared_ptr of the lock is required
        void initDirty();
        
        void setDirty();

        bool isCached() const;

#ifndef NDEBUG
        bool isVolatile() const;
#endif        
        
        BaseStorage &getStorage() const {
            return m_context.m_storage_ref.get();
        }

        inline bool isDirty() const {
            return m_resource_flags & db0::RESOURCE_DIRTY;
        }
        
        void moveFrom(ResourceLock &);

        // clears the no_flush flag if it was set
        void resetNoFlush();
        // discard any changes done to this lock (to be called e.g. on rollback)
        void discard();
        
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

        // Conversion constructor
        ResourceLock(ResourceLock &&, std::vector<std::byte> &&data);

        void setRecycled(bool is_recycled);

        bool addrPageAligned(BaseStorage &) const;

    private:
#ifndef NDEBUG
        static std::atomic<std::size_t> rl_usage;
        static std::atomic<std::size_t> rl_count;
        static std::atomic<std::size_t> rl_op_count;
#endif        
    };

    std::ostream &showBytes(std::ostream &, const std::byte *, std::size_t);

}