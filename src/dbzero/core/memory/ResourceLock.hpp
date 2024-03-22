#pragma once

#include <cstdint>
#include <memory>
#include <atomic>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/threading/ROWO_Mutex.hpp>
#include <dbzero/core/storage/StorageView.hpp>
#include <dbzero/core/threading/Flags.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{
    
    class ResourceLock
    {
    public:
        /**
         * @param storage associated storage reference
         * @param address the resource address
         * @param size size of the resource
         * @param access_mode access flags
         * @param create_new flag indicating if this a newly created resource (e.g. newly appended data page)
        */
        ResourceLock(StorageView &, std::uint64_t address, std::size_t size, FlagSet<AccessOptions>, bool create_new = false);

        /**
         * Create a copied-on-write lock from an existing lock
         * @param src_state_num the state number of the lock being copied
        */
        ResourceLock(const ResourceLock &, std::uint64_t src_state_num, FlagSet<AccessOptions>);

        virtual ~ResourceLock();
        
        /**
         * Get the underlying buffer, retrieve data from storage if needed
        */
        virtual void *getBuffer(std::uint64_t state_num) const;
        
        void *getBuffer(std::uint64_t address, std::uint64_t state_num) const;

        std::vector<std::byte> copyData(std::uint64_t state_num) const;

        /**
         * Flush data from local buffer and clear the 'dirty' flag
         * data is not flushed if not dirty.
         * Data is flushed into the current state of the associated storage view
        */
        virtual void flush();

        /**
         * Clear the 'dirty' flag if it has been set
         * @return true if the flag was set
        */
        virtual bool resetDirtyFlag();
        
        /**
         * Release unmodified storage buffer lock
         * this is to allow updating to a more recent (or older) data version
        */
        virtual void release();

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

        const StorageView &getStorageView() const;
        
    protected:
        friend class CacheRecycler;
        using list_t = db0::FixedList<std::shared_ptr<ResourceLock> >;
        using iterator = list_t::iterator;

        // NOTICE: access mode low bits reserved for the rowo-mutex
        using ResourceFetchMutexT = ROWO_Mutex<
            std::uint16_t, 
            db0::RESOURCE_FETCHED,
            db0::RESOURCE_FETCHED,
            db0::RESOURCE_LOCK >;
        
        using ResourceDirtyMutexT = ROWO_Mutex<
            std::uint16_t, 
            db0::RESOURCE_DIRTY,
            db0::RESOURCE_DIRTY,
            db0::RESOURCE_LOCK >;
        
        StorageView &m_storage_view;
        const std::uint64_t m_address;
        mutable std::atomic<std::uint16_t> m_resource_flags = 0;
        FlagSet<AccessOptions> m_access_mode;
        
        mutable std::vector<std::byte> m_data;
        // CacheRecycler's iterator
        iterator m_recycle_it = 0;

        void setRecycled(bool is_recycled);

        bool isDirty() const {
            return m_resource_flags & db0::RESOURCE_DIRTY;
        }
    };
    
}