#pragma once

#include <stdexcept>
#include <dbzero/core/memory/Prefix.hpp>
#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <dbzero/core/vspace/v_ptr.hpp>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include "utils.hpp"
#include "PrefixViewImpl.hpp"
    
namespace db0

{
    
    class CacheRecycler;
    
    /**
     * The default implementation of the Prefix interface
    */
    template <typename StorageT> class PrefixImpl:
        public Prefix, public std::enable_shared_from_this<PrefixImpl<StorageT>>
    {
    public:
        /**
         * Opens an existing prefix from a specific storage
        */
        template <typename... StorageArgs> PrefixImpl(std::string name, CacheRecycler *cache_recycler_ptr,  
            std::optional<std::uint64_t> state_num, StorageArgs&&... storage_args)
            : Prefix(name)
            , m_storage(std::forward<StorageArgs>(storage_args)...)
            , m_page_size(m_storage.getPageSize())
            , m_shift(getPageShift(m_page_size))
            , m_state_num(getStateNum(m_storage.getMaxStateNum(), state_num))            
            , m_cache(m_storage, cache_recycler_ptr)
        {
            if (m_storage.getAccessType() == AccessType::READ_WRITE) {
                // increment state number for read-write storage (i.e. new data transaction)
                ++m_state_num;
            }
        }
        
        template <typename... StorageArgs> PrefixImpl(std::string name, CacheRecycler &cache_recycler,
            std::optional<std::uint64_t> state_num, StorageArgs&&... storage_args)
            : PrefixImpl(name, &cache_recycler, state_num, std::forward<StorageArgs>(storage_args)...)
        {
        }
        
        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) const override;
        
        std::uint64_t size() const override;

        std::uint64_t getStateNum() const override;
        
        std::size_t getPageSize() const override {
            return m_page_size;
        }

        std::uint64_t commit() override;

        std::uint64_t getLastUpdated() const override;

        BaseStorage &getStorage() const override;

        const PrefixCache &getCache() const;

        /**
         * Release all owned locks from the cache recycler and clear the cache
         * this method should be called before closing the prefix to clean up used resources
         * Finally close the corresponding storage.
        */
        void close() override;
        
        std::uint64_t refresh() override;

        AccessType getAccessType() const override {
            return m_storage.getAccessType();
        }

        std::shared_ptr<Prefix> getSnapshot(std::optional<std::uint64_t> state_num = {}) const override;

        void beginAtomic() override;

        void endAtomic() override;

        void cancelAtomic() override;
        
    protected:
        friend class PrefixViewImpl<PrefixImpl<StorageT>>;
        
        mutable StorageT m_storage;
        const std::size_t m_page_size;
        const std::uint32_t m_shift;
        std::uint64_t m_state_num;        
        mutable PrefixCache m_cache;
        // flag indicating atomic operation in progress
        bool m_atomic = false;

        MemLock mapRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
            FlagSet<AccessOptions> = {}) const;

        std::shared_ptr<ResourceLock> mapPage(std::uint64_t page_num, std::uint64_t state_num, FlagSet<AccessOptions>) const;
        std::shared_ptr<BoundaryLock> mapBoundaryRange(std::uint64_t page_num, std::uint64_t address,
            std::size_t size, std::uint64_t state_num, FlagSet<AccessOptions>) const;
        std::shared_ptr<ResourceLock> mapWideRange(std::uint64_t first_page, std::uint64_t end_page,
            std::uint64_t state_num, FlagSet<AccessOptions>) const;

        std::uint64_t getStateNum(std::uint64_t storage_max_state_num, std::optional<std::uint64_t> user_state_num) const;

        inline bool isPageAligned(std::uint64_t addr_or_size) const {
            return (addr_or_size & (m_page_size - 1)) == 0;
        }

        std::uint64_t findUniqueMutation(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t state_num) const;
        bool tryFindUniqueMutation(std::uint64_t first_page, std::uint64_t end_page,
            std::uint64_t state_num, std::uint64_t &mutation_id) const;

        void adjustAccessMode(FlagSet<AccessOptions> &access_mode, std::uint64_t address, std::size_t size) const;
    };

    template <typename T>
    MemLock PrefixImpl<T>::mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode) const {
        return mapRange(address, size, m_state_num, access_mode);
    }
    
    template <typename T>
    void PrefixImpl<T>::adjustAccessMode(FlagSet<AccessOptions> &access_mode, std::uint64_t address, std::size_t size) const
    {
        if (!isPageAligned(address) || !isPageAligned(size)) {
            // use create logic only in case of page-aligned address ranges (address & size)
            // otherwise data outside of the range but within the page may not be retrieved
            if (access_mode[AccessOptions::create]) {
                access_mode.set(AccessOptions::create, false);
            }
            // apply read flag to fetch contents from outside of the range
            if (!access_mode[AccessOptions::read]) {
                access_mode.set(AccessOptions::read, true);
            }
        }        
    }
    
    template <typename T>
    MemLock PrefixImpl<T>::mapRange(std::uint64_t address, std::size_t size, std::uint64_t state_num, 
        FlagSet<AccessOptions> access_mode) const
    {
        assert(state_num > 0);
        // create flag must be accompanied by write flag
        assert(!access_mode[AccessOptions::create] || access_mode[AccessOptions::write]);
        // for atomic operations use no_flush flag to allow reverting changes
        if (m_atomic) {
            access_mode.set(AccessOptions::no_flush, true);
        }

        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        
        std::shared_ptr<BaseLock> lock;
        if (end_page == first_page + 1) {
            // adjust access mode sice the requested range may not be well aligned
            adjustAccessMode(access_mode, address, size);
            lock = mapPage(first_page, state_num, access_mode);
        } else {
            auto addr_offset = address & (m_page_size - 1);
            // boundary ranges are NOT page aligned
            if ((end_page == first_page + 2) && addr_offset) {                
                // create mode not allowed for boundary range
                access_mode.set(AccessOptions::create, false);
                lock = mapBoundaryRange(first_page, address, size, state_num, access_mode);
            } else {
                // wide ranges must be page aligned / no need to adjust access mode
                assert(!addr_offset && "Wide range must be page aligned");                
                lock = mapWideRange(first_page, end_page, state_num, access_mode);
            }
        }

        assert(lock);
        // fetch data from storage if not initialized
        return { lock->getBuffer(address), lock };
    }
    
    template <typename T>
    std::shared_ptr<BoundaryLock> PrefixImpl<T>::mapBoundaryRange(std::uint64_t first_page_num, std::uint64_t address, std::size_t size,
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode) const
    {
        std::uint64_t read_state_num;
        std::shared_ptr<BoundaryLock> lock;
        std::shared_ptr<ResourceLock> lhs, rhs;
        while (!lock) {
            lock = m_cache.findBoundaryRange(first_page_num, address, size, state_num, access_mode, read_state_num);
            if (!lock) {
                // fetch lhs & rhs so that findBoundaryRange works for the next iteration
                lhs = mapPage(first_page_num, state_num, access_mode | AccessOptions::read);
                rhs = mapPage(first_page_num + 1, state_num, access_mode | AccessOptions::read);
            }
        }

        assert(read_state_num > 0);
        assert(!access_mode[AccessOptions::create] && "Unable to create boundary range");
        if (access_mode[AccessOptions::write]) {
            assert(getAccessType() == AccessType::READ_WRITE);
            // read / write access
            if (read_state_num != state_num) {
                // possibly create CowS of lhs / rhs
                if (!lhs) {
                    lhs = mapPage(first_page_num, state_num, access_mode | AccessOptions::read);
                }
                if (!rhs) {
                    rhs = mapPage(first_page_num + 1, state_num, access_mode | AccessOptions::read);
                }
                // ... and finally the BoundaryLock on top of existing lhs / rhs locks
                lock = m_cache.insertCopy(address, size, lhs, rhs, state_num, access_mode);
            }
        }

        return lock;
    }
    
    template <typename T>
    std::shared_ptr<ResourceLock> PrefixImpl<T>::mapPage(std::uint64_t page_num, std::uint64_t state_num,
        FlagSet<AccessOptions> access_mode) const
    {
        std::uint64_t read_state_num;
        auto lock = m_cache.findRange(page_num, page_num + 1, state_num, access_mode, read_state_num);
        assert(!lock || read_state_num > 0);
        if (access_mode[AccessOptions::create] && !access_mode[AccessOptions::read]) {
            assert(getAccessType() == AccessType::READ_WRITE);
            // create/write-only access
            if (!lock || read_state_num != state_num) {
                // we don't need reading thus passing read_state_num = 0
                // create / write page
                assert(access_mode[AccessOptions::create]);
                lock = m_cache.createRange(page_num, page_num + 1, 0, state_num, access_mode);
            }
            assert(lock);
        } else if (!access_mode[AccessOptions::write]) {
            // read-only access
            if (!lock) {
                // find the relevant mutation ID (aka state number) if this is read-only access
                auto mutation_id = m_storage.findMutation(page_num, state_num);
                // create range under the mutation ID
                // since access is read-only we pass write_state_num = 0
                lock = m_cache.createRange(page_num, page_num + 1, mutation_id, 0, access_mode);
            }
        } else {
            assert(getAccessType() == AccessType::READ_WRITE);
            // read / write access
            if (lock) {
                if (read_state_num != state_num) {
                    // create a new lock as a copy of existing read_lock (CoW)
                    lock = m_cache.insertCopy(*lock, state_num, access_mode);
                }
            } else {
                // try identifying the last available mutation (may not exist yet)
                std::uint64_t mutation_id = 0;
                m_storage.tryFindMutation(page_num, state_num, mutation_id);
                // unable to read if mutation does not exist
                if (mutation_id) {
                    // read / write page
                    lock = m_cache.createRange(page_num, page_num + 1, mutation_id, state_num, access_mode);
                } else {
                    // create / write page
                    access_mode.set(AccessOptions::read, false);
                    lock = m_cache.createRange(page_num, page_num + 1, 0, state_num, access_mode | AccessOptions::create);
                }
            }
        }

        assert(lock);
        return lock;
    }
    
    template <typename T>
    std::shared_ptr<ResourceLock> PrefixImpl<T>::mapWideRange(std::uint64_t first_page, std::uint64_t end_page, 
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode) const
    {
        std::uint64_t read_state_num;
        auto lock = m_cache.findRange(first_page, end_page, state_num, access_mode, read_state_num);
        assert(!lock || read_state_num > 0);
        if (access_mode[AccessOptions::create] && !access_mode[AccessOptions::read]) {
            assert(getAccessType() == AccessType::READ_WRITE);
            // create/write-only access
            if (!lock || read_state_num != state_num) {
                lock = m_cache.createRange(first_page, end_page, 0, state_num, access_mode);
            }
            assert(lock);
        } else if (!access_mode[AccessOptions::write]) {
            // read-only access
            if (!lock) {
                // a consistent mutation must exist for a wide-lock
                // since wide locks can be occupied by a single object only (page aligned)
                auto mutation_id = findUniqueMutation(first_page, end_page, state_num);
                lock = m_cache.createRange(first_page, end_page, mutation_id, 0, access_mode);
            }
        } else {
            assert(getAccessType() == AccessType::READ_WRITE);
            // read/write access
            if (lock) {
                if (read_state_num != state_num) {
                    // create a new lock as a copy of existing read_lock (CoW)                    
                    lock = m_cache.insertCopy(*lock, state_num, access_mode);
                }
            } else {
                // identify the last available mutation (must exist for reading)
                std::uint64_t mutation_id = 0;
                tryFindUniqueMutation(first_page, end_page, state_num, mutation_id);
                // unable to read if mutation does not exist
                assert(mutation_id > 0 || !access_mode[AccessOptions::read]);                
                // we pass both read & write state numbers here
                lock = m_cache.createRange(first_page, end_page, mutation_id, state_num, access_mode);
            }
        }

        assert(lock);
        return lock;
    }    

    template <typename T> std::uint64_t PrefixImpl<T>::size() const {
        THROWF(db0::InternalException) << "not implemented" << THROWF_END;
    }

    template <typename T> std::uint64_t PrefixImpl<T>::getStateNum() const {
        return m_state_num;
    }
    
    template <typename T> std::uint64_t PrefixImpl<T>::commit()
    {
        m_cache.flush();
        if (m_storage.flush()) {
            // increment state number only if there were any changes
            ++m_state_num;
        }
        return m_state_num;
    }
    
    template <typename T> void PrefixImpl<T>::close()
    {
        m_cache.clear();
        m_storage.close();
    }

    template <typename T> std::uint64_t PrefixImpl<T>::getStateNum(
        std::uint64_t storage_max_state_num, std::optional<std::uint64_t> user_state_num) const
    {
        if (user_state_num) {
            if (*user_state_num > storage_max_state_num) {
                THROWF(db0::InputException) << "Invalid state number: " << *user_state_num << THROWF_END;
            }
            return *user_state_num;
        }
        return storage_max_state_num;
    }

    template <typename T> const PrefixCache &PrefixImpl<T>::getCache() const {
        return m_cache;
    }
    
    template <typename T> std::uint64_t PrefixImpl<T>::refresh()
    {
        // remove updated pages from the cache
        // so that the new version can be fetched when needed
        auto result = m_storage.refresh([this](std::uint64_t updated_page_num, std::uint64_t state_num) {
            // mark page-wide range as missing
            m_cache.markRangeAsMissing(updated_page_num, updated_page_num + 1, state_num);
        });

        if (result) {
            // retrieve the refreshed state number
            m_state_num = m_storage.getMaxStateNum();
        }
        return result;
    }
    
    template <typename T> std::uint64_t PrefixImpl<T>::getLastUpdated() const {
        return m_storage.getLastUpdated();
    }

    template <typename T> std::shared_ptr<Prefix> PrefixImpl<T>::getSnapshot(std::optional<std::uint64_t> state_num_req) const
    {   
        auto state_num = state_num_req.value_or(m_state_num);
        return std::shared_ptr<Prefix>(
            new PrefixViewImpl<PrefixImpl<T> >(
                const_cast<PrefixImpl<T> *>(this)->shared_from_this(), state_num));
    }

    template <typename T> void PrefixImpl<T>::beginAtomic()
    {
        assert(!m_atomic);
        // increment state number to allow isolation
        ++m_state_num;
        m_atomic = true;
    }
    
    template <typename T> void PrefixImpl<T>::endAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
    }
    
    template <typename T> void PrefixImpl<T>::cancelAtomic()
    {
        assert(m_atomic);
        m_cache.rollback(m_state_num);
        --m_state_num;
        m_atomic = false;        
    }
    
    template <typename T> std::uint64_t PrefixImpl<T>::findUniqueMutation(std::uint64_t first_page,
        std::uint64_t end_page, std::uint64_t state_num) const
    {
        assert(end_page > first_page);
        std::uint64_t result = 0;
        for (; first_page < end_page; ++first_page) {
            auto mutation_id = m_storage.findMutation(first_page, state_num);
            if (result == 0) {
                result = mutation_id;
            } else if (result != mutation_id) {
                THROWF(db0::InternalException) << "Inconsistent mutations found in a wide range" << THROWF_END;
            }
        }
        return result;
    }

    template <typename T> bool PrefixImpl<T>::tryFindUniqueMutation(std::uint64_t first_page, std::uint64_t end_page,
        std::uint64_t state_num, std::uint64_t &mutation_id) const
    {
        std::uint64_t result = 0;
        bool has_mutation = false;
        for (;first_page < end_page; ++first_page) {
            if (m_storage.tryFindMutation(first_page, state_num, result)) {
                if (has_mutation && result != mutation_id) {
                    THROWF(db0::InternalException) << "Inconsistent mutations found in a wide range" << THROWF_END;
                }
                mutation_id = result;
                has_mutation = true;
            }
        }
        return has_mutation;
    }

    template <typename T> BaseStorage &PrefixImpl<T>::getStorage() const {
        return m_storage;
    }

} 