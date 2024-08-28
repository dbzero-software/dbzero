#pragma once

#include <dbzero/core/exception/Exceptions.hpp>
#include "SnapshotCache.hpp"
#include "utils.hpp"

namespace db0

{
    
    /**
     * @tparam PrefixT the base prefix
     * @tparam PrefixImplT the actual prefix instance to be used (PrefixImplT must be copy constructible from PrefixT)
     */
    template <typename StorageT> class PrefixViewImpl: public Prefix
    {
    public:
        /**
         * @param head_cache the head transaction's cache
         */
        PrefixViewImpl(const std::string &name, std::shared_ptr<StorageT> storage, const PrefixCache &head_cache, 
            std::uint64_t state_num)
            : Prefix(name)
            , m_storage(storage)
            , m_storage_ptr(storage.get())            
            , m_head_cache(head_cache)
            , m_cache(*m_storage_ptr, head_cache.getCacheRecycler())
            , m_state_num(state_num)
            , m_page_size(m_head_cache.getPageSize())
            , m_shift(getPageShift(m_page_size))
        {
        }

        MemLock mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> = {}) override;
        
        std::uint64_t getStateNum() const override;
        
        std::size_t getPageSize() const override;

        std::uint64_t commit() override;

        std::uint64_t getLastUpdated() const override;

        void close() override;
        
        std::uint64_t refresh() override;

        AccessType getAccessType() const override;

        std::shared_ptr<Prefix> getSnapshot(std::optional<std::uint64_t> state_num = {}) const override;

        BaseStorage &getStorage() const override;

    private:
        std::shared_ptr<StorageT> m_storage;
        StorageT *m_storage_ptr;
        const PrefixCache &m_head_cache;
        // snapshot's private cache instance
        mutable SnapshotCache m_cache;
        // immutable snapshot's state number
        const std::uint64_t m_state_num;
        const std::size_t m_page_size;
        const std::uint32_t m_shift;

        std::shared_ptr<DP_Lock> mapPage(std::uint64_t page_num);
        std::shared_ptr<BoundaryLock> mapBoundaryRange(std::uint64_t page_num, std::uint64_t address, std::size_t size);
        std::shared_ptr<WideLock> mapWideRange(std::uint64_t first_page, std::uint64_t end_page, 
            std::size_t size);

        inline bool isPageAligned(std::uint64_t addr_or_size) const {
            return (addr_or_size & (m_page_size - 1)) == 0;
        }
    };
    
    template <typename StorageT>
    MemLock PrefixViewImpl<StorageT>::mapRange(std::uint64_t address, std::size_t size, 
        FlagSet<AccessOptions> access_mode)
    {
        // read-only access is allowed
        assert(!access_mode[AccessOptions::create] && !access_mode[AccessOptions::write]);

        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        
        std::shared_ptr<ResourceLock> lock;
        if (end_page == first_page + 1) {
            lock = mapPage(first_page);
        } else {
            auto addr_offset = address & (m_page_size - 1);
            // boundary ranges are NOT page aligned
            if ((end_page == first_page + 2) && addr_offset) {                
                lock = mapBoundaryRange(first_page, address, size);
            } else {
                // wide ranges must be page aligned / no need to adjust access mode
                assert(!addr_offset && "Wide range must be page aligned");                
                lock = mapWideRange(first_page, end_page, size);
            }
        }

        assert(lock);
        // fetch data from storage if not initialized
        return { lock->getBuffer(address), lock };    
    }    

    template <typename StorageT> std::size_t PrefixViewImpl<StorageT>::getPageSize() const {
        return m_cache.getPageSize();
    }

    template <typename StorageT> AccessType PrefixViewImpl<StorageT>::getAccessType() const {
        // read-only access
        return AccessType::READ_ONLY;
    }
    
    template <typename StorageT> std::uint64_t PrefixViewImpl<StorageT>::getStateNum() const {
        return m_state_num;
    }

    template <typename StorageT> std::uint64_t PrefixViewImpl<StorageT>::commit() {
        THROWF(db0::InternalException)
            << "PrefixViewImpl::commit: cannot commit snapshot" << THROWF_END;        
    }

    template <typename StorageT>
    std::uint64_t PrefixViewImpl<StorageT>::getLastUpdated() const 
    {
        THROWF(db0::InternalException)
            << "PrefixViewImpl::getLastUpdated: cannot get last updated timestamp from snapshot" << THROWF_END;
    } 

    template <typename StorageT> void PrefixViewImpl<StorageT>::close()
    {
        // close does nothing
    }

    template <typename StorageT> std::uint64_t PrefixViewImpl<StorageT>::refresh() {
        // refresh does nothing
        return 0;
    }
    
    template <typename StorageT>
    std::shared_ptr<Prefix> PrefixViewImpl<StorageT>::getSnapshot(std::optional<std::uint64_t>) const
    {
        THROWF(db0::InternalException) 
            << "PrefixViewImpl::getSnapshot: cannot create snapshot from snapshot" << THROWF_END;
    }
    
    template <typename StorageT>
    BaseStorage &PrefixViewImpl<StorageT>::getStorage() const {
        return *m_storage_ptr;
    }
    
    template <typename StorageT>
    std::shared_ptr<DP_Lock> PrefixViewImpl<StorageT>::mapPage(std::uint64_t page_num)
    {
        // read-only access
        std::uint64_t read_state_num;
        auto lock = m_cache.findPage(page_num, m_state_num, { AccessOptions::read }, read_state_num);        
        if (!lock) {
            std::uint64_t mutation_id;
            // page may not be available in storage yet, in such case we pick from the head cache using state_num
            if (!m_storage_ptr->tryFindMutation(page_num, m_state_num, mutation_id)) {
                mutation_id = m_state_num;
            }
            // try looking up the head transaction's cache next (using the actual mutation ID)
            lock = m_head_cache.findPage(page_num, mutation_id, { AccessOptions::read }, read_state_num);            
            if (lock && read_state_num == mutation_id) {
                // add head transaction's range to the local cache under actual mutation ID
                m_cache.insert(lock, mutation_id);
            } else {
                // fetch the range into local cache (as read-only)
                lock = m_cache.createPage(page_num, mutation_id, 0, { AccessOptions::read });
            }
        }
        
        assert(lock);
        return lock;
    }
    
    template <typename StorageT>
    std::shared_ptr<WideLock> PrefixViewImpl<StorageT>::mapWideRange(
        std::uint64_t first_page, std::uint64_t end_page, std::size_t size)
    {
        std::uint64_t read_state_num;        
        auto lock = m_cache.findRange(first_page, end_page, m_state_num, { AccessOptions::read }, read_state_num);
        
        if (!lock) {
            bool has_res = !isPageAligned(size);
            std::uint64_t mutation_id;            
            if (!tryFindUniqueMutation(*m_storage_ptr, first_page, end_page - (has_res ? 1: 0), m_state_num, mutation_id)) {
                mutation_id = m_state_num;
            }

            // try looking up the head transaction's cache next (using the actual mutation ID)
            lock = m_head_cache.findRange(first_page, size, mutation_id, { AccessOptions::read }, read_state_num);
            if (lock && read_state_num == mutation_id) {
                // insert under the actual mutation ID
                m_cache.insertWide(lock, mutation_id);
            } else {
                std::shared_ptr<DP_Lock> res_dp;
                if (has_res) {
                    res_dp = mapPage(end_page - 1);
                }
                lock = m_cache.createRange(first_page, size, mutation_id, 0, { AccessOptions::read }, res_dp);
            }
        }

        assert(lock);
        return lock;
    }    

    template <typename StorageT>
    std::shared_ptr<BoundaryLock> PrefixViewImpl<StorageT>::mapBoundaryRange(std::uint64_t first_page_num, 
        std::uint64_t address, std::size_t size)
    {
        std::uint64_t read_state_num;
        std::shared_ptr<BoundaryLock> lock;
        std::shared_ptr<DP_Lock> lhs, rhs;
        while (!lock) {
            lock = m_cache.findBoundaryRange(first_page_num, address, size, m_state_num, { AccessOptions::read }, read_state_num);
            if (!lock) {
                // fetch lhs & rhs so that findBoundaryRange works for the next iteration
                lhs = mapPage(first_page_num);
                rhs = mapPage(first_page_num + 1);
            }
        }

        assert(lock);
        return lock;
    }

}
