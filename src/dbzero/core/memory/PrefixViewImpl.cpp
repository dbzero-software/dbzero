// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PrefixViewImpl.hpp"

namespace db0

{
    
    PrefixViewImpl::PrefixViewImpl(const std::string &name, std::shared_ptr<BaseStorage> storage,
        const PrefixCache &head_cache, StateNumType state_num)
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
    
    MemLock PrefixViewImpl::mapRange(std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode)
    {
        // read-only access is allowed
        assert(!access_mode[AccessOptions::write]);
        
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        
        std::shared_ptr<ResourceLock> lock;
        // use no_cow flag since PrevixView is read-only
        access_mode.set(AccessOptions::no_cow, true);
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
                lock = mapWideRange(first_page, end_page, address, size);
            }
        }

        assert(lock);
        // fetch data from storage if not initialized
        return { lock->getBuffer(address), lock };    
    }    

    std::size_t PrefixViewImpl::getPageSize() const {
        return m_cache.getPageSize();
    }
    
    AccessType PrefixViewImpl::getAccessType() const {
        // read-only access
        return AccessType::READ_ONLY;
    }
    
    StateNumType PrefixViewImpl::getStateNum(bool) const {
        // snapshots work only on finalized states
        return m_state_num;
    }
    
    std::uint64_t PrefixViewImpl::commit(ProcessTimer *) 
    {
        THROWF(db0::InternalException)
            << "PrefixViewImpl::commit: cannot commit snapshot" << THROWF_END;        
    }
        
    std::uint64_t PrefixViewImpl::getLastUpdated() const
    {
        THROWF(db0::InternalException)
            << "PrefixViewImpl::getLastUpdated: cannot get last updated timestamp from snapshot" << THROWF_END;
    }
    
    void PrefixViewImpl::close(ProcessTimer *) {
        // close does nothing
    }
    
    std::shared_ptr<Prefix> PrefixViewImpl::getSnapshot(std::optional<StateNumType>) const
    {
        THROWF(db0::InternalException) 
            << "PrefixViewImpl::getSnapshot: cannot create snapshot from snapshot" << THROWF_END;
    }
    
    BaseStorage &PrefixViewImpl::getStorage() const {
        return *m_storage_ptr;
    }
    
    std::shared_ptr<DP_Lock> PrefixViewImpl::mapPage(std::uint64_t page_num)
    {
        // read-only access
        StateNumType read_state_num = 0;
        auto lock = m_cache.findPage(page_num, m_state_num, { AccessOptions::read }, read_state_num);
        if (!lock) {
            StateNumType mutation_id;
            // page may not be available in storage yet, in such case we pick from the head cache using state_num
            if (!m_storage_ptr->tryFindMutation(page_num, m_state_num, mutation_id)) {
                mutation_id = m_state_num;
            }
            // try looking up the head transaction's cache next (using the actual mutation ID)
            lock = m_head_cache.findPage(page_num, mutation_id, { AccessOptions::read }, read_state_num);            
            if (lock && read_state_num == mutation_id) {
                // this is to prevent the lock from being overwritten by future transactions
                lock->freeze();
                // add head transaction's range to the local cache under actual mutation ID
                m_cache.insert(lock, mutation_id);
            } else {
                // fetch the range into local cache (as read-only)
                // and to be retrieved from the storage
                lock = m_cache.createPage(page_num, mutation_id, 0, { AccessOptions::read }, nullptr);
            }
        }
        
        assert(lock);
        return lock;
    }
    
    std::shared_ptr<WideLock> PrefixViewImpl::mapWideRange(
        std::uint64_t first_page, std::uint64_t end_page, std::uint64_t address, std::size_t size)
    {        
        StateNumType read_state_num = 0;
        auto lock_info = m_cache.findRange(first_page, end_page, address, size, m_state_num, 
            { AccessOptions::read }, read_state_num);
        if (!lock_info.second && lock_info.first) {
            // repeat the operation with residual lock
            auto res_lock = mapPage(end_page - 1);
            lock_info = m_cache.findRange(first_page, end_page, address, size, m_state_num, { AccessOptions::read }, 
                read_state_num, res_lock);
        }
        
        assert(!lock_info.first || lock_info.second);
        auto lock = lock_info.second;
        if (!lock) {
            bool has_res = !isPageAligned(size);
            StateNumType mutation_id;
            if (!tryFindUniqueMutation(*m_storage_ptr, first_page, end_page - (has_res ? 1: 0), m_state_num, mutation_id)) {
                mutation_id = m_state_num;
            }

            // try looking up the head transaction's cache next (using the actual mutation ID)
            lock = m_head_cache.findRange(first_page, end_page, address, size, mutation_id, { AccessOptions::read }, read_state_num).second;
            if (lock && read_state_num == mutation_id) {
                // this is to prevent the lock from being overwritten by future transactions
                lock->freeze();
                // feed into the local cache under the actual mutation ID
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
    
    std::shared_ptr<BoundaryLock> PrefixViewImpl::mapBoundaryRange(std::uint64_t first_page_num,
        std::uint64_t address, std::size_t size)
    {
        StateNumType read_state_num = 0;
        std::shared_ptr<BoundaryLock> lock;
        std::shared_ptr<DP_Lock> lhs, rhs;
        while (!lock) {
            lock = m_cache.findBoundaryRange(first_page_num, address, size, m_state_num, { AccessOptions::read },
                read_state_num, lhs, rhs);
            if (!lock) {
                // fetch lhs & rhs so that findBoundaryRange works for the next iteration
                lhs = mapPage(first_page_num);
                rhs = mapPage(first_page_num + 1);
            }
        }

        assert(lock);
        return lock;
    }
    
    std::size_t PrefixViewImpl::getDirtySize() const {
        // snapshot is read-only
        return 0;
    }
    
    std::size_t PrefixViewImpl::flushDirty(std::size_t limit) {
        // snapshot is read-only
        return 0;
    }

}
