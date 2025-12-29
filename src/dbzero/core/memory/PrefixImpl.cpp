// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PrefixImpl.hpp"
#include "CacheRecycler.hpp"

namespace db0

{
    
    PrefixImpl::PrefixImpl(std::string name, std::atomic<std::size_t> &dirty_meter, CacheRecycler *cache_recycler_ptr,
        std::shared_ptr<BaseStorage> storage)
        : Prefix(name)
        , m_storage(storage)
        , m_storage_ptr(m_storage.get())
        , m_access_type(m_storage_ptr->getAccessType())
        , m_page_size(m_storage_ptr->getPageSize())
        , m_shift(getPageShift(m_page_size))
        , m_head_state_num(m_storage_ptr->getMaxStateNum())
        , m_cache(*m_storage_ptr, cache_recycler_ptr, &dirty_meter)
    {
        assert(m_storage_ptr);
        if (m_storage_ptr->getAccessType() == AccessType::READ_WRITE) {
            // increment state number for read-write storage (i.e. new data transaction)
            ++m_head_state_num;
        }
    }

    PrefixImpl::PrefixImpl(std::string name, std::atomic<std::size_t> &dirty_meter, CacheRecycler &cache_recycler,
        std::shared_ptr<BaseStorage> storage)
        : PrefixImpl(name, dirty_meter, &cache_recycler, storage)
    {
    }
        
    MemLock PrefixImpl::PrefixImpl::mapRange(std::uint64_t address,
        std::size_t size, FlagSet<AccessOptions> access_mode)
    {
        return mapRange(address, size, m_head_state_num, access_mode);
    }
    
    void PrefixImpl::adjustAccessMode(FlagSet<AccessOptions> &access_mode,
        std::uint64_t address, std::size_t size) const
    {
        if (!isPageAligned(address) || !isPageAligned(size)) {
            // apply read flag to fetch contents from outside of the range
            if (!access_mode[AccessOptions::read]) {
                access_mode.set(AccessOptions::read, true);
            }
        }
    }
    
    MemLock PrefixImpl::mapRange(std::uint64_t address, std::size_t size, StateNumType state_num,
        FlagSet<AccessOptions> access_mode)
    {
        assert(state_num > 0);
        assert(size > 0);
        // for atomic operations use no_flush flag to allow reverting changes
        if (m_atomic) {
            access_mode.set(AccessOptions::no_flush, true);
        }
        
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        
        std::shared_ptr<ResourceLock> lock;
        // use no_cow flag for read-only access
        if (m_access_type == AccessType::READ_ONLY) {
            access_mode.set(AccessOptions::no_cow, true);
        }
        if (end_page == first_page + 1) {
            // adjust access mode since the requested range may not be well aligned
            adjustAccessMode(access_mode, address, size);
            lock = mapPage(first_page, state_num, access_mode);
        } else {
            auto addr_offset = address & (m_page_size - 1);
            if (isBoundaryRange(first_page, end_page, addr_offset)) {
                // create mode not allowed for boundary range                
                lock = mapBoundaryRange(first_page, address, size, state_num, access_mode | AccessOptions::read);
            } else {
                assert(!addr_offset && "Wide range must be page aligned");
                lock = mapWideRange(first_page, end_page, address, size, state_num, access_mode);
            }
        }
        
        assert(lock);
        // fetch data from storage if not initialized
        return { lock->getBuffer(address), lock };
    }
    
    std::shared_ptr<BoundaryLock> PrefixImpl::mapBoundaryRange(
        std::uint64_t first_page_num, std::uint64_t address, std::size_t size, StateNumType state_num, 
        FlagSet<AccessOptions> access_mode)
    {
        StateNumType read_state_num = 0;
        std::shared_ptr<BoundaryLock> lock;
        std::shared_ptr<DP_Lock> lhs, rhs;
        while (!lock) {
            lock = m_cache.findBoundaryRange(
                first_page_num, address, size, state_num, access_mode, read_state_num, lhs, rhs
            );
            if (!lock) {
                // fetch lhs & rhs so that findBoundaryRange works for the next iteration
                lhs = mapPage(first_page_num, state_num, access_mode | AccessOptions::read);
                rhs = mapPage(first_page_num + 1, state_num, access_mode | AccessOptions::read);
            }
        }
        
        assert(read_state_num > 0);
        assert(access_mode[AccessOptions::read] && "Unable to create boundary range");
        if (access_mode[AccessOptions::write]) {
            assert(getAccessType() == AccessType::READ_WRITE);
            // read / write access
            if (read_state_num != state_num) {
                // possibly create CoWs of lhs / rhs
                if (!lhs) {
                    lhs = mapPage(first_page_num, state_num, access_mode | AccessOptions::read);
                }
                if (!rhs) {
                    rhs = mapPage(first_page_num + 1, state_num, access_mode | AccessOptions::read);
                }
                // ... and finally the BoundaryLock on top of the existing lhs / rhs locks
                lock = m_cache.insertCopy(address, size, *lock, lhs, rhs, state_num, access_mode);
            }            
        }

        return lock;
    }
    
    std::shared_ptr<DP_Lock> PrefixImpl::mapPage(std::uint64_t page_num, StateNumType state_num,
        FlagSet<AccessOptions> access_mode)
    {
        StateNumType read_state_num = 0;
        auto lock = m_cache.findPage(page_num, state_num, access_mode, read_state_num);
        assert(!lock || read_state_num > 0);
        if (access_mode[AccessOptions::write] && !access_mode[AccessOptions::read]) {
            assert(getAccessType() == AccessType::READ_WRITE);
            // create/write-only access
            if (!lock || read_state_num != state_num) {
                if (!lock && m_access_type == AccessType::READ_WRITE) {
                    // try identifying the last available mutation (may not exist yet)
                    StateNumType mutation_id = 0;
                    m_storage_ptr->tryFindMutation(page_num, state_num, mutation_id);
                    if (!mutation_id) {
                        // create / write page
                        access_mode.set(AccessOptions::create, true);
                    }
                }
                lock = m_cache.createPage(page_num, 0, state_num, access_mode, lock);
            }
            assert(lock);
            // since locked for write, must be the same state number
            assert(lock->getStateNum() == state_num);
        } else if (!access_mode[AccessOptions::write]) {
            // read-only access
            if (!lock) {
                // find the relevant mutation ID (aka state number) if this is read-only access
                auto mutation_id = m_storage_ptr->findMutation(page_num, state_num);
                // create page under the mutation ID
                // since access is read-only we pass write_state_num = 0
                // clear the no_flush (volatile) flag since lock is from past transaction
                if (access_mode[AccessOptions::no_flush]) {
                    access_mode.set(AccessOptions::no_flush, false);
                    // assert lock is from past transaction
                    assert(mutation_id < state_num);
                }
                lock = m_cache.createPage(page_num, mutation_id, 0, access_mode);
            }
        } else {
            assert(getAccessType() == AccessType::READ_WRITE);
            // read / write access
            if (lock) {
                if (read_state_num != state_num) {
                    // create a new lock as a copy of existing read_lock (CoW)
                    lock = m_cache.insertCopy(lock, state_num, access_mode);
                }
            } else {
                // try identifying the last available mutation (may not exist yet)
                StateNumType mutation_id = 0;
                m_storage_ptr->tryFindMutation(page_num, state_num, mutation_id);
                // unable to read if mutation does not exist
                if (mutation_id) {
                    // read / write page
                    lock = m_cache.createPage(page_num, mutation_id, state_num, access_mode);
                } else {
                    // create / write page
                    access_mode.set(AccessOptions::read, false);
                    // use AccessOptions::create to indicate a newly created page
                    lock = m_cache.createPage(page_num, 0, state_num, access_mode | AccessOptions::create);
                }
            }
            // since locked for read/write, must be the same state number
            assert(lock->getStateNum() == state_num);
        }
        
        assert(lock);
        return lock;
    }
    
    std::shared_ptr<WideLock> PrefixImpl::mapWideRange(
        std::uint64_t first_page, std::uint64_t end_page, std::uint64_t address, std::size_t size, 
        StateNumType state_num, FlagSet<AccessOptions> access_mode)
    {
        StateNumType read_state_num = 0;
        auto lock_info = m_cache.findRange(first_page, end_page, address, size, state_num, access_mode, read_state_num);
        if (!lock_info.second && lock_info.first) {
            // retrieve the residual lock and repeat the operation
            auto res_lock = mapPage(end_page - 1, state_num, access_mode | AccessOptions::read);
            lock_info = m_cache.findRange(first_page, end_page, address, size, state_num, access_mode, read_state_num, res_lock);
        }
        
        assert(!lock_info.first || lock_info.second);
        auto lock = lock_info.second;
        // flag indicating if the residual part of the wide lock is present
        bool has_res = !isPageAligned(size);
        assert(!lock || read_state_num > 0);
        
        if (access_mode[AccessOptions::write] && !access_mode[AccessOptions::read]) {
            assert(getAccessType() == AccessType::READ_WRITE);
            // create/write-only access
            if (!lock || read_state_num != state_num) {
                std::shared_ptr<DP_Lock> res_lock;
                if (has_res) {
                    // read or create the residual DP
                    res_lock = mapPage(end_page - 1, state_num, access_mode | AccessOptions::read);
                }
                // passing previous lock's version for CoW
                lock = m_cache.createRange(first_page, size, 0, state_num, access_mode, res_lock, lock);
            }
            assert(lock);
        } else if (!access_mode[AccessOptions::write]) {
            // read-only access
            if (!lock) {
                // a consistent mutation must exist for the entire wide-lock (with the exception of the residual DP)
                StateNumType mutation_id = 0;
                std::shared_ptr<DP_Lock> res_dp;
                if (has_res) {
                    mutation_id = db0::findUniqueMutation(*m_storage_ptr, first_page, end_page - 1, state_num);
                    res_dp = mapPage(end_page - 1, state_num, access_mode | AccessOptions::read);                    
                } else {
                    mutation_id = db0::findUniqueMutation(*m_storage_ptr, first_page, end_page, state_num);
                }
                // clear the no_flush (volatile) flag since lock is from past transaction
                if (access_mode[AccessOptions::no_flush]) {
                    access_mode.set(AccessOptions::no_flush, false);
                    // assert lock is from past transaction
                    assert(mutation_id < state_num);
                }
                lock = m_cache.createRange(first_page, size, mutation_id, 0, access_mode, res_dp);
            }
        } else {
            assert(getAccessType() == AccessType::READ_WRITE);
            // read/write access
            if (lock) {
                if (read_state_num != state_num) {
                    // create a new lock as a copy of existing read_lock (CoW)
                    std::shared_ptr<DP_Lock> res_lock;
                    if (has_res) {
                        res_lock = mapPage(end_page - 1, state_num, access_mode | AccessOptions::read);
                    }
                    lock = m_cache.insertWideCopy(lock, state_num, access_mode, res_lock);
                }
            } else {
                // identify the last available mutation (must exist for reading)
                StateNumType mutation_id = 0;
                std::shared_ptr<DP_Lock> res_dp;
                if (has_res) {
                    db0::tryFindUniqueMutation(*m_storage_ptr, first_page, end_page - 1, state_num, mutation_id);
                    res_dp = mapPage(end_page - 1, state_num, access_mode | AccessOptions::read);
                }
                
                // unable to read if mutation does not exist
                assert(mutation_id > 0 || !access_mode[AccessOptions::read]);
                if (!mutation_id) {
                    access_mode.set(AccessOptions::create, true);
                }
                // we pass both read & write state numbers here
                lock = m_cache.createRange(first_page, size, mutation_id, state_num, access_mode, res_dp);
            }
        }

        assert(lock);
        return lock;
    }
    
    StateNumType PrefixImpl::getStateNum(bool finalized) const
    {
        // NOTE: must apply atomic operation adjustment
        int adjust = m_atomic ? -1 : 0;
        if (finalized) {
            // in case of read/write prefixes the head state number is never finalized
            return (m_access_type == AccessType::READ_WRITE) ? (m_head_state_num - 1 + adjust):(m_head_state_num + adjust);
        } else {
            return m_head_state_num + adjust;
        }
    }
    
    std::uint64_t PrefixImpl::commit(ProcessTimer *parent_timer)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (parent_timer) {
            timer = std::make_unique<ProcessTimer>("Prefix::commit", parent_timer);
        }
        m_storage_ptr->beginCommit();
        m_cache.commit(timer.get());
        if (m_storage_ptr->flush(timer.get())) {
            // increment state number only if there were any changes
            ++m_head_state_num;
        }
        m_storage_ptr->endCommit();
        return m_head_state_num;
    }
    
    void PrefixImpl::close(ProcessTimer *timer_ptr)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (timer_ptr) {
            timer = std::make_unique<ProcessTimer>("Prefix::close", timer_ptr);
        }
#ifndef NDEBUG
        m_cache.release();
#endif
        m_storage_ptr->close();
    }
    
    PrefixCache &PrefixImpl::getCache() const {
        return m_cache;
    }
    
    bool PrefixImpl::beginRefresh() {
        return m_storage_ptr->beginRefresh();
    }
    
    std::uint64_t PrefixImpl::completeRefresh()
    {
        m_cache.beginRefresh();
        // remove updated pages from the cache
        // so that the new version can be fetched when needed
        auto result = m_storage_ptr->completeRefresh([this](std::uint64_t updated_page_num, std::uint64_t state_num) {
            m_cache.markAsMissing(updated_page_num, state_num);
        });
        
        if (result) {
            // retrieve & sync to the refreshed state number
            m_head_state_num = m_storage_ptr->getMaxStateNum();
        }
        return result;
    }
    
    std::uint64_t PrefixImpl::getLastUpdated() const {
        return m_storage_ptr->getLastUpdated();
    }
    
    std::shared_ptr<Prefix> PrefixImpl::getSnapshot(std::optional<StateNumType> state_num_req) const
    {
        auto is_valid_snapshot = [this](std::uint64_t state_num) {
            if (getAccessType() == AccessType::READ_WRITE) {
                return state_num >= 1 && state_num < m_head_state_num;
            } else {
                // when access is read-only we also allow the head state number
                return state_num >= 1 && state_num <= m_head_state_num;
            }
        };
        
        auto snapshot_state_num = m_head_state_num;
        if (state_num_req && !is_valid_snapshot(*state_num_req)) {
            THROWF(db0::InputException) << "Requested state number is not available (" << getName() << "): " << *state_num_req;
        }
        
        if (state_num_req) {
            snapshot_state_num = *state_num_req;
        } else {
            if (getAccessType() == AccessType::READ_WRITE) {
                // For read/write prefix use state_num - 1 to read last fully consistent state            
                if (snapshot_state_num <= 1) {
                    THROWF(db0::InputException) << "Unable to create snapshot, no data transaction yet";
                }
                --snapshot_state_num;       
            }
        }
        
        return std::shared_ptr<Prefix>(
            new PrefixViewImpl(this->getName(), m_storage, m_cache, snapshot_state_num)
        );
    }
    
    void PrefixImpl::beginAtomic()
    {        
        assert(!m_atomic);
        // Flush all boundary locks before the start of a new atomic operation
        // this is to avoid flushing (which in case of the boundary locks - mutates the underlying DPs)
        // during the atomic operation. Otherwise it would result in a data inconsistency - 
        // this is because the atomic operation needs to start over a DP-consistent state
        // Due to the same reason, also flush the residual parts of wide locks
        m_cache.flushBoundary();
        // increment state number to allow isolation
        ++m_head_state_num;
        m_atomic = true;
    }
    
    void PrefixImpl::endAtomic()
    {                
        assert(m_atomic);
        std::vector<std::shared_ptr<ResourceLock> > reused_locks;
        // merge all results into the current transaction
        m_cache.merge(m_head_state_num, m_head_state_num - 1, reused_locks);
        --m_head_state_num;
        m_atomic = false;

        // update reused locks with CacheRecycler
        // this can only be done AFTER completing the atomic operation (as it's a potentially mutable operation)        
        if (m_cache.getCacheRecycler()) {
            auto cache_recycler_ptr = m_cache.getCacheRecycler();
            for (auto &lock: reused_locks) {
                cache_recycler_ptr->update(lock);
            }
        }
    }
    
    void PrefixImpl::cancelAtomic()
    {
        assert(m_atomic);
        m_cache.rollback(m_head_state_num);
        --m_head_state_num;
        m_atomic = false;        
    }
    
    BaseStorage &PrefixImpl::getStorage() const {
        return *m_storage_ptr;
    }
    
    void PrefixImpl::cleanup() const {
        m_cache.clearExpired(m_head_state_num);
    }      
    
    std::size_t PrefixImpl::getDirtySize() const {
        return m_cache.getDirtySize();
    }
    
    std::size_t PrefixImpl::flushDirty(std::size_t limit) {
        return m_cache.flushDirty(limit);
    }
    
    void PrefixImpl::getStats(std::function<void(const std::string &name, std::uint64_t value)> callback) const
    {   
        auto cow_stats = m_cache.getCoWStats();
        callback("dirty_cache_bytes", m_cache.getDirtySize());        
        callback("dirty_dp_total", cow_stats.first);
        callback("dirty_dp_cow", cow_stats.second);
    }

}