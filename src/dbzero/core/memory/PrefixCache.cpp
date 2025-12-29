// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/utils.hpp>
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/storage/BaseStorage.hpp>
#include <dbzero/core/utils/ProcessTimer.hpp>
#include <iostream>
#include "BoundaryLock.hpp"
#include "CacheRecycler.hpp"
#include "WideLock.hpp"
#include <unordered_set>

namespace db0

{
    
    PrefixCache::PrefixCache(BaseStorage &storage, CacheRecycler *cache_recycler_ptr, std::atomic<std::size_t> *dirty_meter_ptr)
        : m_page_size(storage.getPageSize())
        , m_shift(getPageShift(m_page_size)) 
        , m_mask(getPageMask(m_page_size))        
        , m_storage(storage)
        , m_dirty_dp_cache(m_page_size, dirty_meter_ptr)
        , m_dirty_wide_cache(m_page_size, dirty_meter_ptr)
        , m_dp_context { m_dirty_dp_cache, storage }
        , m_wide_context { m_dirty_wide_cache, storage }
        , m_dp_map(m_page_size)
        , m_boundary_map(m_page_size)
        , m_wide_map(m_page_size)
        , m_cache_recycler_ptr(cache_recycler_ptr)
        , m_missing_dp_lock_ptr(std::make_shared<DP_Lock>(m_dp_context, 0, 0, FlagSet<AccessOptions> {}, 0, 0))
        , m_missing_wide_lock_ptr(std::make_shared<WideLock>(m_wide_context, 0, 0, FlagSet<AccessOptions> {}, 0, 0, nullptr))
    {
    }
    
    std::shared_ptr<DP_Lock> PrefixCache::createPage(std::uint64_t page_num, StateNumType read_state_num,
        StateNumType state_num, FlagSet<AccessOptions> access_mode, std::shared_ptr<ResourceLock> cow_lock)
    {
        bool is_volatile = access_mode[AccessOptions::no_flush];
        // in case of volatile locks, don't pass the CoW lock
        if (is_volatile) {
            cow_lock = nullptr;
        }

        auto lock = std::make_shared<DP_Lock>(m_dp_context, page_num << m_shift, m_page_size,
            access_mode, read_state_num, state_num, cow_lock);
        // register under the lock's evaluated state number
        m_dp_map.insert(lock->getStateNum(), lock);
        
        // register with the volatile locks
        assert(lock);
        if (is_volatile) {
            // NOTE: volatile locks are not registered with the recycler
            m_volatile_locks.push_back(lock);
        } else {
            // register or update lock with the recycler
            if (m_cache_recycler_ptr) {
                m_cache_recycler_ptr->update(lock);
            }
        }

        return lock;
    }
    
    std::shared_ptr<DP_Lock> PrefixCache::findPage(std::uint64_t page_num, StateNumType state_num,
        FlagSet<AccessOptions> access_mode, StateNumType &read_state_num) const
    {
        std::shared_ptr<DP_Lock> dp_lock;
        auto weak_ref = m_dp_map.find(state_num, page_num, read_state_num);
        if (!weak_ref) {
            // not found in cache
            return nullptr;
        }
        
        dp_lock = weak_ref->lock();
        // must restore cache-expired lock
        if (!dp_lock) {
            // NOTE: restored locks are created with "no_cow" flag
            // because the persisted state might not be final
            if (read_state_num == state_num) {
                // the restored lock can be created as read/write if requested
                dp_lock = std::make_shared<DP_Lock>(m_dp_context, page_num << m_shift, m_page_size,
                    access_mode | AccessOptions::read | AccessOptions::no_cow, read_state_num, state_num
                );
            } else {
                // the restored lock is created as "for read"
                dp_lock = std::make_shared<DP_Lock>(m_dp_context, page_num << m_shift, m_page_size,
                    FlagSet<AccessOptions> { AccessOptions::read, AccessOptions::no_cow }, 
                    read_state_num, read_state_num
                );
            }
            // feed the lock back into the cache
            *weak_ref = dp_lock;
        }
        
        if (dp_lock.get() == m_missing_dp_lock_ptr.get()) {
            // invalidate result, this range is marked as missing
            return nullptr;
        }
        
        assert(dp_lock);
        bool is_volatile = access_mode[AccessOptions::no_flush];
            // Try upgrading the unused lock to the write state
            // this is to avoid CoW in a writer process
            if (access_mode[AccessOptions::write] && read_state_num != state_num) {
            // unused lock condition (i.e. might only be used by the CacheRecycler)
            // note that dirty locks cannot be upgraded (otherwise data would be lost)            
            if (dp_lock->allowReuse() && dp_lock.use_count() == (dp_lock->isRecycled() ? 1 : 0) + 1) {
                assert(read_state_num == dp_lock->getStateNum());
                m_dp_map.erase(read_state_num, dp_lock);
                // note that this operation may also assign the no_flush flag if it was requested
                dp_lock->updateStateNum(state_num, is_volatile);
                // re-register the upgraded lock under a new state
                // note that the actual lock may span a wider range, avoid passing first_page / end_page here !!
                m_dp_map.insert(state_num, dp_lock);
                read_state_num = state_num;
                // mark as dirty since write access was requested
                dp_lock->setDirty();
                // upgraded locks may need to be registered as volatile
                if (is_volatile) {
                    m_volatile_locks.push_back(dp_lock);
                }
            }        
        }
        
        // register / update lock with the recycler (mark as accessed for LRU policy evaluation)
        assert(dp_lock);
        if (m_cache_recycler_ptr && !is_volatile) {
            m_cache_recycler_ptr->update(dp_lock);
        }

        return dp_lock;
    }
    
    std::shared_ptr<WideLock> PrefixCache::createRange(std::uint64_t page_num, std::size_t size,
        StateNumType read_state_num, StateNumType state_num, FlagSet<AccessOptions> access_mode,
        std::shared_ptr<DP_Lock> res_dp, std::shared_ptr<ResourceLock> cow_lock)
    {
        bool is_volatile = access_mode[AccessOptions::no_flush];
        // in case of volatile locks, don't pass the CoW lock
        if (is_volatile) {
            cow_lock = nullptr;
        }

        auto lock = std::make_shared<WideLock>(m_wide_context, page_num << m_shift, size,
            access_mode, read_state_num, state_num, res_dp, cow_lock);
        // register under the lock's evaluated state number
        m_wide_map.insert(lock->getStateNum(), lock);
        
        assert(lock);
        // register with the volatile locks
        if (is_volatile) {
            m_volatile_wide_locks.push_back(lock);
        } else {
            // register or update lock with the recycler
            if (m_cache_recycler_ptr) {
                m_cache_recycler_ptr->update(lock);
            }
        }

        return lock;
    }
    
    std::pair<bool, std::shared_ptr<WideLock> > PrefixCache::findRange(std::uint64_t first_page, std::uint64_t end_page, 
        std::uint64_t address, std::size_t size, StateNumType state_num, FlagSet<AccessOptions> access_mode,
        StateNumType &read_state_num, std::shared_ptr<DP_Lock> res_lock) const
    {
        // wide range must span at least 2 pages
        assert(end_page > first_page + 1);
        auto weak_ref = m_wide_map.find(state_num, first_page, read_state_num);
        if (!weak_ref) {
            return { false, nullptr };
        }

        auto wide_lock = weak_ref->lock();
        // must restore cache-expired lock        
        if (!wide_lock) {
            // volatile locks must not expire
            assert(!access_mode[AccessOptions::no_flush]);
            // NOTE: residual lock not required if the wide lock is page-aligned
            if (!res_lock && !isPageAligned(size)) {
                // operation needs to be repeated with the residual lock
                return { true, nullptr };
            }
            // restore lock and feed it back into the cache
            // NOTE: restored locks are created with "no_cow" flag
            if (read_state_num == state_num) {
                // restore as read/write if requested
                wide_lock = std::make_shared<WideLock>(m_wide_context, address, size,
                    access_mode | AccessOptions::read | AccessOptions::no_cow, read_state_num, state_num, res_lock
                );
            } else {
                // restore as "for read"
                wide_lock = std::make_shared<WideLock>(m_wide_context, address, size, 
                    FlagSet<AccessOptions> { AccessOptions::read, AccessOptions::no_cow },
                    read_state_num, read_state_num, res_lock
                );
            }
            // feed the lock back into the cache
            *weak_ref = wide_lock;
        }
        
        if (wide_lock.get() == m_missing_wide_lock_ptr.get()) {
            // invalidate result, this range is marked as missing
            return { false, nullptr };
        }
        
        assert(wide_lock);
        assert(wide_lock->size() > 0);
        // Conflicting lock exists in the same transaction
        if ((((wide_lock->size() - 1) >> m_shift) + 1) != end_page - first_page) {
            // Report conflicting lock as missing
            if (read_state_num < state_num && !access_mode[AccessOptions::read]) {
                return { false, nullptr };
            }

            assert(false && "Conflicting wide lock found");
            THROWF(db0::InternalException) << "Conflicting wide lock found";
        }
        
        bool is_volatile = access_mode[AccessOptions::no_flush];
        // Try upgrading the unused lock to the write state
        // this is to avoid CoW in a writer process
        if (access_mode[AccessOptions::write] && read_state_num != state_num) {
            // Unused lock condition (i.e. might only be used by the CacheRecycler)
            // note that dirty locks cannot be upgraded (otherwise data would be lost)
            // note that, on upgrade, the residual lock must be refreshed as well
            if (wide_lock->allowReuse() && wide_lock.use_count() == (wide_lock->isRecycled() ? 1 : 0) + 1) {
                // have the operation repeated with the res_lock
                if (!res_lock) {
                    return { true, nullptr };
                }
                assert(res_lock);
                assert(read_state_num == wide_lock->getStateNum());
                m_wide_map.erase(read_state_num, wide_lock);
                // note that this operation may also assign the no_flush flag if it was requested
                wide_lock->updateStateNum(state_num, access_mode[AccessOptions::no_flush], res_lock);
                // re-register the upgraded lock under a new state
                // note that the actual lock may span a wider range, avoid passing first_page / end_page here !!
                m_wide_map.insert(state_num, wide_lock);
                // mark lock as dirty since write access was requested
                wide_lock->setDirty();
                read_state_num = state_num;
                // upgraded locks may need to be registered as volatile
                if (is_volatile) {
                    m_volatile_wide_locks.push_back(wide_lock);
                }
            }
        }
        
        // register / update lock with the recycler (mark as accessed for LRU policy evaluation)
        assert(wide_lock);
        if (m_cache_recycler_ptr && !is_volatile) {
            m_cache_recycler_ptr->update(wide_lock);
        }
        
        return { true, wide_lock };
    }
    
    std::shared_ptr<BoundaryLock> PrefixCache::findBoundaryRange(std::uint64_t first_page,
        std::uint64_t address, std::size_t size, StateNumType state_num, FlagSet<AccessOptions> access_mode, 
        StateNumType &read_state_num, std::shared_ptr<DP_Lock> lhs, std::shared_ptr<DP_Lock> rhs) const
    {
        assert((lhs && rhs) || (!lhs && !rhs));
        auto weak_ref = m_boundary_map.find(state_num, first_page, read_state_num);
        std::shared_ptr<BoundaryLock> br_lock;
        if (weak_ref) {
            assert(read_state_num > 0);
            br_lock = weak_ref->lock();
            if (!br_lock) {
                // operation must be repeated with lhs & rhs
                if (!lhs || !rhs) {
                    return {};
                }
                auto lhs_size = ((first_page + 1) << m_shift) - address;
                // restore the lock and feed it back into the cache
                if (read_state_num == state_num) {
                    // create as read/write if requested
                    br_lock = std::make_shared<BoundaryLock>(m_dp_context, address, lhs, lhs_size, rhs, size - lhs_size, 
                        access_mode | AccessOptions::read);
                } else {
                    // restore as "for read"
                    br_lock = std::make_shared<BoundaryLock>(m_dp_context, address, lhs, lhs_size, rhs, size - lhs_size, 
                        FlagSet<AccessOptions> { AccessOptions::read });
                }
                *weak_ref = br_lock;
            }
        }
        
        if (!br_lock) {
            // if both lhs & rhs parents are available and from the same state, we may create the boundary lock
            // and feed it back into the cache
            StateNumType lhs_state_num, rhs_state_num;
            if (lhs && rhs) {
                lhs_state_num = lhs->getStateNum();
                rhs_state_num = rhs->getStateNum();                
            } else {
                lhs = findPage(first_page, state_num, access_mode, lhs_state_num);
                rhs = findPage(first_page + 1, state_num, access_mode, rhs_state_num);
                if (!lhs || !rhs) {
                    // inconsitent locks
                    return {};
                }
            }
            
            // pick the minimum of the underlying states as the read state number
            read_state_num = std::min(lhs_state_num, rhs_state_num);
            // write lock cannot be created due to inconsistent lhs / rhs state numbers
            if (read_state_num != state_num && access_mode[AccessOptions::write]) {
                return {};
            }            
            
            assert(lhs && rhs);
            auto lhs_size = ((first_page + 1) << m_shift) - address;
            // remove the no_flush flag if accessing from a historical transaction (as lock is non-volatile)
            if (read_state_num < state_num) {
                access_mode.clear(AccessOptions::no_flush);
            }

            // NOTE: boundary locks are not registered with the dirty-cache
            // we use dp_context as a placeholder for the dirty-cache
            br_lock = std::make_shared<BoundaryLock>(m_dp_context, address, lhs, lhs_size, rhs, size - lhs_size, access_mode);
            // register with the read state number
            m_boundary_map.insert(read_state_num, br_lock);
            // the new lock may need to be registered as "potentially" volatile because parent locks may already be volatile
            if (access_mode[AccessOptions::no_flush]) {
                m_volatile_boundary_locks.push_back(br_lock);
            }
        }
        
        // in case of a boundary lock the address must be precisely matched
        // since boundary lock contains only the single allocation's data            
        assert(br_lock->getAddress() == address);
        
        // note that boundary locks are not cached (i.e. no CacheRecycler registration)
        return br_lock;
    }
    
    std::size_t PrefixCache::getSizeOfResources() const
    {
        std::size_t result = 0;
        m_dp_map.forEach([&](const DP_Lock &lock) {
            result += lock.size();
        });
        m_wide_map.forEach([&](const WideLock &lock) {
            // include size page-aligned
            result += static_cast<std::size_t>(lock.size() / m_page_size) * m_page_size;            
        });
        return result;
    }
    
    std::shared_ptr<DP_Lock> PrefixCache::insertCopy(std::shared_ptr<DP_Lock> lock, StateNumType write_state_num,
        FlagSet<AccessOptions> access_mode)
    {
        auto result = std::make_shared<DP_Lock>(lock, write_state_num, access_mode);
        m_dp_map.insert(result->getStateNum(), result);

        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {            
            m_volatile_locks.push_back(result);
        } else {
            // register with the recycler
            if (m_cache_recycler_ptr) {
                m_cache_recycler_ptr->update(result);
            }
        }

        return result;
    }
    
    std::shared_ptr<WideLock> PrefixCache::insertWideCopy(std::shared_ptr<WideLock> lock, StateNumType write_state_num,
        FlagSet<AccessOptions> access_mode, std::shared_ptr<DP_Lock> res_lock)
    {
        auto result = std::make_shared<WideLock>(lock, write_state_num, access_mode, res_lock);
        m_wide_map.insert(result->getStateNum(), result);
        
        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {            
            m_volatile_wide_locks.push_back(result);
        } else {
            // register with the recycler
            if (m_cache_recycler_ptr) {
                m_cache_recycler_ptr->update(result);
            }
        }

        return result;
    }
    
    std::shared_ptr<BoundaryLock> PrefixCache::insertCopy(std::uint64_t address, std::size_t size,
        const BoundaryLock &lock, std::shared_ptr<DP_Lock> lhs, std::shared_ptr<DP_Lock> rhs, 
        StateNumType state_num, FlagSet<AccessOptions> access_mode)
    {
        auto lhs_size = ((lhs->getAddress() + lhs->size()) - address);
        auto rhs_size = size - lhs_size;
        assert(access_mode[AccessOptions::read] && "Cannot use create mode for BoundaryLocks");
        // NOTE: boundary locks are not registered with the dirty-cache
        // we use dp_context as a placeholder for the dirty-cache
        auto result = std::make_shared<BoundaryLock>(m_dp_context, address, lock, lhs, lhs_size, rhs, rhs_size, access_mode);        
        assert(state_num == std::min(lhs->getStateNum(), rhs->getStateNum()));
        m_boundary_map.insert(state_num, result);
        
        // note that BoundaryLocks are not recycled
        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_boundary_locks.push_back(result);
        }
        
        return result;
    }
    
    void PrefixCache::forEach(std::function<void(ResourceLock &)> f) const
    {
        m_boundary_map.forEach(f);
        m_wide_map.forEach(f);
        m_dp_map.forEach(f);
    }
    
    void PrefixCache::clear()
    {        
        m_boundary_map.clear();
        m_wide_map.clear();
        m_dp_map.clear();    
    }
    
    void PrefixCache::release()
    {
        discardAll(m_volatile_boundary_locks);
        discardAll(m_volatile_wide_locks);
        discardAll(m_volatile_locks);
        // undo write / remove dirty flag from all owned locks
        forEach([&](ResourceLock &lock) {
            lock.discard();
        });
        
        // ... and this prefix's locks owned by the recycler
        if (m_cache_recycler_ptr) {
            std::vector<std::shared_ptr<ResourceLock> > locks;
            m_cache_recycler_ptr->forEach([&](std::shared_ptr<ResourceLock> lock) {
                // process only locks associated with this prefix
                if (&lock->getStorage() == &m_storage) {
                    lock->discard();
                    locks.push_back(std::move(lock));
                }
            });
            
            // release locks from the recycler
            auto mx = m_cache_recycler_ptr->lock();
            for (auto &lock: locks) {
                m_cache_recycler_ptr->release(*lock, mx);
            }
        }
        clear();
    }
    
    bool PrefixCache::empty() const {
        return m_boundary_map.empty() && m_dp_map.empty() && m_wide_map.empty();
    }
    
    void PrefixCache::flushBoundary()
    {
        m_boundary_map.forEach([&](BoundaryLock &lock) {
            // flush boundary part of the lock
            lock.flushBoundary();
        });
        m_wide_map.forEach([&](WideLock &lock) {
            // flush residual part of the wide locks
            lock.flushResidual();
        });
    }
    
    void PrefixCache::commit(ProcessTimer *parent_timer)
    {
        std::unique_ptr<ProcessTimer> timer;
        if (parent_timer) {
            timer = std::make_unique<ProcessTimer>("PrefixCache::commit", parent_timer);
        }
        // boundary locks need to be flushed first (from the map since they're not cached)
        m_boundary_map.forEach([&](BoundaryLock &lock) {
            // flush the boundary area only, since parent lock will be flushed either as DP or wide locks
            lock.flushBoundary();
        });
        // NOTE: only commit operation can use the FlushMethod::diff method
        // NOTE: we first flush using the diff-method and then again using full-write method
        // this is to reduce the number of interleaved diff / full writes
        // wide locks need to be flushed before dp-locks
        m_dirty_wide_cache.tryFlush(FlushMethod::diff);
        m_dirty_wide_cache.flush();
        // finally flush DP_Locks using the DirtyCache
        m_dirty_dp_cache.tryFlush(FlushMethod::diff);
        m_dirty_dp_cache.flush();        
    }
    
    void PrefixCache::markAsMissing(std::uint64_t page_num, StateNumType state_num)
    {
        // only mark already existing ranges
        if (m_dp_map.exists(state_num, page_num)) {
            // mark with the negation lock
            m_dp_map.insert(state_num, m_missing_dp_lock_ptr, page_num);
        }
        // same process repeat for the wide range
        if (m_wide_map.exists(state_num, page_num)) {
            // mark with the negation lock
            m_wide_map.insert(state_num, m_missing_wide_lock_ptr, page_num);
        }
    }
    
    void PrefixCache::rollback(StateNumType state_num)
    {
        // remove all volatile locks
        for (auto &lock: m_volatile_boundary_locks) {
            // erase range
            eraseBoundaryRange(lock->getAddress(), lock->size(), state_num);
            lock->discard();
        }
        m_volatile_boundary_locks.clear();
        for (auto &lock: m_volatile_wide_locks) {
            // erase range
            eraseRange(lock->getAddress(), lock->size(), state_num);
            lock->discard();
        }
        m_volatile_wide_locks.clear();
        for (auto &lock: m_volatile_locks) {
            // erase range
            eraseRange(lock->getAddress(), lock->size(), state_num);
            lock->discard();
        }
        m_volatile_locks.clear();
    }
    
    void PrefixCache::eraseRange(std::uint64_t address, std::size_t size, StateNumType state_num)
    {
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;        
        assert(!isBoundaryRange(first_page, end_page, address & m_mask));
        if (end_page == first_page + 1) {
            m_dp_map.erase(state_num, first_page);
        } else {
            // erase wide range
            m_wide_map.erase(state_num, first_page);
        }
    }
    
    void PrefixCache::eraseBoundaryRange(std::uint64_t address, std::size_t size, StateNumType state_num)
    {
        auto first_page = address >> m_shift;
        assert(isBoundaryRange(first_page, ((address + size - 1) >> m_shift) + 1, address & m_mask));
        m_boundary_map.erase(state_num, first_page);
    }
    
    std::shared_ptr<DP_Lock> PrefixCache::replaceRange(std::uint64_t address, std::size_t size, StateNumType state_num,
        std::shared_ptr<DP_Lock> new_lock)
    {
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        // operation not allowed for the boundary range
        assert(!isBoundaryRange(first_page, end_page, address & m_mask));
        if (end_page == first_page + 1) {
            auto existing_lock = m_dp_map.replace(state_num, new_lock, first_page);
            if (existing_lock) {
                assert(!existing_lock->isVolatile());
                return existing_lock;
            } else {
                // must clear the no_flush flag if lock was reused
                new_lock->resetNoFlush();
                return {};
            }
        } else {
            auto existing_lock = m_wide_map.replace(state_num, std::dynamic_pointer_cast<WideLock>(new_lock), first_page);
            if (existing_lock) {
                assert(!existing_lock->isVolatile());
                return existing_lock;
            } else {
                // must clear the no_flush flag if lock was reused
                new_lock->resetNoFlush();
                return {};
            }
        }
    }
    
    bool PrefixCache::replaceBoundaryRange(std::uint64_t address, std::size_t size, StateNumType state_num,
        std::shared_ptr<BoundaryLock> new_lock)
    {
        auto first_page = address >> m_shift;
        assert(isBoundaryRange(first_page, ((address + size - 1) >> m_shift) + 1, address & m_mask));
        auto existing_lock = m_boundary_map.replace(state_num, new_lock, first_page);
        if (existing_lock) {
            assert(!existing_lock->isVolatile());
            return true;
        } else {
            // must clear the no_flush flag if lock was reused (i.e. added to cache under a different state)
            new_lock->resetNoFlush();            
            return false;
        }
    }
    
    void PrefixCache::merge(StateNumType from_state_num, StateNumType to_state_num,
        std::vector<std::shared_ptr<ResourceLock> > &reused_locks)
    {
        // Remove all volatile boundary locks before merge, to flush them
        // into the underlying parent DP-locks
        // This is fine because in the head transaction we've released all boundary locks
        // so they need not to be merged
        m_volatile_boundary_locks.clear();
        
        // first collect residual parts from dirty wide locks
        // which need to be trated as dirty, otherwise may not be merged / flushed properly
        std::unordered_set<const ResourceLock*> dirty_res_locks;
        for (auto &lock: m_volatile_wide_locks) {
            if (lock->isDirty()) {
                // collect dirty residual locks
                dirty_res_locks.insert(lock->getResLockPtr());
            }
        }

        std::unordered_map<const ResourceLock*, std::shared_ptr<DP_Lock> > rebase_map;
        // merge DP-locks first
        for (auto &lock: m_volatile_locks) {
            // erase volatile range related lock
            eraseRange(lock->getAddress(), lock->size(), from_state_num);
            // NOTE: volatile locks may not be dirty if first accessed as read-only from the atomic context
            if (lock->isDirty() || dirty_res_locks.count(lock.get())) {
                // need to update to final state number (head) before merging with active transaction
                lock->merge(to_state_num);
                auto existing_lock = replaceRange(lock->getAddress(), lock->size(), to_state_num, lock);
                // register the mapping to existing locks
                if (existing_lock) {
                    rebase_map[lock.get()] = existing_lock;
                } else {
                    reused_locks.push_back(lock);
                }
            }
        }
        m_volatile_locks.clear();
        
        // merge wide locks next & rebase residual locks if needed
        for (auto &lock: m_volatile_wide_locks) {
            // erase volatile range related lock
            eraseRange(lock->getAddress(), lock->size(), from_state_num);
            // NOTE: volatile locks may not be dirty if first accessed as read-only from the atomic context
            if (lock->isDirty()) {
                // need to update to final state number (head) before merging with active transaction
                lock->merge(to_state_num);
                auto existing_lock = replaceRange(lock->getAddress(), lock->size(), to_state_num, lock);
                if (existing_lock) {
                    rebase_map[lock.get()] = existing_lock;
                } else {
                    // need to rebase parent locks if the boundary lock was reused
                    lock->rebase(rebase_map);
                    reused_locks.push_back(lock);
                }
            }
        }
        m_volatile_wide_locks.clear();        
    }
    
    std::size_t PrefixCache::getPageSize() const {
        return m_page_size;
    }
    
    CacheRecycler *PrefixCache::getCacheRecycler() const {
        return m_cache_recycler_ptr;
    }
    
    void PrefixCache::clearExpired(StateNumType head_state_num) const
    {
        m_dp_map.clearExpired(head_state_num);
        m_boundary_map.clearExpired(head_state_num);
        m_wide_map.clearExpired(head_state_num);
    }
    
    std::size_t PrefixCache::getDirtySize() const {
        return m_dirty_dp_cache.size() + m_dirty_wide_cache.size();
    }
    
    std::size_t PrefixCache::flushDirty(std::size_t limit)
    {
        std::size_t result = 0;
        result += m_dirty_wide_cache.flush(limit);
        if (result >= limit) {
            return result;
        }
        result += m_dirty_dp_cache.flush(limit - result);
        return result;
    }
    
    const PageMap<DP_Lock> &PrefixCache::getDPMap() const {
        return m_dp_map;
    }
    
    const PageMap<BoundaryLock> &PrefixCache::getBoundaryMap() const {
        return m_boundary_map;
    }
    
    const PageMap<WideLock> &PrefixCache::getWideMap() const {
        return m_wide_map;
    }
    
    void PrefixCache::beginRefresh()
    {
        // NOTE boundary map may contain non-expired locks - e.g. ones supported by Snapshot (e.g. PrefixViewImpl)
        // there must be no volatile locks
        assert(m_volatile_locks.empty());
        assert(m_volatile_wide_locks.empty());
        assert(m_volatile_boundary_locks.empty());
        // clear all expired locks from the boundary map
        m_boundary_map.clear();
    }
    
    std::pair<std::uint64_t, std::uint64_t> PrefixCache::getCoWStats() const
    {
        std::uint64_t dp_total = 0;
        std::uint64_t dp_cow = 0;
        m_dirty_dp_cache.forAll([&](const ResourceLock &lock) {
            ++dp_total;
            if (lock.hasCoWData()) {
                ++dp_cow;
            }
        });

        m_dirty_wide_cache.forAll([&](const ResourceLock &lock) {
            auto dp_count = (lock.size() / m_page_size);
            dp_total += dp_count;
            if (lock.hasCoWData()) {
                dp_cow += dp_count;
            }
        });

        return { dp_total, dp_cow };
    }
        
} 