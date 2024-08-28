#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/utils.hpp>
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/storage/BaseStorage.hpp>
#include <iostream>
#include "BoundaryLock.hpp"
#include "CacheRecycler.hpp"
#include "WideLock.hpp"

namespace db0

{
    
    inline bool isCreateNew(FlagSet<AccessOptions> access_mode) {
        return access_mode[AccessOptions::create] && !access_mode[AccessOptions::read];
    }

    PrefixCache::PrefixCache(BaseStorage &storage, CacheRecycler *cache_recycler_ptr)
        : m_storage(storage)
        , m_page_size(storage.getPageSize())
        , m_shift(getPageShift(m_page_size)) 
        , m_mask(getPageMask(m_page_size))
        , m_dp_map(m_page_size)
        , m_boundary_map(m_page_size)
        , m_wide_map(m_page_size)
        , m_cache_recycler_ptr(cache_recycler_ptr)
        , m_missing_dp_lock_ptr(std::make_shared<DP_Lock>(storage, 0, 0, FlagSet<AccessOptions> {}, 0, 0, true))
        , m_missing_wide_lock_ptr(std::make_shared<WideLock>(storage, 0, 0, FlagSet<AccessOptions> {}, 0, 0, nullptr, true))
    {
    }
    
    std::shared_ptr<DP_Lock> PrefixCache::createPage(std::uint64_t page_num, std::uint64_t read_state_num,
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode)
    {
        auto lock = std::make_shared<DP_Lock>(m_storage, page_num << m_shift, m_page_size,
            access_mode, read_state_num, state_num, isCreateNew(access_mode));
        // register under the lock's evaluated state number
        m_dp_map.insert(lock->getStateNum(), lock);
        
        // register or update lock with the recycler
        if (lock && m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(lock);
        }

        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(lock);
        }

        return lock;
    }
    
    std::shared_ptr<DP_Lock> PrefixCache::findPage(std::uint64_t page_num, std::uint64_t state_num,
        FlagSet<AccessOptions> access_mode, std::uint64_t &read_state_num) const
    {
        auto lock = m_dp_map.find(state_num, page_num, read_state_num);
        if (lock.get() == m_missing_dp_lock_ptr.get()) {
            // invalidate result, this range is marked as missing
            lock = nullptr;
        }
        
        if (!lock) {
            // not found
            return nullptr;
        }

        // Try upgrading the unused lock to the write state
        // this is to avoid CoW in a writer process
        if (access_mode[AccessOptions::write] && !access_mode[AccessOptions::create] && read_state_num != state_num) {
            // unused lock condition (i.e. might only be used by the CacheRecycler)
            // note that dirty locks cannot be upgraded (otherwise data would be lost)            
            if (!lock->isDirty() && lock.use_count() == (lock->isRecycled() ? 1 : 0) + 1) {
                m_dp_map.erase(state_num, lock);
                // note that this operation may also assign the no_flush flag if it was requested
                lock->updateStateNum(state_num, access_mode[AccessOptions::no_flush]);
                // re-register the upgraded lock under a new state
                // note that the actual lock may span a wider range, avoid passing first_page / end_page here !!
                m_dp_map.insert(state_num, lock);
                read_state_num = state_num;
                // upgraded locks may need to be registered as volatile
                if (access_mode[AccessOptions::no_flush]) {
                    m_volatile_locks.push_back(lock);
                }
            }
        }

        // register / update lock with the recycler (mark as accessed for LRU policy evaluation)
        assert(lock);
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(lock);
        }

        return lock;
    }
    
    std::shared_ptr<WideLock> PrefixCache::createRange(std::uint64_t page_num, std::size_t size,
        std::uint64_t read_state_num, std::uint64_t state_num, FlagSet<AccessOptions> access_mode, 
        std::shared_ptr<DP_Lock> res_dp)
    {
        auto lock = std::make_shared<WideLock>(m_storage, page_num << m_shift, size,
            access_mode, read_state_num, state_num, res_dp, isCreateNew(access_mode));
        // register under the lock's evaluated state number
        m_wide_map.insert(lock->getStateNum(), lock);
        
        // register or update lock with the recycler
        if (lock && m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(lock);
        }

        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(lock);
        }

        return lock;
    }
    
    std::shared_ptr<WideLock> PrefixCache::findRange(std::uint64_t first_page, std::uint64_t end_page, 
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode, std::uint64_t &read_state_num) const
    {
        // wide range must span at least 2 pages
        assert(end_page > first_page + 1);
        auto lock = m_wide_map.find(state_num, first_page, read_state_num);
        if (lock.get() == m_missing_wide_lock_ptr.get()) {
            // invalidate result, this range is marked as missing
            lock = nullptr;
        }
        
        if (!lock) {
            // not found
            return nullptr;
        }
        
        assert(lock->size() > 0);
        // Conflicting lock exists in the same transaction
        if ((((lock->size() - 1) >> m_shift) + 1) != end_page - first_page) {            
            // Report conflicting lock as missing
            if (read_state_num < state_num && !access_mode[AccessOptions::read]) {
                return nullptr;
            }

            assert(false && "Conflicting wide lock found");
            THROWF(db0::InternalException) << "Conflicting wide lock found";
        }
        
        // Try upgrading the unused lock to the write state
        // this is to avoid CoW in a writer process
        if (access_mode[AccessOptions::write] && !access_mode[AccessOptions::create] && read_state_num != state_num) {
            // unused lock condition (i.e. might only be used by the CacheRecycler)
            // note that dirty locks cannot be upgraded (otherwise data would be lost)
            if (!lock->isDirty() && lock.use_count() == (lock->isRecycled() ? 1 : 0) + 1) {
                m_wide_map.erase(state_num, lock);
                // note that this operation may also assign the no_flush flag if it was requested
                lock->updateStateNum(state_num, access_mode[AccessOptions::no_flush]);
                // re-register the upgraded lock under a new state
                // note that the actual lock may span a wider range, avoid passing first_page / end_page here !!
                m_wide_map.insert(state_num, lock);
                read_state_num = state_num;
                // upgraded locks may need to be registered as volatile
                if (access_mode[AccessOptions::no_flush]) {
                    m_volatile_locks.push_back(lock);
                }
            }
        }
        
        // register / update lock with the recycler (mark as accessed for LRU policy evaluation)
        assert(lock);
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(lock);
        }

        return lock;
    }

    std::shared_ptr<BoundaryLock> PrefixCache::findBoundaryRange(std::uint64_t first_page, std::uint64_t address, std::size_t size,
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode, std::uint64_t &read_state_num) const
    {
        auto result = m_boundary_map.find(state_num, first_page, read_state_num);
        if (!result) {
            // if both lhs & rhs parents are available and from the same state, we may create the boundary lock
            std::uint64_t lhs_state_num, rhs_state_num;
            auto lhs = findPage(first_page, state_num, access_mode, lhs_state_num);
            auto rhs = findPage(first_page + 1, state_num, access_mode, rhs_state_num);            
            if (!lhs || !rhs) {
                // inconsitent locks
                return nullptr;
            }
            
            // pick the minimum of the underlying states as the read state number
            read_state_num = std::min(lhs_state_num, rhs_state_num);
            auto lhs_size = ((first_page + 1) << m_shift) - address;
            result = std::make_shared<BoundaryLock>(m_storage, address, lhs, lhs_size, rhs, size - lhs_size, access_mode);
            // register with the read state number
            m_boundary_map.insert(read_state_num, result);
            // the new lock may need to be registered as volatile
            if (access_mode[AccessOptions::no_flush]) {
                m_volatile_boundary_locks.push_back(result);
            }
        }

        // in case of a boundary lock the address must be precisely matched
        // since boundary lock contains only the single allocation's data            
        assert(result->getAddress() == address);

        // note that boundary locks are not cached
        return result;
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
    
    std::shared_ptr<DP_Lock> PrefixCache::insertCopy(const DP_Lock &lock, std::uint64_t write_state_num,
        FlagSet<AccessOptions> access_mode)
    {
        auto result = std::make_shared<DP_Lock>(lock, write_state_num, access_mode);
        m_dp_map.insert(result->getStateNum(), result);

        // register / update lock with the recycler
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(result);
        }
        
        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(result);
        }

        return result;
    }
    
    std::shared_ptr<WideLock> PrefixCache::insertWideCopy(const WideLock &lock, std::uint64_t write_state_num,
        FlagSet<AccessOptions> access_mode, std::shared_ptr<DP_Lock> res_lock)
    {
        auto result = std::make_shared<WideLock>(lock, write_state_num, access_mode, res_lock);
        m_wide_map.insert(result->getStateNum(), result);

        // register / update lock with the recycler
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(result);
        }
        
        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(result);
        }

        return result;
    }

    std::shared_ptr<BoundaryLock> PrefixCache::insertCopy(std::uint64_t address, std::size_t size,
        const BoundaryLock &lock, std::shared_ptr<DP_Lock> lhs, std::shared_ptr<DP_Lock> rhs, 
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode)
    {
        auto lhs_size = ((lhs->getAddress() + lhs->size()) - address);
        auto rhs_size = size - lhs_size;
        assert(!isCreateNew(access_mode) && "Cannot use create mode for BoundaryLocks");
        auto result = std::make_shared<BoundaryLock>(m_storage, address, lock, lhs, lhs_size, rhs, rhs_size, access_mode);
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
        m_volatile_locks.clear();
        m_volatile_boundary_locks.clear();
        // undo write / remove dirty flag from all owned locks
        forEach([&](ResourceLock &lock) {
            lock.resetDirtyFlag();            
        });
        
        // ... and this prefix's locks owned by the recycler
        if (m_cache_recycler_ptr) {
            std::vector<std::shared_ptr<ResourceLock> > locks;
            m_cache_recycler_ptr->forEach([&](std::shared_ptr<ResourceLock> lock) {
                // process only locks associated with this prefix
                if (&lock->getStorage() == &m_storage) {
                    lock->resetDirtyFlag();
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
        m_boundary_map.forEach([&](ResourceLock &lock) {
            lock.flush();
        });
    }

    void PrefixCache::flush()
    {
        // flush all dirty locks with the related storage, this is a synchronous operation
        // note that BoundaryLocks are flushed first, WideLocks next and finally DP_Locks
        forEach([&](ResourceLock &lock) {
            lock.flush();
        });
    }

    void PrefixCache::markAsMissing(std::uint64_t page_num, std::uint64_t state_num)
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

    void PrefixCache::rollback(std::uint64_t state_num)
    {
        // remove all volatile locks
        for (auto &lock: m_volatile_boundary_locks) {
            // erase range
            eraseBoundaryRange(lock->getAddress(), lock->size(), state_num);
        }
        m_volatile_boundary_locks.clear();
        for (auto &lock: m_volatile_locks) {
            // erase range
            eraseRange(lock->getAddress(), lock->size(), state_num);
        }
        m_volatile_locks.clear();
    }
        
    void PrefixCache::eraseRange(std::uint64_t address, std::size_t size, std::uint64_t state_num)
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

    void PrefixCache::eraseBoundaryRange(std::uint64_t address, std::size_t size, std::uint64_t state_num)
    {
        auto first_page = address >> m_shift;
        assert(isBoundaryRange(first_page, ((address + size - 1) >> m_shift) + 1, address & m_mask));
        m_boundary_map.erase(state_num, first_page);
    }
    
    void PrefixCache::replaceRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
        std::shared_ptr<DP_Lock> new_lock)
    {
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        // operation not allowed for the boundary range
        assert(!isBoundaryRange(first_page, end_page, address & m_mask));
        if (end_page == first_page + 1) {
            if (m_dp_map.replace(state_num, new_lock, first_page)) {
                // must clear the no_flush flag if lock was reused
                new_lock->resetNoFlush();
            }
        } else {
            if (m_wide_map.replace(state_num, std::dynamic_pointer_cast<WideLock>(new_lock), first_page)) {
                // must clear the no_flush flag if lock was reused
                new_lock->resetNoFlush();
            }
        }
    }
    
    void PrefixCache::replaceBoundaryRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
        std::shared_ptr<BoundaryLock> new_lock)
    {
        auto first_page = address >> m_shift;
        assert(isBoundaryRange(first_page, ((address + size - 1) >> m_shift) + 1, address & m_mask));
        if (m_boundary_map.replace(state_num, new_lock, first_page)) {
            // must clear the no_flush flag if lock was reused
            new_lock->resetNoFlush();
        }
    }
    
    void PrefixCache::merge(std::uint64_t from_state_num, std::uint64_t to_state_num)
    {
        // merge boundary locks first
        for (auto &lock: m_volatile_boundary_locks) {
            if (lock->isDirty()) {
                // erase volatile range related lock
                eraseBoundaryRange(lock->getAddress(), lock->size(), from_state_num);
                replaceBoundaryRange(lock->getAddress(), lock->size(), to_state_num, lock);
            }
        }
        m_volatile_boundary_locks.clear();

        for (auto &lock: m_volatile_locks) {
            if (lock->isDirty()) {
                // erase volatile range related lock
                eraseRange(lock->getAddress(), lock->size(), from_state_num);
                // need to update to final state number before merging with active transaction
                lock->merge(to_state_num);
                replaceRange(lock->getAddress(), lock->size(), to_state_num, lock);
            }
        }
        m_volatile_locks.clear();
    }
    
    std::size_t PrefixCache::getPageSize() const {
        return m_page_size;
    }
    
    CacheRecycler *PrefixCache::getCacheRecycler() const {
        return m_cache_recycler_ptr;
    }

}