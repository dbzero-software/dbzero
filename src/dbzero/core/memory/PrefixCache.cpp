#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/utils.hpp>
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/storage/BaseStorage.hpp>
#include <iostream>
#include "BoundaryLock.hpp"
#include "CacheRecycler.hpp"

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
        , m_boundary_map(m_page_size)
        , m_page_map(m_page_size)
        , m_cache_recycler_ptr(cache_recycler_ptr)
        , m_missing_lock_ptr(std::make_shared<ResourceLock>(storage, 0, 0, FlagSet<AccessOptions> {}, 0, 0, true))
    {
    }
    
    std::shared_ptr<ResourceLock> PrefixCache::createRange(std::uint64_t first_page, std::uint64_t end_page,
        std::uint64_t read_state_num, std::uint64_t state_num, FlagSet<AccessOptions> access_mode)
    {
        auto lock = std::make_shared<ResourceLock>(m_storage, first_page << m_shift, (end_page - first_page) << m_shift,
            access_mode, read_state_num, state_num, isCreateNew(access_mode));
        // register under the lock's evaluated state number
        m_page_map.insertRange(lock->getStateNum(), lock, first_page, end_page);

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

    std::shared_ptr<ResourceLock> PrefixCache::findRange(std::uint64_t first_page, std::uint64_t end_page, 
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode, std::uint64_t &read_state_num) const
    {
        auto lock = m_page_map.findRange(state_num, first_page, end_page, read_state_num);
        if (lock.get() == m_missing_lock_ptr.get()) {
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
                m_page_map.erase(state_num, lock);
                // note that this operation may also assign the no_flush flag if it was requested
                lock->updateStateNum(state_num, access_mode[AccessOptions::no_flush]);
                // re-register the upgraded lock under a new state
                m_page_map.insertRange(state_num, lock, first_page, first_page + 1);
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
        auto result = m_boundary_map.findPage(state_num, first_page, read_state_num);
        if (!result) {
            // if both lhs & rhs parents are available and from the same state, we may create the boundary lock
            std::uint64_t lhs_state_num, rhs_state_num;
            auto lhs = findRange(first_page, first_page + 1, state_num, access_mode, lhs_state_num);
            auto rhs = findRange(first_page + 1, first_page + 2, state_num, access_mode, rhs_state_num);
            if (!lhs || !rhs) {
                // inconsitent locks
                return nullptr;
            }
            
            // pick the minimum of the underlying states as the read state number
            read_state_num = std::min(lhs_state_num, rhs_state_num);
            auto lhs_size = ((first_page + 1) << m_shift) - address;
            result = std::make_shared<BoundaryLock>(m_storage, address, lhs, lhs_size, rhs, size - lhs_size, access_mode);
            // register with the read state number
            m_boundary_map.insertPage(read_state_num, result, first_page);
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
        m_page_map.forEach([&](const ResourceLock &lock) {
            result += lock.size();
        });
        return result;
    }
    
    std::shared_ptr<ResourceLock> PrefixCache::insertCopy(const ResourceLock &lock,
        std::uint64_t write_state_num, FlagSet<AccessOptions> access_mode)
    {        
        auto first_page = lock.getAddress() >> m_shift;
        auto end_page = ((lock.getAddress() + lock.size() - 1) >> m_shift) + 1;
        auto result = std::make_shared<ResourceLock>(lock, write_state_num, access_mode);
        m_page_map.insertRange(write_state_num, result, first_page, end_page);

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
    
    std::shared_ptr<BoundaryLock> PrefixCache::insertCopy(std::uint64_t address, std::size_t size, std::shared_ptr<ResourceLock> lhs,
        std::shared_ptr<ResourceLock> rhs, std::uint64_t state_num, FlagSet<AccessOptions> access_mode)
    {
        auto lhs_size = ((lhs->getAddress() + lhs->size()) - address);
        auto rhs_size = size - lhs_size;
        assert(!isCreateNew(access_mode) && "Cannot use create mode for BoundaryLocks");
        auto result = std::make_shared<BoundaryLock>(m_storage, address, lhs, lhs_size, rhs, rhs_size, access_mode, false);
        m_boundary_map.insertPage(state_num, result, address >> m_shift);
        
        // note that BoundaryLocks are not recycled
        // register with the volatile locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_boundary_locks.push_back(result);
        }
        
        return result;
    }
    
    void PrefixCache::forEach(std::function<void(BaseLock &)> f) const
    {
        m_boundary_map.forEach(f);
        m_page_map.forEach(f);
    }
    
    void PrefixCache::clear()
    {        
        m_boundary_map.clear();
        m_page_map.clear();    
    }
    
    void PrefixCache::release()
    {
        m_volatile_locks.clear();
        m_volatile_boundary_locks.clear();
        // undo write / remove dirty flag from all owned locks
        forEach([&](BaseLock &lock) {
            lock.resetDirtyFlag();            
        });
        
        // ... and this prefix's locks owned by the recycler
        if (m_cache_recycler_ptr) {
            std::vector<std::shared_ptr<BaseLock> > locks;
            m_cache_recycler_ptr->forEach([&](std::shared_ptr<BaseLock> lock) {
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
        return m_boundary_map.empty() && m_page_map.empty();
    }
    
    void PrefixCache::flush()
    {
        // flush all dirty locks with the related storage, this is a synchronous operation
        // note that BoundaryLocks are flushed first
        forEach([&](BaseLock &lock) {
            lock.flush();
        });
    }
    
    void PrefixCache::markRangeAsMissing(std::uint64_t page, std::uint64_t end_page, std::uint64_t state_num)
    {
        // only mark already existing ranges
        if (m_page_map.rangeExists(state_num, page, end_page)) {
            // mark with the negation lock
            m_page_map.insertRange(state_num, m_missing_lock_ptr, page, end_page);            
        }
    }
    
    void PrefixCache::rollback(std::uint64_t state_num)
    {
        // remove all volatile locks
        for (auto &lock: m_volatile_boundary_locks) {
            // erase range
            eraseRange(lock->getAddress(), lock->size(), state_num);
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
        if (isBoundaryRange(first_page, end_page, address & m_mask)) {
            m_boundary_map.erasePage(state_num, first_page);
        }
        m_page_map.eraseRange(state_num, first_page, end_page);
    }
    
    void PrefixCache::replaceRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
        std::shared_ptr<ResourceLock> new_lock)
    {
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        // operation not allowed for the boundary range
        assert(!isBoundaryRange(first_page, end_page, address & m_mask));
        if (m_page_map.replaceRange(state_num, new_lock, first_page, end_page)) {
            // must clear the no_flush flag if lock was reused
            new_lock->resetNoFlush();
        }
    }
    
    void PrefixCache::merge(std::uint64_t from_state_num, std::uint64_t to_state_num)
    {
        // simply erase volatile boundary locks
        for (auto &lock: m_volatile_boundary_locks) {
            // erase volatile range related lock
            eraseRange(lock->getAddress(), lock->size(), from_state_num);
        }
        m_volatile_boundary_locks.clear();

        for (auto &lock: m_volatile_locks) {
            if (lock->isDirty()) {
                // erase volatile range related lock
                eraseRange(lock->getAddress(), lock->size(), from_state_num);
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