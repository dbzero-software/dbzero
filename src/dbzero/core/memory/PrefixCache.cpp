#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/utils.hpp>
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <iostream>
#include "BoundaryLock.hpp"
#include "CacheRecycler.hpp"

namespace db0

{
    
    inline bool isCreateNew(FlagSet<AccessOptions> access_mode) {
        return access_mode[AccessOptions::create] && !access_mode[AccessOptions::read];
    }
    
    PrefixCache::PrefixCache(StorageView &storage_view, CacheRecycler *cache_recycler_ptr)
        : m_storage_view(storage_view)
        , m_page_size(storage_view.get().first.get().getPageSize())
        , m_shift(getPageShift(m_page_size)) 
        , m_mask(getPageMask(m_page_size))
        , m_cache_recycler_ptr(cache_recycler_ptr)
        , m_missing_lock_ptr(std::make_shared<ResourceLock>(storage_view, 0, 0, FlagSet<AccessOptions> {}))
    {
    }

    std::shared_ptr<ResourceLock> PrefixCache::createLock(std::uint64_t page_num, 
        FlagSet<AccessOptions> access_mode) const
    {
        return std::make_shared<ResourceLock>(m_storage_view, page_num << m_shift, m_page_size,
            access_mode, isCreateNew(access_mode));        
    }

    std::shared_ptr<ResourceLock> PrefixCache::createWideLock(std::uint64_t first_page, std::uint64_t end_page,
        FlagSet<AccessOptions> access_mode) const
    {
        return std::make_shared<ResourceLock>(m_storage_view, first_page << m_shift, (end_page - first_page) << m_shift,
            access_mode, isCreateNew(access_mode));
    }        

    /*
    std::shared_ptr<ResourceLock> PrefixCache::createLock(std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode) const
    {
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;

        // boundary range (spanning two pages)
        if (isBoundaryRange(first_page, end_page)) {
            assert(end_page == first_page + 2);
            // create lhs & rhs parent locks
            auto lhs = createLock(first_page << m_shift, m_page_size, access_mode);
            auto rhs = createLock((first_page + 1) << m_shift, m_page_size, access_mode);
            auto lhs_size = ((first_page + 1) << m_shift) - address;
            return std::make_shared<BoundaryLock>(m_storage_view, address, lhs, lhs_size, rhs, size - lhs_size, 
                access_mode, isCreateNew(access_mode));
        } else {
            return std::make_shared<ResourceLock>(m_storage_view, first_page << m_shift, (end_page - first_page) << m_shift,
                access_mode, isCreateNew(access_mode));
        }
    }
    */

    std::shared_ptr<ResourceLock> PrefixCache::createPage(std::uint64_t page_num, std::uint64_t state_num,
        FlagSet<AccessOptions> access_mode)
    {
        auto result = std::make_shared<ResourceLock>(m_storage_view, page_num << m_shift, m_page_size,
            access_mode, isCreateNew(access_mode));
        m_page_map.insertRange(state_num, result, page_num, page_num + 1);

        // register or update lock with the recycler
        if (result && m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(result);
        }

        // register with the persistent locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(result);
        }

        return result;
    }

    std::shared_ptr<ResourceLock> PrefixCache::createRange(std::uint64_t first_page, std::uint64_t end_page,
        std::uint64_t state_num, FlagSet<AccessOptions> access_mode)
    {
        // wide ranges must be page aligned
        // FIXME: unblock below assert when implementation is ready
        // assert(size <= m_page_size / 2 || ((address & m_mask) == 0));

        assert(!isBoundaryRange(first_page, end_page));
        auto result = std::make_shared<ResourceLock>(m_storage_view, first_page << m_shift, (end_page - first_page) << m_shift,
            access_mode, isCreateNew(access_mode));
        m_page_map.insertRange(state_num, result, first_page, end_page);

        // register or update lock with the recycler
        if (result && m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(result);
        }

        // register with the persistent locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(result);
        }

        return result;
    }
    
    std::shared_ptr<ResourceLock> PrefixCache::findOrCreatePage(std::uint64_t state_num, std::uint64_t page_num,
        FlagSet<AccessOptions> access_mode)
    {
        std::uint64_t existing_state_num;
        auto page = m_page_map.findPage(state_num, page_num, &existing_state_num);
        if (!page || existing_state_num != state_num) {
            page = createPage(page_num, state_num, access_mode);
        }
        return page;
    }

    std::shared_ptr<ResourceLock> PrefixCache::createBoundaryRange(std::uint64_t first_page, std::uint64_t address,
        std::size_t size, std::uint64_t state_num, FlagSet<AccessOptions> access_mode)
    {
        // wide ranges must be page aligned
        // FIXME: unblock below assert when implementation is ready
        // assert(size <= m_page_size / 2 || ((address & m_mask) == 0));

        std::uint64_t result_state_num;
        auto result = m_boundary_map.findPage(state_num, first_page, &result_state_num);
        if (!result || result_state_num != state_num) {
            // find or create lhs & rhs parent ranges
            // note that either or both of the ranges might be already present in the cache                
            auto lhs = findOrCreatePage(state_num, first_page, access_mode);
            auto rhs = findOrCreatePage(state_num, first_page + 1, access_mode);
            auto lhs_size = ((first_page + 1) << m_shift) - address;
            result = std::make_shared<BoundaryLock>(m_storage_view, address, lhs, lhs_size, rhs, size - lhs_size, 
                access_mode, isCreateNew(access_mode));
            m_boundary_map.insertPage(state_num, result, first_page);
        }

        // in case of a boundary lock the address must be precisely matched
        // since boundary lock contains only the single allocation's data            
        assert(result->getAddress() == address);

        // register with volatile locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(result);
        }

        // note that boundary ranges are not cached
        return result;
    }   

    std::shared_ptr<ResourceLock> PrefixCache::findPage(std::uint64_t page_num, std::uint64_t state_num,
        std::uint64_t *result_state_num) const
    {
        auto result = m_page_map.findRange(state_num, page_num, page_num + 1, result_state_num);
        if (result.get() == m_missing_lock_ptr.get()) {
            // invalidate result, this range is marked as missing
            result = nullptr;
        }        
        
        // register / update lock with the recycler (mark as accessed for LRU policy evaluation)
        if (result && m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(result);
        }

        return result;
    }

    std::shared_ptr<ResourceLock> PrefixCache::findRange(std::uint64_t first_page, std::uint64_t end_page, std::uint64_t state_num,
        std::uint64_t *result_state_num) const
    {
        // wide ranges must be page aligned
        // FIXME: unblock below assert when implementation is ready
        // assert(size <= m_page_size / 2 || ((address & m_mask) == 0));

        auto result = m_page_map.findRange(state_num, first_page, end_page, result_state_num);
        if (result.get() == m_missing_lock_ptr.get()) {
            // invalidate result, this range is marked as missing
            result = nullptr;
        }        
        
        // register / update lock with the recycler (mark as accessed for LRU policy evaluation)
        if (result && m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(result);
        }

        return result;
    }

    std::shared_ptr<ResourceLock> PrefixCache::findBoundaryRange(std::uint64_t first_page, std::uint64_t address, std::size_t size,
        std::uint64_t state_num, std::uint64_t *result_state_num) const
    {
        auto result = m_boundary_map.findPage(state_num, first_page, result_state_num);
        if (!result) {
            // if both lhs & rhs parents are available and from the same state, we may create the boundary lock
            std::uint64_t lhs_state_num, rhs_state_num;
            auto lhs = m_page_map.findPage(state_num, first_page, &lhs_state_num);
            auto rhs = m_page_map.findPage(state_num, first_page + 1, &rhs_state_num);
            if (!lhs || !rhs) {
                // inconsitent locks
                return nullptr;
            }
            
            // pick the minimum state number for reading
            lhs_state_num = std::min(lhs_state_num, rhs_state_num);
            auto lhs_size = ((first_page + 1) << m_shift) - address;
            result = std::make_shared<BoundaryLock>(m_storage_view, address, lhs, lhs_size, rhs, 
                size - lhs_size, FlagSet<AccessOptions>());
            m_boundary_map.insertPage(state_num, result, first_page);

            if (result_state_num) {
                *result_state_num = lhs_state_num;
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
        m_page_map.forEach([&](const ResourceLock &lock) {
            result += lock.size();
        });
        return result;
    }

    std::shared_ptr<ResourceLock> PrefixCache::insertCopy(std::uint64_t state_num, const ResourceLock &lock,
        std::uint64_t src_state_num, FlagSet<AccessOptions> access_mode)
    {
        auto page_num = lock.getAddress() >> m_shift;
        auto result = std::make_shared<ResourceLock>(lock, src_state_num, access_mode);
        m_page_map.insertRange(state_num, result, page_num, page_num + 1);

        // register / update lock with the recycler
        if (m_cache_recycler_ptr) {
            m_cache_recycler_ptr->update(result);
        }
        
        // register with the persistent locks
        if (access_mode[AccessOptions::no_flush]) {
            m_volatile_locks.push_back(result);
        }

        return result;
    }

    void PrefixCache::forEach(std::function<void(ResourceLock &)> f) const 
    {
        m_boundary_map.forEach(f);
        m_page_map.forEach(f);
    }
    
    void PrefixCache::clear()
    {
        m_volatile_locks.clear();
        // undo write / remove dirty flag from all owned locks
        forEach([&](ResourceLock &lock) {            
            lock.resetDirtyFlag();            
        });

        // ... and this prefix's locks owned by the recycler
        if (m_cache_recycler_ptr) {
            std::vector<std::shared_ptr<ResourceLock> > locks;
            m_cache_recycler_ptr->forEach([&](std::shared_ptr<ResourceLock> lock) {
                // process only locks associated with this prefix
                if (lock->getStorageView() == m_storage_view) {
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
        
        m_boundary_map.clear();
        m_page_map.clear();
    }
    
    bool PrefixCache::empty() const {
        return m_boundary_map.empty() && m_page_map.empty();
    }
    
    void PrefixCache::flush()
    {
        // flush all dirty locks with the related storage, this is a synchronous operation
        // note that BoundaryLocks are flushed first
        forEach([&](ResourceLock &lock) {
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
        // likely boundary range
        if (isBoundaryRange(first_page, end_page)) {
            assert(end_page == first_page + 2);
            m_boundary_map.erasePage(state_num, first_page);
        }
        m_page_map.eraseRange(state_num, first_page, end_page);        
    }

    void PrefixCache::replaceRange(std::uint64_t address, std::size_t size, std::uint64_t state_num,
        std::shared_ptr<ResourceLock> new_lock)
    {
        auto first_page = address >> m_shift;
        auto end_page = ((address + size - 1) >> m_shift) + 1;
        // likely boundary range
        if (isBoundaryRange(first_page, end_page)) {
            assert(end_page == first_page + 2);
            m_boundary_map.replacePage(state_num, std::dynamic_pointer_cast<BoundaryLock>(new_lock),
                first_page);            
        } else {
            m_page_map.replaceRange(state_num, new_lock, first_page, end_page);
        }
    }
    
    void PrefixCache::merge(std::uint64_t from_state_num, std::uint64_t to_state_num)
    {
        for (auto &lock: m_volatile_locks) {
            // erase volatile range
            eraseRange(lock->getAddress(), lock->size(), from_state_num);            
            replaceRange(lock->getAddress(), lock->size(), to_state_num, lock);
        }
        m_volatile_locks.clear();
    }

}