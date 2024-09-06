#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <mutex>
#include <dbzero/core/memory/DP_Lock.hpp>
#include <dbzero/core/memory/BoundaryLock.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/utils.hpp>

namespace db0

{   
    
    class CacheRecycler;
    
    /**
     * @tparam ResourceLockT the resource lock type
     * @tparam StateKeyT the state number type (can be either a simple state number or write/read state)
     */
    template <typename ResourceLockT> class PageMap
    {
    public:
        PageMap(std::size_t page_size);

        // page_num, state_num
        using PageKeyT = std::pair<std::uint64_t, std::uint64_t>;
        
        /**
         * Check if the DP exists without returning its parameters
        */
        bool exists(std::uint64_t state_num, std::uint64_t page_num) const;

        std::shared_ptr<ResourceLockT> find(std::uint64_t state_num, std::uint64_t page_num,
            std::uint64_t &read_state_num) const;
        
        void insert(std::uint64_t state_num, std::shared_ptr<ResourceLockT>);

        void insert(std::uint64_t state_num, std::shared_ptr<ResourceLockT>, std::uint64_t page_num);

        void forEach(std::function<void(const ResourceLockT &)>) const;

        void forEach(std::function<void(ResourceLockT &)>);

        // @return true if the lock was reused (inserted into the existing range)
        bool replace(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t page_num);

        void clear();

        bool empty() const;

        std::size_t size() const;
                
    protected:
        // NOTE: since erase operations may potentially lead to inconsistencies 
        // (e.g. data not available in cache and not flushed to storage yet)
        // we need to only perform them from a well researched contexts
        friend class PrefixCache;
        
        // Erase lock stored under a known state number
        void erase(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock);
        void erase(std::uint64_t state_num, std::uint64_t page_num);

    private:        
        const unsigned int m_shift;
        mutable std::shared_mutex m_rw_mutex;

        struct CompT {
            inline bool operator()(const PageKeyT &k1, const PageKeyT &k2) const {
                return k1.first < k2.first || (k1.first == k2.first && k1.second < k2.second);
            }
        };

        // page-wise cache, note that a single DP_Lock may be associated with multiple pages
        mutable std::map<PageKeyT, std::weak_ptr<ResourceLockT>, CompT> m_cache;        
        using CacheIterator = typename decltype(m_cache)::iterator;

        CacheIterator find(std::uint64_t page_num, std::uint64_t state_num) const;

        // Erase ALL locks with a given page number where state < state_num
        // irrespective of their use count, this is required for handling inconsistent locks problem
        void eraseAll(std::uint64_t page_num, std::uint64_t state_num) const;

        // remove expired locks only
        // @return the number of removed (expired) locks
        std::size_t clearExpired();
    };
    
    template <typename ResourceLockT>
    PageMap<ResourceLockT>::PageMap(std::size_t page_size)
        : m_shift(getPageShift(page_size))
    {
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insert(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock)
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        m_cache[{lock->getAddress() >> m_shift, state_num}] = lock;
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insert(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, 
        std::uint64_t page_num)
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        m_cache[{page_num, state_num}] = lock;
    }

    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::forEach(std::function<void(const ResourceLockT &)> f) const 
    {
        std::shared_lock<std::shared_mutex> _lock(m_rw_mutex);
        for (const auto &p: m_cache) {
            auto lock = p.second.lock();
            if (lock) {
                f(*lock);
            }
        }
    }

    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::forEach(std::function<void(ResourceLockT &)> f) 
    {
        std::shared_lock<std::shared_mutex> _lock(m_rw_mutex);
        for (const auto &p: m_cache) {
            auto lock = p.second.lock();
            if (lock) {
                f(*lock);
            }
        }
    }

    template <typename ResourceLockT>
    bool PageMap<ResourceLockT>::exists(std::uint64_t state_num, std::uint64_t page_num) const
    {
        std::shared_lock<std::shared_mutex> _lock(m_rw_mutex);        
        return find(page_num, state_num) != m_cache.end();
    }
    
    template <typename ResourceLockT>
    std::shared_ptr<ResourceLockT> PageMap<ResourceLockT>::find(std::uint64_t state_num, std::uint64_t page_num,
        std::uint64_t &read_state_num) const
    {
        // needs to be unique locked due to potential m_cache::erase operation
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        auto it = find(page_num, state_num);
        if (it == m_cache.end()) {
            return nullptr;
        }
        auto lock = it->second.lock();
        if (!lock) {
            // remove expired weak_ptr
            m_cache.erase(it);
            return nullptr;
        }        
        read_state_num = it->first.second;
        return lock;
    }
    
    template <typename ResourceLockT>
    typename PageMap<ResourceLockT>::CacheIterator PageMap<ResourceLockT>::find(
        std::uint64_t page_num, std::uint64_t state_num) const
    {
        auto it = m_cache.lower_bound({page_num, state_num});
        if (it == m_cache.end() && !m_cache.empty()) {
            --it;
        }
        if (it != m_cache.begin() && (it->first.second > state_num || it->first.first != page_num)) {
            --it;
        }
        if (it->first.first == page_num && it->first.second <= state_num) {
            return it;
        }
        return m_cache.end();
    }

    template <typename ResourceLockT> void PageMap<ResourceLockT>::eraseAll(
        std::uint64_t page_num, std::uint64_t state_num) const
    {
        auto it = m_cache.lower_bound({page_num, state_num});
        if (it == m_cache.end() && !m_cache.empty()) {
            --it;
        }
        if (it != m_cache.begin() && (it->first.second > state_num || it->first.first != page_num)) {
            --it;
        }
        // NOTE: we're NOT erasing locks exactly matching the state number
        while (it->first.first == page_num && it->first.second < state_num) {
            it = m_cache.erase(it);
        }
    }

    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::erase(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock)
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        auto page_num = lock->getAddress() >> m_shift;                
        auto it = find(page_num, state_num);
        assert(it != m_cache.end());
        assert(it->second.lock() == lock);
        m_cache.erase(it);        
    }
    
    template <typename ResourceLockT> void PageMap<ResourceLockT>::clear() 
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        m_cache.clear();
    }

    template <typename ResourceLockT> bool PageMap<ResourceLockT>::empty() const 
    {
        std::shared_lock<std::shared_mutex> _lock(m_rw_mutex);
        return m_cache.empty();
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::erase(std::uint64_t state_num, std::uint64_t page_num)
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        m_cache.erase({page_num, state_num});
    }

    template <typename ResourceLockT>
    bool PageMap<ResourceLockT>::replace(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, 
        std::uint64_t page_num)
    {
        // find exact match of the page / state
        auto it = m_cache.find({page_num, state_num});
        if (it == m_cache.end()) {
            insert(state_num, lock);
            return false;
        }
        auto existing_lock = it->second.lock();
        if (!existing_lock) {
            // remove expired weak_ptr
            m_cache.erase(it);
            insert(state_num, lock);
            return false;
        }

        assert(existing_lock->size() == lock->size());
        // apply changes from the lock being merged
        existing_lock->copyFrom(*lock);
        return true;
    }

    template <typename ResourceLockT>
    std::size_t PageMap<ResourceLockT>::size() const 
    {
        std::shared_lock<std::shared_mutex> _lock(m_rw_mutex);
        return m_cache.size();
    }

    template <typename ResourceLockT>
    std::size_t PageMap<ResourceLockT>::clearExpired()
    {
        std::size_t count = 0;
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        for (auto it = m_cache.begin(); it != m_cache.end();) {
            if (it->second.expired()) {
                it = m_cache.erase(it);
                ++count;
            } else {
                ++it;
            }
        }
        return count;
    }

}