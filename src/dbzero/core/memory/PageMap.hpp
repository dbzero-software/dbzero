// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <mutex>
#include "config.hpp"
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
        using PageKeyT = std::pair<std::uint64_t, StateNumType>;
        
        /**
         * Check if the DP exists without returning its parameters
        */
        bool exists(StateNumType state_num, std::uint64_t page_num) const;

        // NOTE: may return expired lock
        // @return nullptr if lock not found
        std::weak_ptr<ResourceLockT> *find(StateNumType state_num, std::uint64_t page_num,
            StateNumType &read_state_num) const;

        void insert(StateNumType state_num, std::shared_ptr<ResourceLockT>);

        void insert(StateNumType state_num, std::shared_ptr<ResourceLockT>, std::uint64_t page_num);

        void forEach(std::function<void(const ResourceLockT &)>) const;

        void forEach(std::function<void(ResourceLockT &)>);

        // @return existing lock updated with the new lock's contents
        std::shared_ptr<ResourceLockT> replace(StateNumType state_num, std::shared_ptr<ResourceLockT> new_lock,
            std::uint64_t page_num);

        void clear();

        bool empty() const;

        std::size_t size() const;
        
        // check if the map contains any non-expired locks
        bool hasLocks() const;
                
    protected:
        // NOTE: since erase operations may potentially lead to inconsistencies 
        // (e.g. data not available in cache and not flushed to storage yet)
        // we need to only perform them from a well researched contexts
        friend class PrefixCache;
        
        void insert(std::unique_lock<std::shared_mutex> &, StateNumType state_num, 
            std::shared_ptr<ResourceLockT>);
        
        // Erase lock stored under a known state number
        void erase(StateNumType state_num, std::shared_ptr<ResourceLockT> lock);
        void erase(StateNumType state_num, std::uint64_t page_num);

        // remove expired locks only up to a specific state number
        // note that the lock with the highest state number below head_state_num is not erased even if it's expired
        // @return the number of removed (expired) locks
        std::size_t clearExpired(StateNumType head_state_num);

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

        CacheIterator findImpl(std::uint64_t page_num, StateNumType state_num) const;
    };
    
    template <typename ResourceLockT>
    PageMap<ResourceLockT>::PageMap(std::size_t page_size)
        : m_shift(getPageShift(page_size))
    {
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insert(StateNumType state_num, std::shared_ptr<ResourceLockT> res_lock)
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        m_cache[{res_lock->getAddress() >> m_shift, state_num}] = res_lock;
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insert(std::unique_lock<std::shared_mutex> &, StateNumType state_num, 
        std::shared_ptr<ResourceLockT> res_lock)
    {
        m_cache[{res_lock->getAddress() >> m_shift, state_num}] = res_lock;
    }

    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insert(StateNumType state_num, std::shared_ptr<ResourceLockT> res_lock,
        std::uint64_t page_num)
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        m_cache[{page_num, state_num}] = res_lock;
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
    bool PageMap<ResourceLockT>::exists(StateNumType state_num, std::uint64_t page_num) const
    {
        std::shared_lock<std::shared_mutex> _lock(m_rw_mutex);
        return findImpl(page_num, state_num) != m_cache.end();
    }
    
    template <typename ResourceLockT>
    std::weak_ptr<ResourceLockT> *PageMap<ResourceLockT>::find(StateNumType state_num, std::uint64_t page_num,
        StateNumType &read_state_num) const
    {        
        std::shared_lock<std::shared_mutex> lock(m_rw_mutex);
        auto it = findImpl(page_num, state_num);
        if (it == m_cache.end()) {
            return nullptr;
        }
        read_state_num = it->first.second;
        return &it->second;
    }
    
    template <typename ResourceLockT>
    typename PageMap<ResourceLockT>::CacheIterator PageMap<ResourceLockT>::findImpl(
        std::uint64_t page_num, StateNumType state_num) const
    {
        if (m_cache.empty()) {
            return m_cache.end();
        }
        
        // Find the first element with key >= {page_num, state_num}
        auto it = m_cache.lower_bound({page_num, state_num});
        
        // If we found exact match or an element with same page_num and state <= state_num
        if (it != m_cache.end() && it->first.first == page_num && it->first.second <= state_num) {
            return it;
        }
        
        // Look backwards for the largest state <= state_num with same page_num
        if (it == m_cache.begin()) {
            return m_cache.end(); // No valid element found
        }
        
        --it; // Safe because we checked it != m_cache.begin()
        
        // Check if this element matches our criteria
        if (it->first.first == page_num && it->first.second <= state_num) {
            return it;
        }
        
        return m_cache.end();
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::erase(StateNumType state_num, std::shared_ptr<ResourceLockT> res_lock)
    {
        std::unique_lock<std::shared_mutex> lock(m_rw_mutex);
        auto page_num = res_lock->getAddress() >> m_shift;
        auto it = findImpl(page_num, state_num);
        assert(it != m_cache.end());
        if (it == m_cache.end()) {
            THROWF(db0::InternalException) << "Attempt to erase non-existing lock from PageMap";
        }
        assert(it->second.lock() == res_lock);
        m_cache.erase(it);
    }
    
    template <typename ResourceLockT> void PageMap<ResourceLockT>::clear() 
    {
        std::unique_lock<std::shared_mutex> lock(m_rw_mutex);
        m_cache.clear();
    }
    
    template <typename ResourceLockT> bool PageMap<ResourceLockT>::empty() const
    {
        std::shared_lock<std::shared_mutex> lock(m_rw_mutex);
        return m_cache.empty();
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::erase(StateNumType state_num, std::uint64_t page_num)
    {
        std::unique_lock<std::shared_mutex> lock(m_rw_mutex);
#ifndef NDEBUG        
        auto result = m_cache.erase({page_num, state_num});
        assert(result);
#else
        m_cache.erase({page_num, state_num});
#endif                
    }
    
    template <typename ResourceLockT>
    std::shared_ptr<ResourceLockT> PageMap<ResourceLockT>::replace(
        StateNumType state_num, std::shared_ptr<ResourceLockT> res_lock, std::uint64_t page_num)
    {
        std::unique_lock<std::shared_mutex> _lock(m_rw_mutex);
        // find exact match of the page / state
        auto it = m_cache.find({page_num, state_num});
        if (it == m_cache.end()) {
            insert(_lock, state_num, res_lock);
            return {};
        }
        auto existing_lock = it->second.lock();
        if (!existing_lock) {
            // remove expired weak_ptr
            // this is fine because we're inserting under updated more recent state
            assert(state_num >= it->first.second);
            m_cache.erase(it);
            insert(_lock, state_num, res_lock);
            return {};
        }
        
        assert(existing_lock->size() == res_lock->size());
        // apply changes from the lock being merged (discarding changes in this lock)
        existing_lock->moveFrom(*res_lock);
        return existing_lock;
    }
    
    template <typename ResourceLockT>
    std::size_t PageMap<ResourceLockT>::size() const 
    {
        std::shared_lock<std::shared_mutex> lock(m_rw_mutex);
        return m_cache.size();
    }
    
    template <typename ResourceLockT>
    std::size_t PageMap<ResourceLockT>::clearExpired(StateNumType head_state_num)
    {
        std::size_t count = 0;
        std::unique_lock<std::shared_mutex> lock(m_rw_mutex);
        if (m_cache.empty()) {
            return 0;
        }
                
        auto it = m_cache.begin();
        while (it != m_cache.end()) {
            auto page_num = it->first.first;
            // remove expired locks of a specific page until reaching the head_state_num
            for (;;) {
                assert(it != m_cache.end());
                assert(it->first.first == page_num);
                if (!it->second.expired() || it->first.second > head_state_num) {
                    break;
                }
                assert(it->first.second <= head_state_num);
                assert(it->second.expired());
                it = m_cache.erase(it);
                ++count;

                // NOTE: if the lock is non-expired then all higher state locks must also be retained
                // otherwise would result in breaking the cache consistency for accessing head_state_num
                if (it == m_cache.end() || it->first.first != page_num) {
                    break;
                }
            }
            // iterate over to the next page
            while (it != m_cache.end() && it->first.first == page_num) {
                ++it;
            }
        }        
        
        return count;
    }
    
    template <typename ResourceLockT>
    bool PageMap<ResourceLockT>::hasLocks() const
    {
        std::shared_lock<std::shared_mutex> lock(m_rw_mutex);
        for (const auto &p: m_cache) {
            if (!p.second.expired()) {
                return true;
            }
        }
        return false;
    }
    
}