#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <dbzero/core/memory/ResourceLock.hpp>
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
         * Try finding the closest match for a given state
        */
        std::shared_ptr<ResourceLockT> findRange(std::uint64_t state_num, std::uint64_t first_page,
            std::uint64_t end_page, std::uint64_t &read_state_num) const;

        /**
         * Check if the range exists without returning its parameters
        */
        bool rangeExists(std::uint64_t state_num, std::uint64_t first_page, std::uint64_t end_page) const;

        std::shared_ptr<ResourceLockT> findPage(std::uint64_t state_num, std::uint64_t page_num,
            std::uint64_t &read_state_num) const;
        
        void insertRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT>);
        void insertRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT>,
            std::uint64_t first_page, std::uint64_t end_page);

        void insertPage(std::uint64_t state_num, std::shared_ptr<ResourceLockT>, std::uint64_t page_num);

        void forEach(std::function<void(const ResourceLockT &)>) const;

        void forEach(std::function<void(ResourceLockT &)>);

        // Erase lock stored under a known state number
        void erase(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock);
        void erasePage(std::uint64_t state_num, std::uint64_t page_num);
        void eraseRange(std::uint64_t state_num, std::uint64_t first_page, std::uint64_t end_page);        
        
        // @return true if the lock was reused (inserted into the existing range)
        bool replaceRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t first_page,
            std::uint64_t end_page);
        // @return true if the lock was reused (inserted into the existing range)
        bool replacePage(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t page_num);

        void clear();

        bool empty() const;

        std::size_t size() const;
        
    private:        
        const unsigned int m_shift;

        struct CompT {
            inline bool operator()(const PageKeyT &k1, const PageKeyT &k2) const {
                return k1.first < k2.first || (k1.first == k2.first && k1.second < k2.second);
            }
        };

        // page-wise cache, note that a single ResourceLock may be associated with multiple pages
        mutable std::map<PageKeyT, std::weak_ptr<ResourceLockT>, CompT> m_cache;        
        using CacheIterator = typename decltype(m_cache)::iterator;

        CacheIterator find(std::uint64_t page_num, std::uint64_t state_num) const;
    };
    
    template <typename ResourceLockT>
    PageMap<ResourceLockT>::PageMap(std::size_t page_size)
        : m_shift(getPageShift(page_size))
    {
    }

    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insertRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock,
        std::uint64_t first_page, std::uint64_t end_page)
    {
        PageKeyT key { first_page, state_num };
        while (key.first != end_page) {
            // this is to overwrite existing keys which may already exist
            // e.g. due to "missing" range sentinels
            m_cache[key] = lock;
            ++key.first;
        }
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insertRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock)
    {
        auto first_page = lock->getAddress() >> m_shift;
        auto end_page = ((lock->getAddress() + lock->size() - 1) >> m_shift) + 1;
        insertRange(state_num, lock, first_page, end_page);
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::insertPage(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t page_num)
    {
        m_cache.insert({{page_num, state_num}, lock});
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::forEach(std::function<void(const ResourceLockT &)> f) const {
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
        for (const auto &p: m_cache) {
            auto lock = p.second.lock();
            if (lock) {
                f(*lock);
            }
        }
    }
    
    template <typename ResourceLockT>
    std::shared_ptr<ResourceLockT> PageMap<ResourceLockT>::findRange(std::uint64_t state_num, std::uint64_t first_page,
        std::uint64_t end_page, std::uint64_t &read_state_num) const
    {
        assert(first_page < end_page);
        std::shared_ptr<ResourceLock> result;
        read_state_num = 0;
        for (; first_page != end_page; ++first_page) {
            auto it = find(first_page, state_num);
            if (it == m_cache.end()) {
                if (result) {
                    throw std::runtime_error("PrefixCache::findRange: inconsistent locks exist for the same range");
                }
            } else {
                auto lock = it->second.lock();
                if (lock) {
                    if (result && result != lock) {
                        throw std::runtime_error("PrefixCache::findRange: multiple locks found for the same range");
                    }
                    result = lock;
                    if (!read_state_num) {
                        read_state_num = it->first.second;
                    } else if (read_state_num != it->first.second) {
                        throw std::runtime_error("PrefixCache::findRange: inconsistent states exist for the same range");
                    }                    
                } else {
                    // remove expired weak_ptr
                    m_cache.erase(it);
                    if (result) {
                        throw std::runtime_error("PrefixCache::findRange: inconsistent locks exist for the same range");
                    }
                }
            }
        }
        return result;
    }

    template <typename ResourceLockT>
    bool PageMap<ResourceLockT>::rangeExists(std::uint64_t state_num, std::uint64_t first_page, std::uint64_t end_page) const
    {                
        for (; first_page != end_page; ++first_page) {
            auto it = find(first_page, state_num);
            if (it != m_cache.end()) {
                return true;
            }
        }
        return false;
    }
    
    template <typename ResourceLockT>
    std::shared_ptr<ResourceLockT> PageMap<ResourceLockT>::findPage(std::uint64_t state_num, std::uint64_t page_num,
        std::uint64_t &read_state_num) const
    {
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
        if (it->first.first != page_num || it->first.second > state_num) {
            return m_cache.end();
        }
        return it;
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::erase(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock)
    {
        auto first_page = lock->getAddress() >> m_shift;
        auto end_page = ((lock->getAddress() + lock->size() - 1) >> m_shift) + 1;
        for (;first_page != end_page; ++first_page) {
            auto it = find(first_page, state_num);
            assert(it != m_cache.end());
            assert(it->second.lock() == lock);
            m_cache.erase(it);
        }
    }

    template <typename ResourceLockT> void PageMap<ResourceLockT>::clear() {
        m_cache.clear();
    }

    template <typename ResourceLockT> bool PageMap<ResourceLockT>::empty() const {
        return m_cache.empty();
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::erasePage(std::uint64_t state_num, std::uint64_t page_num)
    {
        m_cache.erase({page_num, state_num});
    }

    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::eraseRange(std::uint64_t state_num, std::uint64_t first_page,
        std::uint64_t end_page)
    {
        for (; first_page != end_page; ++first_page) {
            m_cache.erase({first_page, state_num});
        }
    }
    
    template <typename ResourceLockT>
    bool PageMap<ResourceLockT>::replacePage(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, 
        std::uint64_t page_num)
    {
        std::uint64_t existing_state_num;
        auto existing_lock = findPage(state_num, page_num, existing_state_num);
        if (existing_lock && existing_state_num == state_num) {
            assert(existing_lock->size() == lock->size());
            // apply changes from the lock being merged
            existing_lock->copyFrom(*lock);
            return false;
        } else {
            insertPage(state_num, lock, page_num);
            return true;
        }
    }

    template <typename ResourceLockT>
    bool PageMap<ResourceLockT>::replaceRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock,
        std::uint64_t first_page, std::uint64_t end_page)
    {
        std::uint64_t existing_state_num;
        auto existing_lock = findRange(state_num, first_page, end_page, existing_state_num);        
        if (existing_lock && existing_state_num == state_num) {
            assert(existing_lock->size() == lock->size());
            // apply changes from the lock being merged
            existing_lock->copyFrom(*lock);
            return false;
        } else {
            insertRange(state_num, lock);
            return true;
        }
    }
       
    template <typename ResourceLockT>
    std::size_t PageMap<ResourceLockT>::size() const {
        return m_cache.size();
    }

}