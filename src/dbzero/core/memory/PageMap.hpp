#pragma once

#include <memory>
#include <map>
#include <cstdint>
#include <functional>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/memory/BoundaryLock.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/storage/StorageView.hpp>

namespace db0

{   
    
    class CacheRecycler;
    
    template <typename ResourceLockT> class PageMap
    {
    public:
        // page_num, state_num
        using PageKeyT = std::pair<std::uint64_t, std::uint64_t>;
        
        /**
         * Try finding the closest match for a given state
        */
        std::shared_ptr<ResourceLockT> findRange(std::uint64_t state_num, std::uint64_t first_page,
            std::uint64_t end_page, std::uint64_t *result_state_num = nullptr) const;

        /**
         * Check if the range exists without returning its parameters
        */
        bool rangeExists(std::uint64_t state_num, std::uint64_t first_page, std::uint64_t end_page) const;

        std::shared_ptr<ResourceLockT> findPage(std::uint64_t state_num, std::uint64_t page_num,
            std::uint64_t *result_state_num = nullptr) const;
        
        void insertRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT>, std::uint64_t first_page,
            std::uint64_t end_page);

        void updateRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT>, std::uint64_t first_page,
            std::uint64_t end_page);

        void insertPage(std::uint64_t state_num, std::shared_ptr<ResourceLockT>, std::uint64_t page_num);

        void forEach(std::function<void(const ResourceLockT &)>) const;

        void forEach(std::function<void(ResourceLockT &)>);

        void erasePage(std::uint64_t state_num, std::uint64_t page_num);
        void eraseRange(std::uint64_t state_num, std::uint64_t first_page, std::uint64_t end_page);

        void replacePage(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t page_num);
        void replaceRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t first_page,
            std::uint64_t end_page);

        void clear();

        bool empty() const;

        std::size_t size() const;
        
    private:
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
    void PageMap<ResourceLockT>::insertRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t first_page,
        std::uint64_t end_page)
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
    void PageMap<ResourceLockT>::updateRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t first_page,
        std::uint64_t end_page)
    {
        PageKeyT key { first_page, state_num };
        while (key.first != end_page) {
            auto it = m_cache.find(key);
            if (it == m_cache.end()) {
                THROWF(InternalException) << "PageMap::updateRange: lock not found for page " << key.first << " in state " << key.second;
            }
            it->second = lock;
            ++key.first;
        }
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
    void PageMap<ResourceLockT>::forEach(std::function<void(ResourceLockT &)> f) {
        for (const auto &p: m_cache) {
            auto lock = p.second.lock();
            if (lock) {
                f(*lock);
            }
        }
    }
    
    template <typename ResourceLockT>
    std::shared_ptr<ResourceLockT> PageMap<ResourceLockT>::findRange(std::uint64_t state_num, std::uint64_t first_page,
        std::uint64_t end_page, std::uint64_t *result_state_num) const
    {
        std::shared_ptr<ResourceLock> result;
        bool has_result = false;
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
                    if (result_state_num) {
                        // take max from cached states
                        if (!has_result || (*result_state_num < it->first.second)) {
                            *result_state_num = it->first.second;
                            has_result = true;
                        }
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
    bool PageMap<ResourceLockT>::rangeExists(std::uint64_t state_num, std::uint64_t first_page, 
        std::uint64_t end_page) const
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
        std::uint64_t *result_state_num) const
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
        if (result_state_num) {
            *result_state_num = it->first.second;
        }
        return lock;
    }

    template <typename ResourceLockT>
    typename PageMap<ResourceLockT>::CacheIterator PageMap<ResourceLockT>::find(std::uint64_t page_num, std::uint64_t state_num) const
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
    void PageMap<ResourceLockT>::clear() {
        m_cache.clear();
    }

    template <typename ResourceLockT>
    bool PageMap<ResourceLockT>::empty() const {
        return m_cache.empty();
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::erasePage(std::uint64_t state_num, std::uint64_t page_num)
    {
        m_cache.erase({page_num, state_num});
    }

    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::eraseRange(std::uint64_t state_num, std::uint64_t first_page, std::uint64_t end_page)
    {
        for (; first_page != end_page; ++first_page) {
            m_cache.erase({first_page, state_num});
        }
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::replacePage(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t page_num) 
    {
        erasePage(state_num, page_num);
        insertPage(state_num, lock, page_num);
    }
    
    template <typename ResourceLockT>
    void PageMap<ResourceLockT>::replaceRange(std::uint64_t state_num, std::shared_ptr<ResourceLockT> lock, std::uint64_t first_page,
        std::uint64_t end_page)
    {
        eraseRange(state_num, first_page, end_page);
        insertRange(state_num, lock, first_page, end_page);
    }

    template <typename ResourceLockT>
    std::size_t PageMap<ResourceLockT>::size() const {
        return m_cache.size();
    }

}