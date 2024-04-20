#pragma once

#include <memory>
#include <unordered_map>
#include <dbzero/core/utils/shared_void.hpp>
#include <dbzero/core/threading/ProgressiveMutex.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{
    
    // Fixed-capacity object list
    class FixedObjectList
    {
    public:
        FixedObjectList(std::uint32_t capacity);

        bool full() const;

        /**
         * Erase count consecutive items
        */
        void eraseItems(std::uint32_t count);

        /**
         * Remove item at index
        */
        void eraseAt(std::uint32_t index);

        /**
         * Append a new item
         * @return the index of the new item
        */
        std::uint32_t append(std::shared_ptr<void> item);

        std::uint32_t size() const;

        void clear();

    private:
        const std::uint32_t m_capacity;
        std::uint32_t m_size = 0;
        std::vector<std::shared_ptr<void> > m_data;
        // round-robin iterator
        std::vector<std::shared_ptr<void> >::iterator m_insert_iterator;
    };
    
    class VObjectCache
    {
    public:
        VObjectCache(Memspace &, FixedObjectList &shared_object_list);

        /**
         * Create a new v_object instance add add to cache
         * @return the v_object's shared_ptr
        */
        template <typename T, typename... Args> std::shared_ptr<T> create(Args&&... args);
        
        /**
         * Try locating an existing instance in cache
         * @return nullptr if not found (not cached)
        */
        template <typename T>
        std::shared_ptr<T> tryFind(std::uint64_t address) const;

        /**
         * Either locate existing instance in cache or create a new one
         * @return the v_object's shared_ptr
        */
        template <typename T, typename... Args> std::shared_ptr<T> findOrCreate(std::uint64_t address, Args&&... args);

        /**
         * Remove element from cache if it exists, object is not destroyed
         * NOTE: as a side effect of this operation some other item may be removed from cache
         * if the requested object is no longer present at its original index
        */
        void erase(std::uint64_t address);

        FixedObjectList &getSharedObjectList() const;
        
    private:
        Memspace &m_memspace;
        FixedObjectList &m_shared_object_list;
        // store pairs: address -> (weak_ptr, likely index)
        mutable std::unordered_map<std::uint64_t, std::pair<std::weak_ptr<void>, std::uint32_t> > m_cache;
    };
    
    template <typename T, typename... Args>
    std::shared_ptr<T> VObjectCache::create(Args&&... args)
    {
        if (m_shared_object_list.full()) {
            // remove 1/4 of cached objects once the max_size is reached
            m_shared_object_list.eraseItems(m_shared_object_list.size() >> 2);            
        }
        
        auto ptr = make_shared_void<T>(m_memspace, std::forward<Args>(args)...);
        // note that the index may be at any moment released and reused by other item
        auto index = m_shared_object_list.append(ptr);
        auto result_ptr = std::static_pointer_cast<T>(ptr);
        m_cache[result_ptr->getAddress()] = { ptr, index };        
        return result_ptr;
    }
    
    template <typename T, typename... Args>
    std::shared_ptr<T> VObjectCache::findOrCreate(std::uint64_t address, Args&&... args)
    {
        auto it = m_cache.find(address);
        if (it != m_cache.end()) {
            auto lock = it->second.first.lock();
            if (lock) {
                return std::static_pointer_cast<T>(lock);
            }
        }
        return create<T>(std::forward<Args>(args)...);
    }
    
    template <typename T>
    std::shared_ptr<T> VObjectCache::tryFind(std::uint64_t address) const
    {
        auto it = m_cache.find(address);
        if (it == m_cache.end())
        {
            return nullptr;
        }
        auto lock = it->second.first.lock();
        if (!lock)
        {
            m_cache.erase(it);
            return nullptr;
        }
        return std::static_pointer_cast<T>(lock);
    }
    
}   