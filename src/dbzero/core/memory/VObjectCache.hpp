// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <functional>
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
         * @param has_detach whether the object can be detached
         * @return the v_object's shared_ptr
        */
        template <typename T, typename... Args> std::shared_ptr<T> create(bool has_detach, Args&&... args);
        
        /**
         * Try locating an existing instance in cache
         * @return nullptr if not found (not cached)
        */
        template <typename T>
        std::shared_ptr<T> tryFind(std::uint64_t address) const;

        /**
         * Either locate existing instance in cache or create a new one
         * @param address the instance address
         * @param has_detach whether the object can be detached         
         * @return the v_object's shared_ptr
        */
        template <typename T, typename... Args> std::shared_ptr<T> findOrCreate(std::uint64_t address, 
            bool has_detach, Args&&... args);

        /**
         * Remove element from cache if it exists, object is not destroyed
         * NOTE: as a side effect of this operation some other item may be removed from cache
         * if the requested object is no longer present at its original index
        */
        void erase(std::uint64_t address);

        // Detach all managed instances
        void detach() const;

        void commit() const;
        
        FixedObjectList &getSharedObjectList() const;

        void beginAtomic();
        void endAtomic();
        void cancelAtomic();
        
    private:
        Memspace &m_memspace;
        FixedObjectList &m_shared_object_list;
        // Store tuples: address -> (weak_ptr, likely index, commit function, detach function)
        // note that detach function may not be present (non-detachable)
        mutable std::unordered_map<std::uint64_t, std::tuple<std::weak_ptr<void>, std::uint32_t,
            std::function<void()>, std::function<void()> > > m_cache;
        bool m_atomic = false;
        // volatile instances - i.e. ones created during atomic operation
        mutable std::unordered_set<std::uint64_t> m_volatile;
    };
    
    template <typename T, typename... Args>
    std::shared_ptr<T> VObjectCache::create(bool has_detach, Args&&... args)
    {
        if (m_shared_object_list.full()) {
            // remove 1/4 of cached objects once the max_size is reached
            m_shared_object_list.eraseItems((m_shared_object_list.size() >> 2) + 1);
        }

        auto ptr = make_shared_void<T>(m_memspace, std::forward<Args>(args)...);
        // note that the index may be at any moment released and reused by other item
        auto index = m_shared_object_list.append(ptr);
        auto result_ptr = std::static_pointer_cast<T>(ptr);
        auto raw_ptr = result_ptr.get();
        auto commit_func = [raw_ptr]() {
            raw_ptr->commit();
        };
        std::function<void()> detach_func;
        // only available if has_detach is true (e.g. not available for MorphingBIndex)
        if (has_detach) {
            detach_func = [raw_ptr]() {
                raw_ptr->detach();
            };
        }
        m_cache[result_ptr->getAddress()] = { ptr, index, commit_func, detach_func };
        if (m_atomic) {
            m_volatile.insert(result_ptr->getAddress());
        }
        return result_ptr;
    }
    
    template <typename T, typename... Args>
    std::shared_ptr<T> VObjectCache::findOrCreate(std::uint64_t address, bool has_detach, Args&&... args)
    {
        auto it = m_cache.find(address);
        if (it != m_cache.end()) {
            auto lock = std::get<0>(it->second).lock();
            if (lock) {
                // for atomic operations register the address as volatile
                if (m_atomic) {
                    m_volatile.insert(address);
                }
                return std::static_pointer_cast<T>(lock);
            } else {
                // erase expired lock
                m_cache.erase(it);            
            }
        }
        return create<T>(has_detach, std::forward<Args>(args)...);
    }
    
    template <typename T>
    std::shared_ptr<T> VObjectCache::tryFind(std::uint64_t address) const
    {
        auto it = m_cache.find(address);
        if (it == m_cache.end()) {
            return nullptr;
        }
        auto lock = std::get<0>(it->second).lock();
        if (!lock) {
            // erase expired lock
            m_cache.erase(it);
            return nullptr;
        }
        if (m_atomic) {
            m_volatile.insert(address);
        }
        return std::static_pointer_cast<T>(lock);
    }
    
}   