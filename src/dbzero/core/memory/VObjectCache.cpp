// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "VObjectCache.hpp"

namespace db0

{

    FixedObjectList::FixedObjectList(std::uint32_t capacity)
        : m_capacity(capacity)
        // create vector of 1/4 larger capacity than requested to improve insert performance
        , m_data(capacity + (capacity >> 2))
        , m_insert_iterator(m_data.begin())
    {
    }
    
    bool FixedObjectList::full() const {
        return m_size == m_capacity;
    }

    void FixedObjectList::eraseItems(std::uint32_t count)
    {
        assert(count <= m_capacity);
        while (count > 0) {
            if (*m_insert_iterator != nullptr) {
                --m_size;
                --count;
                *m_insert_iterator = nullptr;
            }
            ++m_insert_iterator;
            if (m_insert_iterator == m_data.end()) {
                m_insert_iterator = m_data.begin();
            }
            ++m_insert_iterator;
            if (m_insert_iterator == m_data.end()) {
                m_insert_iterator = m_data.begin();
            }
        }
    }
    
    void FixedObjectList::eraseAt(std::uint32_t index)
    {
        if (m_data[index] != nullptr) {
            m_data[index] = nullptr;
            --m_size;
        }
    }

    std::uint32_t FixedObjectList::append(std::shared_ptr<void> item)
    {
        assert(m_size < m_data.size());
        while (*m_insert_iterator != nullptr) {
            ++m_insert_iterator;
            if (m_insert_iterator == m_data.end()) {
                m_insert_iterator = m_data.begin();
            }
        }
        assert(*m_insert_iterator == nullptr);
        auto index = m_insert_iterator - m_data.begin();
        *(m_insert_iterator++) = item;
        ++m_size;
        if (m_insert_iterator == m_data.end()) {
            m_insert_iterator = m_data.begin();
        }
        return index;
    }

    std::uint32_t FixedObjectList::size() const {
        return m_size;
    }
    
    void FixedObjectList::clear()
    {
        m_size = 0;
        std::fill(m_data.begin(), m_data.end(), nullptr);
        m_insert_iterator = m_data.begin();        
    }

    VObjectCache::VObjectCache(Memspace &memspace, FixedObjectList &shared_object_list)
        : m_memspace(memspace)
        , m_shared_object_list(shared_object_list)
    {
    }
    
    FixedObjectList &VObjectCache::getSharedObjectList() const {
        return m_shared_object_list;
    }
    
    void VObjectCache::erase(std::uint64_t address)
    {
        auto it = m_cache.find(address);
        if (it != m_cache.end()) {
            if (!std::get<0>(it->second).expired()) {
                m_shared_object_list.eraseAt(std::get<1>(it->second));
            }
            m_cache.erase(it);
        }
    }
    
    void VObjectCache::commit() const
    {
        // commit all cached instances
        auto it = m_cache.begin();
        while (it != m_cache.end()) {
            if (std::get<0>(it->second).expired()) {
                it = m_cache.erase(it);                
            } else {
                // commit
                std::get<2>(it->second)();
                ++it;
            }
        }
    }
    
    void VObjectCache::detach() const
    {
        // detach all cached instance
        auto it = m_cache.begin();
        while (it != m_cache.end()) {
            if (std::get<0>(it->second).expired()) {
                it = m_cache.erase(it);
                continue;
            }
            
            // detach or erase (if detach operator not provided)
            if (std::get<3>(it->second)) {
                // detach
                std::get<3>(it->second)();
                ++it;
            } else {
                // erase when detach is not available
                m_shared_object_list.eraseAt(std::get<1>(it->second));
                it = m_cache.erase(it);
            }
        }        
    }
    
    void VObjectCache::beginAtomic()
    {
        assert(!m_atomic);
        commit();
        m_atomic = true;
    }

    void VObjectCache::endAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
        m_volatile.clear();
    }

    void VObjectCache::cancelAtomic()
    {
        assert(m_atomic);
        m_atomic = false;
        // remove volatile instances from cache
        for (auto address : m_volatile) {
            erase(address);
        }
        m_volatile.clear();
    }

}
