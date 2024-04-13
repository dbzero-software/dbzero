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

    bool FixedObjectList::full() const
    {
        return m_size == m_capacity;
    }

    void FixedObjectList::eraseItems(std::uint32_t count)
    {
        assert(count <= m_capacity);
        for (;count != 0; --count) {
            if (*m_insert_iterator != nullptr) {
                --m_size;
                *m_insert_iterator = nullptr;
            }
            ++m_insert_iterator;
            if (m_insert_iterator == m_data.end()) {
                m_insert_iterator = m_data.begin();
            }
            // skip every other item
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
            m_shared_object_list.eraseAt(it->second.second);
            m_cache.erase(it);
        }        
    }
    
}
