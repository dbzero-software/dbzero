#include "LangCache.hpp"

namespace db0

{

    /* FIXME:
    void LangCache::erase(std::uint64_t address)
    {
        auto it = m_cache.find(address);
        if (it == m_cache.end()) {
            return;
        }
        if (it->second.m_strong_ref) {
            LangToolkit::decRef(it->second.m_object);
        }
        m_cache.erase(it);
    }

    void LangCache::clear()
    {
        for (auto &item : m_cache) {
            if (item.second.m_strong_ref) {
                LangToolkit::decRef(item.second.m_object);
            }
        }
        m_cache.clear();
    }
    */

    LangCache::LangCache(std::optional<std::size_t> capacity, std::optional<std::uint32_t> step)
        : m_capacity(std::max(DEFAULT_INITIAL_SIZE, capacity.value_or(DEFAULT_CAPACITY)))
        , m_step(step.value_or(DEFAULT_STEP))
        , m_cache(DEFAULT_INITIAL_SIZE)
        , m_evict_hand(m_cache.begin())
        , m_insert_hand(m_cache.begin())        
        , m_visited(DEFAULT_INITIAL_SIZE)
    {
    }
    
    void LangCache::moveFrom(LangCache &other, const Fixture &src_fixture, std::uint64_t src_address,
        const Fixture &dst_fixture, std::uint64_t dst_address)
    {
        // FIXME: implement address shortening
        auto it = other.m_uid_to_index.find(src_address);
        // instance not found
        if (it == other.m_uid_to_index.end()) {
            return;
        }
        add(dst_fixture, dst_address, m_cache[it->second].get());
        other.m_cache[it->second] = nullptr;
        other.m_uid_to_index.erase(it);        
        --other.m_size;        
    }

    bool LangCache::isFull() const {
        return m_size == m_cache.size();
    }
    
    void LangCache::add(const db0::Fixture &, std::uint64_t address, ObjectPtr obj)
    {
        // FIXME: implement address shortening
        std::optional<std::uint32_t> slot;
        if (isFull()) {
            slot = evictOne();
            if (!slot) {                
                if (m_cache.size() < m_capacity) {
                    m_cache.resize(m_cache.size() * 2);
                    m_visited.resize(m_cache.size());
                } else {
                    m_cache.resize(m_cache.size() + m_step);
                    m_visited.resize(m_cache.size());
                }
                slot = findEmptySlot();
                assert(slot);
            }
        } else {
            slot = findEmptySlot();        
            assert(slot);
        }
        auto slot_id = *slot;
        m_cache[slot_id] = obj;
        m_visited[slot_id] = true;
        m_uid_to_index[address] = slot_id;
        ++m_size;  
    }

    LangCache::ObjectSharedPtr LangCache::get(const db0::Fixture &, std::uint64_t address) const
    {
        auto it = m_uid_to_index.find(address);
        if (it == m_uid_to_index.end()) {
            return {};
        }
        // set the visited flag (see Sieve cache eviction algorithm)
        m_visited[it->second] = true;
        return m_cache[it->second];
    }

    std::optional<std::uint32_t> LangCache::evictOne()
    {
        if (m_size == 0) {
            return {};
        }

        auto end = m_evict_hand;
        ++m_evict_hand;
        for (;m_evict_hand != end; ++m_evict_hand) {
            // round-robin
            if (m_evict_hand == m_cache.end()) {
                m_evict_hand = m_cache.begin();
            }
            // only cache-owned objects can be evicted
            if (*m_evict_hand) {
                if (m_visited[m_evict_hand - m_cache.begin()]) {
                    m_visited[m_evict_hand - m_cache.begin()] = false;
                } else {
                    if (LangToolkit::getRefCount(m_evict_hand->get()) == 1) {
                        // evict the object
                        *m_evict_hand = nullptr;
                        --m_size;
                        return m_evict_hand - m_cache.begin();
                    }
                }
            }
        }
        // no evictable element exists
        return std::nullopt;
    }
    
    std::optional<std::uint32_t> LangCache::findEmptySlot() const
    {
        for (;;++m_insert_hand) {
            if (m_insert_hand == m_cache.end()) {
                m_insert_hand = m_cache.begin();
            }
            if (m_insert_hand == m_evict_hand) {
                return std::nullopt;
            }
            if (!*m_insert_hand) {
                return m_insert_hand - m_cache.begin();
            }
        }
    }
    
    void LangCache::add(std::uint64_t address, ObjectPtr)
    {

    }

}