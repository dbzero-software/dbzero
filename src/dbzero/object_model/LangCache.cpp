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
        assert(DEFAULT_INITIAL_SIZE > 0);
    }

    void LangCache::moveFrom(LangCache &other, const Fixture &src_fixture, std::uint64_t src_address,
        const Fixture &dst_fixture, std::uint64_t dst_address)
    {
        moveFrom(other, getFixtureId(src_fixture), src_address, getFixtureId(dst_fixture), dst_address);
    }

    void LangCache::moveFrom(LangCache &other, std::uint16_t src_fixture_id, std::uint64_t src_address,
        std::uint16_t dst_fixture_id, std::uint64_t dst_address)
    {
        auto src_uid = makeUID(src_fixture_id, src_address);
        auto it = other.m_uid_to_index.find(src_uid);
        // instance not found
        if (it == other.m_uid_to_index.end()) {
            return;
        }
        add(dst_fixture_id, dst_address, m_cache[it->second].get());
        other.m_cache[it->second] = nullptr;
        other.m_uid_to_index.erase(it);        
        --other.m_size;        
    }

    bool LangCache::isFull() const {
        return m_size == m_cache.size();
    }
    
    std::uint16_t LangCache::getFixtureId(const Fixture &fixture) const {
        return m_fixture_to_id.addUnique(&fixture);
    }

    void LangCache::add(const db0::Fixture &fixture, std::uint64_t address, ObjectPtr obj) {
        add(getFixtureId(fixture), address, obj);
    }

    void LangCache::add(std::uint16_t fixture_id, std::uint64_t address, ObjectPtr obj)
    {
        auto uid = makeUID(fixture_id, address);
        std::optional<std::uint32_t> slot;
        if (isFull()) {
            slot = evictOne();
            if (!slot) {
                auto evict_index = m_evict_hand - m_cache.begin();
                auto insert_index = m_insert_hand - m_cache.begin();                
                if (m_cache.size() < m_capacity) {
                    m_cache.resize(m_cache.size() * 2);
                    m_visited.resize(m_cache.size());
                } else {
                    m_cache.resize(m_cache.size() + m_step);
                    m_visited.resize(m_cache.size());
                }
                m_evict_hand = m_cache.begin() + evict_index;
                m_insert_hand = m_cache.begin() + insert_index;
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
        m_uid_to_index[uid] = slot_id;
        ++m_size;
    }

    void LangCache::erase(const Fixture &fixture, std::uint64_t address) {
        return erase(getFixtureId(fixture), address);
    }

    void LangCache::erase(std::uint16_t fixture_id, std::uint64_t address)
    {
        auto uid = makeUID(fixture_id, address);
        auto it = m_uid_to_index.find(uid);
        // instance not found
        if (it == m_uid_to_index.end()) {
            return;
        }
        m_cache[it->second] = nullptr;
        m_uid_to_index.erase(it);
        --m_size;        
    }

    LangCache::ObjectSharedPtr LangCache::get(const Fixture &fixture, std::uint64_t address) const {
        return get(getFixtureId(fixture), address);
    }

    LangCache::ObjectSharedPtr LangCache::get(std::uint16_t fixture_id, std::uint64_t address) const 
    {
        auto uid = makeUID(fixture_id, address);
        auto it = m_uid_to_index.find(uid);
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
            return std::nullopt;
        }

        assert(m_evict_hand != m_cache.end());
        auto end = m_evict_hand;
        ++m_evict_hand;
        for (;m_evict_hand != end; ++m_evict_hand) {
            // round-robin
            if (m_evict_hand == m_cache.end()) {
                m_evict_hand = m_cache.begin();
                if (m_evict_hand == end) {
                    // no evictable element exists
                    return std::nullopt;
                }
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
        for (;;) {
            if (m_insert_hand == m_cache.end()) {
                m_insert_hand = m_cache.begin();
            }
            if (!*m_insert_hand) {
                return m_insert_hand - m_cache.begin();
            }
            ++m_insert_hand;
            if (m_insert_hand == m_evict_hand) {
                return std::nullopt;
            }
        }
    }

    LangCacheView::LangCacheView(const Fixture &fixture, LangCache &cache)
        : m_fixture(fixture)
        , m_cache(cache)
        , m_fixture_id(cache.getFixtureId(fixture))
    {
    }
    
    void LangCacheView::add(std::uint64_t address, ObjectPtr obj) {
        m_cache.add(m_fixture_id, address, obj);
        m_objects.insert(address);
    }

    void LangCacheView::erase(std::uint64_t address) {
        m_cache.erase(m_fixture_id, address);
        m_objects.erase(address);
    }

    LangCacheView::ObjectSharedPtr LangCacheView::get(std::uint64_t address) const {
        return m_cache.get(m_fixture_id, address);
    }
    
    void LangCacheView::moveFrom(LangCacheView &other, std::uint64_t src_address, std::uint64_t dst_address) {
        m_cache.moveFrom(other.m_cache, other.m_fixture_id, src_address, m_fixture_id, dst_address);
    }

    void LangCacheView::clear()
    {
        for (auto addr: m_objects) {
            m_cache.erase(m_fixture_id, addr);
        }
    }

}