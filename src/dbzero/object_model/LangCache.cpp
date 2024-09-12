#include "LangCache.hpp"

namespace db0

{
    
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

    LangCache::~LangCache()
    {
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
        // move object from the other LangCache
        add(dst_fixture_id, dst_address, other.m_cache[it->second].second.get());
        other.m_cache[it->second] = {};
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

    void LangCache::resize(std::size_t new_size)
    {
        assert(new_size > m_cache.size());
        auto evict_index = m_evict_hand - m_cache.begin();
        auto insert_index = m_insert_hand - m_cache.begin();                        
        m_cache.resize(new_size);
        m_visited.resize(m_cache.size());
        m_evict_hand = m_cache.begin() + evict_index;
        m_insert_hand = m_cache.begin() + insert_index;
    }

    void LangCache::add(std::uint16_t fixture_id, std::uint64_t address, ObjectPtr obj)
    {
        auto uid = makeUID(fixture_id, address);
        std::optional<std::uint32_t> slot;
        if (isFull()) {
            if (m_cache.size() < m_capacity) {                
                resize(m_cache.size() * 2);                
                slot = findEmptySlot();
                assert(slot);
            } else {
                int num_visited = 0;
                slot = evictOne(&num_visited);
                if (!slot && num_visited > 0) {
                    // try again after visiting some elements
                    slot = evictOne();
                }
                if (!slot) {
                    // resize by a predefined step
                    resize(m_cache.size() + m_step);
                    slot = findEmptySlot();
                    assert(slot);
                }
            }
        } else {
            slot = findEmptySlot();
            assert(slot);
        }
        auto slot_id = *slot;
        assert(!m_cache[slot_id].second);
        m_cache[slot_id] = { uid, obj };
        m_visited[slot_id] = true;
        m_uid_to_index[uid] = slot_id;
        ++m_size;
    }

    void LangCache::erase(const Fixture &fixture, std::uint64_t address, bool expired_only) {
        return erase(getFixtureId(fixture), address, expired_only);
    }
    
    void LangCache::erase(std::uint16_t fixture_id, std::uint64_t address, bool expired_only)
    {
        auto uid = makeUID(fixture_id, address);
        auto it = m_uid_to_index.find(uid);
        // instance not found
        if (it == m_uid_to_index.end()) {
            return;
        }

        auto slot_id = it->second;
        if (expired_only && LangToolkit::getRefCount(m_cache[slot_id].second.get()) > 1) {
            return;
        }

        // need to remove from the map first because destroy may trigger erase from GC0        
        m_uid_to_index.erase(it);
        m_cache[slot_id] = {};
        --m_size;
    }
    
    void LangCache::clear(bool expired_only)
    {
        for (auto &item: m_cache) {
            if (item.second && (!expired_only || LangToolkit::getRefCount(item.second.get()) == 1)) {
                m_uid_to_index.erase(item.first);
                item = {};
                --m_size;            
            }
        }
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
        assert(it->second < m_visited.size());
        // set the visited flag (see Sieve cache eviction algorithm)
        m_visited[it->second] = true;
        return m_cache[it->second].second;
    }

    std::optional<std::uint32_t> LangCache::evictOne(int *num_visited)
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
            if (m_evict_hand->second) {
                if (m_visited[m_evict_hand - m_cache.begin()]) {
                    // visited but not evicted
                    if (num_visited) {
                        ++(*num_visited);
                    }
                    m_visited[m_evict_hand - m_cache.begin()] = false;
                } else {
                    if (LangToolkit::getRefCount(m_evict_hand->second.get()) == 1) {
                        // evict the object
                        m_uid_to_index.erase(m_evict_hand->first);
                        *m_evict_hand = {};
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
        auto end = m_insert_hand;
        for (;;) {
            if (m_insert_hand == m_cache.end()) {
                m_insert_hand = m_cache.begin();
            }
            if (!m_insert_hand->second) {
                return m_insert_hand - m_cache.begin();
            }
            ++m_insert_hand;
            if (m_insert_hand == end) {
                return std::nullopt;
            }
        }
    }

    std::size_t LangCache::size() const {
        return m_size;
    }
    
    std::size_t LangCache::getCapacity() const {
        return m_capacity;
    }
    
    LangCacheView::LangCacheView(const Fixture &fixture, std::shared_ptr<LangCache> cache_ptr)
        : m_fixture(fixture)
        , m_cache_ptr(cache_ptr)
        , m_cache(*m_cache_ptr)
        , m_fixture_id(m_cache.getFixtureId(fixture))
    {
    }
    
    void LangCacheView::add(std::uint64_t address, ObjectPtr obj)
    {
        m_cache.add(m_fixture_id, address, obj);
        m_objects.insert(address);
    }

    void LangCacheView::erase(std::uint64_t address)
    {
        m_cache.erase(m_fixture_id, address);
        m_objects.erase(address);
    }    
    
    LangCacheView::ObjectSharedPtr LangCacheView::get(std::uint64_t address) const {
        return m_cache.get(m_fixture_id, address);
    }
    
    void LangCacheView::moveFrom(LangCacheView &other, std::uint64_t src_address, std::uint64_t dst_address)
    {
        m_cache.moveFrom(other.m_cache, other.m_fixture_id, src_address, m_fixture_id, dst_address);
        other.m_objects.erase(src_address);
        m_objects.insert(dst_address);
    }
    
    void LangCacheView::clear(bool expired_only)
    {
        // erase expired objects only
        for (auto addr: m_objects) {
            m_cache.erase(m_fixture_id, addr, expired_only);
        }
    }

}