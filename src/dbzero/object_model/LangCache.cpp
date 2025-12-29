// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
    
    void LangCache::moveFrom(LangCache &other, const Fixture &src_fixture, Address src_address,
        const Fixture &dst_fixture, Address dst_address)
    {
        moveFrom(other, getFixtureId(src_fixture), src_address, getFixtureId(dst_fixture), dst_address);
    }

    void LangCache::moveFrom(LangCache &other, std::uint16_t src_fixture_id, Address src_address,
        std::uint16_t dst_fixture_id, Address dst_address)
    {
        ObjectSharedExtPtr obj_ptr;
        {
            auto other_lock = other.lockUnique();
            auto src_uid = makeUID(src_fixture_id, src_address);
            auto it = other.m_uid_to_index.find(src_uid);
            // instance not found
            if (it == other.m_uid_to_index.end()) {
                return;
            }
            obj_ptr = std::move(other.m_cache[it->second].second);
            other.m_uid_to_index.erase(it);
            --other.m_size;
        }
        // move object from the other LangCache
        add(dst_fixture_id, dst_address, obj_ptr.get());
    }
    
    bool LangCache::isFull() const
    {        
        // internal method, no need to lock
        return m_size == m_cache.size();
    }
    
    std::uint16_t LangCache::getFixtureId(const Fixture &fixture) const {
        return m_fixture_to_id.addUnique(&fixture);
    }
    
    void LangCache::add(const db0::Fixture &fixture, Address address, ObjectPtr obj) {
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
    
    void LangCache::add(std::uint16_t fixture_id, Address address, ObjectPtr obj)
    {
        // optional object to be deleted outside of the lock
        ObjectSharedExtPtr to_delete;
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        auto uid = makeUID(fixture_id, address);
        std::optional<std::uint32_t> slot;
        if (isFull()) {
            if (m_cache.size() < m_capacity) {
                resize(std::min(m_capacity, m_cache.size() * 2));
                slot = findEmptySlot();
                assert(slot);
            } else {
                int num_visited = 0;
                slot = evictOne(to_delete, &num_visited);
                if (!slot && num_visited > 0) {
                    // try again after visiting some elements
                    slot = evictOne(to_delete);
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
        // slot must be empty
        assert(!m_cache[slot_id].second);
        m_cache[slot_id] = { uid, obj };
        m_visited[slot_id] = true;
        m_uid_to_index[uid] = slot_id;        
        ++m_size;
        lock.unlock();
    }
    
    bool LangCache::erase(const Fixture &fixture, Address address, bool expired_only, bool as_defunct) {
        return erase(getFixtureId(fixture), address, expired_only);
    }

    bool LangCache::erase(std::uint16_t fixture_id, Address address, bool expired_only, bool as_defunct)
    {
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        auto uid = makeUID(fixture_id, address);
        auto it = m_uid_to_index.find(uid);
        // instance not found
        if (it == m_uid_to_index.end()) {
            return true;
        }

        auto slot_id = it->second;
        // NOTE: we check for any refernces except from LangCache itself (+1)
        if (expired_only && LangToolkit::hasAnyLangRefs(m_cache[slot_id].second.get(), 1)) {
            return false;
        }

        // need to remove from the map first because destroy may trigger erase from GC0        
        m_uid_to_index.erase(it);
        if (as_defunct) {
            // just release the pointer since Python is defunct
            m_cache[slot_id].second.steal();
        }
        // grab object from cache / invalidate slot
        auto cached_obj_ptr = std::move(m_cache[slot_id].second);
        --m_size;        
        lock.unlock();
        // NOTE: potential object destruction outside of the lock
        return true;
    }
    
    void LangCache::clear(bool expired_only)
    {
        std::vector<ObjectSharedExtPtr> to_destroy;
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        for (auto &item: m_cache) {
            // NOTE: we check for any refernces except from LangCache itself (+1)
            if (item.second.get() && (!expired_only || !LangToolkit::hasAnyLangRefs(item.second.get(), 1))) {
                m_uid_to_index.erase(item.first);
                // grab object for destruction outside of the lock
                to_destroy.push_back(std::move(item.second));
                --m_size;
            }
        }        
        lock.unlock();
        // destroy outside of the lock
    }
    
    void LangCache::clearDefunct()
    {        
        std::unique_lock<std::shared_mutex> lock(m_mutex);
        for (auto &item: m_cache) {
            if (item.second.get()) {
                m_uid_to_index.erase(item.first);
                // just release the pointer since Python is defunct
                item.second.steal();
                --m_size;
            }            
        }
    }
    
    LangCache::ObjectSharedExtPtr LangCache::get(std::uint16_t fixture_id, Address address) const
    {
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        auto uid = makeUID(fixture_id, address);
        auto it = m_uid_to_index.find(uid);
        if (it == m_uid_to_index.end()) {
            return {};
        }
        assert(it->second < m_visited.size());
        // set the visited flag (see Sieve cache eviction algorithm)
        m_visited[it->second] = true;
        return m_cache[it->second].second.get();
    }
    
    std::optional<std::uint32_t> LangCache::evictOne(ObjectSharedExtPtr &evicted, int *num_visited)
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
            if (m_evict_hand->second.get()) {
                if (m_visited[m_evict_hand - m_cache.begin()]) {
                    // visited but not evicted
                    if (num_visited) {
                        ++(*num_visited);
                    }
                    m_visited[m_evict_hand - m_cache.begin()] = false;
                } else {
                    // NOTE: we check for any references except from LangCache itself (+1)
                    if (!LangToolkit::hasAnyLangRefs(m_evict_hand->second.get(), 1)) {
                        // evict the object
                        m_uid_to_index.erase(m_evict_hand->first);
                        evicted = std::move(m_evict_hand->second);                        
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
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        return m_size;
    }
    
    std::size_t LangCache::getCapacity() const {
        std::shared_lock<std::shared_mutex> lock(m_mutex);
        return m_capacity;
    }
    
    std::unique_lock<std::shared_mutex> LangCache::lockUnique() const {
        return std::unique_lock<std::shared_mutex>(m_mutex);
    }

    LangCacheView::LangCacheView(const Fixture &fixture, std::shared_ptr<LangCache> cache_ptr)
        : m_fixture(fixture)
        , m_cache_ptr(cache_ptr)
        , m_cache(*m_cache_ptr)
        , m_fixture_id(m_cache.getFixtureId(fixture))
    {
    }
    
    void LangCacheView::add(Address address, ObjectPtr obj)
    {
        m_cache.add(m_fixture_id, address, obj);
        m_objects.insert(address);
    }

    void LangCacheView::erase(Address address)
    {
        m_cache.erase(m_fixture_id, address);
        m_objects.erase(address);
    }
    
    LangCacheView::ObjectSharedExtPtr LangCacheView::get(Address address) const {
        return m_cache.get(m_fixture_id, address);
    }
        
    void LangCacheView::moveFrom(LangCacheView &other, Address src_address, Address dst_address)
    {
        m_cache.moveFrom(other.m_cache, other.m_fixture_id, src_address, m_fixture_id, dst_address);
        other.m_objects.erase(src_address);
        m_objects.insert(dst_address);
    }
    
    void LangCacheView::clear(bool expired_only, bool as_defunct)
    {
        // copy for erase safety
        auto objects = std::move(m_objects);
        m_objects = {};
        
        // erase expired objects only
        if (expired_only) {
            std::unordered_set<Address> non_expired_objects;
            for (auto addr: objects) {
                if (!m_cache.erase(m_fixture_id, addr, true, as_defunct)) {
                    non_expired_objects.insert(addr);
                }
            }
            m_objects = std::move(non_expired_objects);
        } else {
            for (auto addr: objects) {
                m_cache.erase(m_fixture_id, addr, false, as_defunct);
            }            
        }
    }
    
}