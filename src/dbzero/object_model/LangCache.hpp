// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "LangConfig.hpp"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <dbzero/core/utils/auto_map.hpp>
#include <shared_mutex>

namespace db0 

{
    
    class Fixture;
    
    class LangCache
    {
    public:        
        using LangToolkit = typename db0::object_model::LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedExtPtr = typename LangToolkit::ObjectSharedExtPtr;
        static constexpr std::size_t DEFAULT_CAPACITY = 1024;
        // the default growth step after reaching capacity        
        static constexpr std::size_t DEFAULT_STEP = 32;
        static constexpr std::size_t DEFAULT_INITIAL_SIZE = 128;
        
        LangCache(std::optional<std::size_t> capacity = {}, std::optional<std::uint32_t> step = {});
        virtual ~LangCache();
        
        // Add a new instance to cache
        // @return slot id the element was written to
        void add(const Fixture &, Address, ObjectPtr);
        
        // Erase an instance from cache
        // @param expired_only if true, only expired instances will be removed
        bool erase(const Fixture &, Address, bool expired_only = false, bool as_defunct = false);
        
        // Try retrieving an existing instance from cache
        // nullptr will be returned if the instance has not been found in cache
        ObjectSharedExtPtr get(const Fixture &, Address) const;
        
        // Move instance from a different cache (changing its address)
        void moveFrom(LangCache &other, const Fixture &src_fixture, Address src_address,
            const Fixture &dst_fixture, Address dst_address);

        std::size_t size() const;

        std::size_t getCapacity() const;

        // Remove all cached instances
        // NOTE: only instances with NO existing references will be removed from cache
        // this is to avoid instance duplication in the program (e.g. when later being fetched by UUID)
        void clear(bool expired_only);
        
        // Release all cached instances (cannot be deleted since Python interpreter is no longer available)
        void clearDefunct();
        
    protected:
        friend class LangCacheView;
        mutable db0::auto_map<const Fixture*, std::uint16_t> m_fixture_to_id;

        std::uint16_t getFixtureId(const Fixture &fixture) const;

        void add(std::uint16_t fixture_id, Address, ObjectPtr);
        
        bool erase(std::uint16_t fixture_id, Address, bool expired_only = false, bool as_defunct = false);
        
        std::unique_lock<std::shared_mutex> lockUnique() const;
        
    private:
        mutable std::shared_mutex m_mutex;
        // UID + instance pair
        using CacheItem = std::pair<std::uint64_t, ObjectSharedExtPtr>;
        const std::size_t m_capacity;
        const std::uint32_t m_step;
        // the number of currently cached objects
        std::size_t m_size = 0;
        // positionally encoded cached objects (uid + instance)
        mutable std::vector<CacheItem> m_cache;
        mutable std::vector<CacheItem>::iterator m_evict_hand;
        mutable std::vector<CacheItem>::iterator m_insert_hand;
        // the "visited" flags (see Sieve cache eviction algorithm)
        mutable std::vector<bool> m_visited;
        // instance UID to index in cache
        std::unordered_map<std::uint64_t, std::uint32_t> m_uid_to_index;
        
        bool isFull() const;
        
        ObjectSharedExtPtr get(std::uint16_t fixture_id, Address) const;
        
        void moveFrom(LangCache &other, std::uint16_t src_fixture_id, Address src_address,
            std::uint16_t dst_fixture_id, Address dst_address);
        
        // Try evicting one element from cache
        std::optional<std::uint32_t> evictOne(ObjectSharedExtPtr &evicted, int *num_visited = nullptr);
        std::optional<std::uint32_t> findEmptySlot() const;
        
        // Combine high 50 bits of the physical address (aka memory offset) with the fixture id
        inline std::uint64_t makeUID(std::uint16_t fixture_id, Address address) const {
            return (static_cast<std::uint64_t>(fixture_id) << 50) | address.getOffset();
        }
        
        void resize(std::size_t new_size);
    };
    
    // The fixture-specific LangCache wrapper
    class LangCacheView
    {
    public:
        using ObjectPtr = typename LangCache::ObjectPtr;
        using ObjectSharedExtPtr = typename LangCache::ObjectSharedExtPtr;
        
        LangCacheView(const Fixture &, std::shared_ptr<LangCache>);
        
        void add(Address, ObjectPtr);

        void erase(Address);
        
        ObjectSharedExtPtr get(Address) const;

        void moveFrom(LangCacheView &other, Address src_address, Address dst_address);
        
        // Erase all instances added via this view
        void clear(bool expired_only, bool as_defunct = false);
        
    private:
        const Fixture &m_fixture;
        std::shared_ptr<LangCache> m_cache_ptr;
        LangCache &m_cache;
        const std::uint16_t m_fixture_id;
        std::unordered_set<Address> m_objects;
    };
    
}