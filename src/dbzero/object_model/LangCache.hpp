#pragma once

#include "config.hpp"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <dbzero/core/utils/auto_map.hpp>

namespace db0 

{
    
    class Fixture;

    class LangCache
    {
    public:
        using Config = db0::object_model::Config;
        using LangToolkit = typename Config::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;        
        static constexpr std::size_t DEFAULT_CAPACITY = 1024;
        // the default growth step after reaching capacity        
        static constexpr std::size_t DEFAULT_STEP = 32;        
        static constexpr std::size_t DEFAULT_INITIAL_SIZE = 128;
        
        LangCache(std::optional<std::size_t> capacity = {}, std::optional<std::uint32_t> step = {});
        virtual ~LangCache();

        // Add a new instance to cache
        // @return slot id the element was written to
        void add(const Fixture &, std::uint64_t address, ObjectPtr);
        
        void erase(const Fixture &, std::uint64_t address);
        
        // Try retrieving an existing instance from cache
        // nullptr will be returned if the instance has not been found in cache
        ObjectSharedPtr get(const Fixture &, std::uint64_t address) const;

        // Move instance from a different cache (changing its address)
        void moveFrom(LangCache &other, const Fixture &src_fixture, std::uint64_t src_address,
            const Fixture &dst_fixture, std::uint64_t dst_address);

        std::size_t size() const;

        // Remove all cached instances
        void clear();
        
    protected:
        friend class LangCacheView;
        mutable db0::auto_map<const Fixture*, std::uint16_t> m_fixture_to_id;

        std::uint16_t getFixtureId(const Fixture &fixture) const;

    private:
        using CacheItem = std::pair<std::uint64_t, ObjectSharedPtr>;
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

        void add(std::uint16_t fixture_id, std::uint64_t address, ObjectPtr);
        
        void erase(std::uint16_t fixture_id, std::uint64_t address);

        ObjectSharedPtr get(std::uint16_t fixture_id, std::uint64_t address) const;

        void moveFrom(LangCache &other, std::uint16_t src_fixture_id, std::uint64_t src_address,
            std::uint16_t dst_fixture_id, std::uint64_t dst_address);

        // Try evicting one element from cache
        std::optional<std::uint32_t> evictOne();
        std::optional<std::uint32_t> findEmptySlot() const;
        
        // Combine high 48bits of the address with the fixture id
        inline std::uint64_t makeUID(std::uint16_t fixture_id, std::uint64_t address) const {
            // FIXME: this assert to be revisited after including instance_id in the address
            assert((address & 0xFFFF000000000000) == 0);
            return (static_cast<std::uint64_t>(fixture_id) << 48) | (address & 0x0000FFFFFFFFFFFF);
        }

    };
    
    // The fixture-specific LangCache wrapper
    class LangCacheView
    {
    public:
        using ObjectPtr = typename LangCache::ObjectPtr;
        using ObjectSharedPtr = typename LangCache::ObjectSharedPtr;

        LangCacheView(const Fixture &, LangCache &);
        
        void add(std::uint64_t address, ObjectPtr);

        void erase(std::uint64_t address);
        
        ObjectSharedPtr get(std::uint64_t address) const;

        void moveFrom(LangCacheView &other, std::uint64_t src_address, std::uint64_t dst_address);
        
        // Erase all instances added via this view
        void clear();

    private:
        const Fixture &m_fixture;
        LangCache &m_cache;
        const std::uint16_t m_fixture_id;     
        std::unordered_set<std::uint64_t> m_objects;
    };
    
}