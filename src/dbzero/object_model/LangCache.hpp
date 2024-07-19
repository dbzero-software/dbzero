#pragma once

#include "config.hpp"

namespace db0::object_model

{

    /**
     * Language-specific object cache.
    */
    class LangCache
    {
    public:
        using LangToolkit = Config::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

        LangCache() = default;
        virtual ~LangCache();
        
        /**
         * Try retrieving existing object from cache
         * @return nullptr if not found
        */
        ObjectSharedPtr get(std::uint64_t address) const;
        
        /**
         * Add object to cache (weak reference)
         * @param strong_ref if true then object will be inc-refed and persisted until erase
        */
        void add(std::uint64_t address, ObjectPtr, bool strong_ref);

        // Move instance from a different cache (changing its address)
        void moveFrom(LangCache &other, std::uint64_t src_address, 
            std::uint64_t dst_address);
        
        void erase(std::uint64_t address);
        
        std::size_t size() const;

        void clear();

    private:

        struct LangCacheItem
        {
            ObjectPtr m_object;
            bool m_strong_ref;
        };

        std::unordered_map<std::uint64_t, LangCacheItem> m_cache;
    };

}