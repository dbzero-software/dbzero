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
        
        /**
         * Try retrieving existing object from cache
         * @return nullptr if not found
        */
        ObjectPtr get(std::uint64_t address) const;
        
        /**
         * Add object to cache (weak reference)
         * @param strong_ref if true then object will be inc-refed and persisted until erase
        */
        void add(std::uint64_t address, ObjectPtr, bool strong_ref);

        void erase(std::uint64_t address);
        
        std::size_t size() const;

    private:

        struct LangCacheItem
        {
            ObjectPtr m_object;
            bool m_strong_ref;
        };

        std::unordered_map<std::uint64_t, LangCacheItem> m_cache;
    };

}