// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>

namespace db0::object_model

{

    /**
     * Wraps extends the RangeTree::Builder providing persistency cache for dbzero instances
    */
    template <typename KeyT> class IndexBuilder: public RangeTree<KeyT, UniqueAddress>::Builder
    {
    public:
        using super_t = typename RangeTree<KeyT, UniqueAddress>::Builder;
        using RangeTreeT = RangeTree<KeyT, UniqueAddress>;
        using LangToolkit = typename LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using ObjectSharedExtPtr = typename LangToolkit::ObjectSharedExtPtr;
                
        IndexBuilder();
        IndexBuilder(std::unordered_set<UniqueAddress> &&remove_null_values,
            std::unordered_set<UniqueAddress> &&add_null_values,
            std::unordered_map<UniqueAddress, ObjectSharedPtr> &&object_cache);
        ~IndexBuilder();

        void add(KeyT key, ObjectPtr obj_ptr);
        void remove(KeyT key, ObjectPtr obj_ptr);

        void addNull(ObjectPtr obj_ptr);
        void removeNull(ObjectPtr obj_ptr);

        // Flush and incRef to unique added objects
        void flush(RangeTreeT &index);

        std::unordered_map<UniqueAddress, ObjectSharedPtr> &&releaseObjectCache() {
            return std::move(m_object_cache);
        }

    private:
        typename LangToolkit::TypeManager &m_type_manager;

        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the range-tree index
        // NOTE: cache must hold "shared" language reference to prevent object drop (index owns its objects)
        mutable std::unordered_map<UniqueAddress, ObjectSharedPtr> m_object_cache;

        // add to cache and return object's address        
        UniqueAddress addToCache(ObjectPtr);
    };
    
    template <typename KeyT> IndexBuilder<KeyT>::IndexBuilder()
        : super_t()
        , m_type_manager(LangToolkit::getTypeManager())
    {
    }
    
    template <typename KeyT> IndexBuilder<KeyT>::IndexBuilder(
        std::unordered_set<UniqueAddress> &&remove_null_values, std::unordered_set<UniqueAddress> &&add_null_values, 
        std::unordered_map<UniqueAddress, ObjectSharedPtr> &&object_cache)
        : super_t(std::move(remove_null_values), std::move(add_null_values))        
        , m_type_manager(LangToolkit::getTypeManager())
        , m_object_cache(std::move(object_cache))
    {
    }
    
    template <typename KeyT> IndexBuilder<KeyT>::~IndexBuilder()
    {
    }
    
    template <typename KeyT> void IndexBuilder<KeyT>::add(KeyT key, ObjectPtr obj_ptr) {
        super_t::add(key, addToCache(obj_ptr));
    }

    template <typename KeyT> void IndexBuilder<KeyT>::remove(KeyT key, ObjectPtr obj_ptr) {              
        super_t::remove(key, addToCache(obj_ptr));
    }

    template <typename KeyT> void IndexBuilder<KeyT>::addNull(ObjectPtr obj_ptr) {
        super_t::addNull(addToCache(obj_ptr));
    }
    
    template <typename KeyT> void IndexBuilder<KeyT>::removeNull(ObjectPtr obj_ptr) {
        super_t::removeNull(addToCache(obj_ptr));
    }
    
    template <typename KeyT> void IndexBuilder<KeyT>::flush(RangeTreeT &index)
    {        
        std::function<void(UniqueAddress)> add_callback = [&](UniqueAddress address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            m_type_manager.extractMutableAnyObject(it->second.get()).incRef(false);
        };
        
        std::function<void(UniqueAddress)> erase_callback = [&](UniqueAddress address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            m_type_manager.extractMutableAnyObject(it->second.get()).decRef(false);
        };        
        
        super_t::flush(index, &add_callback, &erase_callback);
        m_object_cache.clear();
    }
    
    template <typename KeyT>
    UniqueAddress IndexBuilder<KeyT>::addToCache(ObjectPtr obj_ptr)
    {
        auto obj_addr = m_type_manager.extractAnyObject(obj_ptr).getUniqueAddress();
        if (m_object_cache.find(obj_addr) == m_object_cache.end()) {
            m_object_cache.emplace(obj_addr, obj_ptr);
        }
        return obj_addr;
    }
    
}