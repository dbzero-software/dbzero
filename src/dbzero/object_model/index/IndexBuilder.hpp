// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>

namespace db0::object_model

{
    void unrefAnyMemoObject(db0::swine_ptr<Fixture> &fixture, UniqueAddress address);

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
                
        explicit IndexBuilder(bool passive = false);
        IndexBuilder(std::unordered_set<UniqueAddress> &&remove_null_values,
            std::unordered_set<UniqueAddress> &&add_null_values,
            std::unordered_map<UniqueAddress, ObjectSharedPtr> &&object_cache,
            bool passive = false);
        ~IndexBuilder();

        void add(KeyT key, ObjectPtr obj_ptr);
        void remove(KeyT key, ObjectPtr obj_ptr);
        void add(KeyT key, UniqueAddress address);
        void remove(KeyT key, UniqueAddress address);

        void addNull(ObjectPtr obj_ptr);
        void removeNull(ObjectPtr obj_ptr);
        void addNull(UniqueAddress address);
        void removeNull(UniqueAddress address);

        // Flush and incRef to unique added objects
        void flush(RangeTreeT &index);

        std::unordered_map<UniqueAddress, ObjectSharedPtr> &&releaseObjectCache() {
            return std::move(m_object_cache);
        }

    private:
        typename LangToolkit::TypeManager &m_type_manager;
        bool m_passive = false;

        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the range-tree index
        // NOTE: cache must hold "shared" language reference to prevent object drop (index owns its objects)
        mutable std::unordered_map<UniqueAddress, ObjectSharedPtr> m_object_cache;

        // add to cache and return object's address        
        UniqueAddress addToCache(ObjectPtr);
    };
    
    template <typename KeyT> IndexBuilder<KeyT>::IndexBuilder(bool passive)
        : super_t()
        , m_type_manager(LangToolkit::getTypeManager())
        , m_passive(passive)
    {
    }
    
    template <typename KeyT> IndexBuilder<KeyT>::IndexBuilder(
        std::unordered_set<UniqueAddress> &&remove_null_values, std::unordered_set<UniqueAddress> &&add_null_values, 
        std::unordered_map<UniqueAddress, ObjectSharedPtr> &&object_cache, bool passive)
        : super_t(std::move(remove_null_values), std::move(add_null_values))        
        , m_type_manager(LangToolkit::getTypeManager())
        , m_passive(passive)
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

    template <typename KeyT> void IndexBuilder<KeyT>::add(KeyT key, UniqueAddress address) {
        assert(m_passive && "Address-only index updates are only valid for passive indexes");
        super_t::add(key, address);
    }

    template <typename KeyT> void IndexBuilder<KeyT>::remove(KeyT key, UniqueAddress address) {
        assert(m_passive && "Address-only index updates are only valid for passive indexes");
        super_t::remove(key, address);
    }

    template <typename KeyT> void IndexBuilder<KeyT>::addNull(ObjectPtr obj_ptr) {
        super_t::addNull(addToCache(obj_ptr));
    }
    
    template <typename KeyT> void IndexBuilder<KeyT>::removeNull(ObjectPtr obj_ptr) {
        super_t::removeNull(addToCache(obj_ptr));
    }

    template <typename KeyT> void IndexBuilder<KeyT>::addNull(UniqueAddress address) {
        assert(m_passive && "Address-only index updates are only valid for passive indexes");
        super_t::addNull(address);
    }

    template <typename KeyT> void IndexBuilder<KeyT>::removeNull(UniqueAddress address) {
        assert(m_passive && "Address-only index updates are only valid for passive indexes");
        super_t::removeNull(address);
    }
    
    template <typename KeyT> void IndexBuilder<KeyT>::flush(RangeTreeT &index)
    {
        if (m_passive) {
            std::function<void(UniqueAddress)> no_op_callback = [](UniqueAddress) {};
            super_t::flush(index, &no_op_callback, &no_op_callback);
            return;
        }

        std::function<void(UniqueAddress)> add_callback = [&](UniqueAddress address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            m_type_manager.incObjectRef(it->second.get());
        };
        
        std::function<void(UniqueAddress)> erase_callback = [&](UniqueAddress address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            auto fixture = m_type_manager.extractObjectFixture(it->second.get());
            unrefAnyMemoObject(fixture, address);
        };        
        
        super_t::flush(index, &add_callback, &erase_callback);
        m_object_cache.clear();
    }
    
    template <typename KeyT>
    UniqueAddress IndexBuilder<KeyT>::addToCache(ObjectPtr obj_ptr)
    {
        auto obj_addr = m_type_manager.extractObjectUniqueAddress(obj_ptr);
        if (m_passive) {
            return obj_addr;
        }
        if (m_object_cache.find(obj_addr) == m_object_cache.end()) {
            m_object_cache.emplace(obj_addr, obj_ptr);
        }
        return obj_addr;
    }
    
}
