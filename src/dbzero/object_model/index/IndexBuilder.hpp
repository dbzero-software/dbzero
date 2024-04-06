#pragma once

#include <dbzero/object_model/config.hpp>
#include <dbzero/object_model/object/Object.hpp>

namespace db0::object_model

{

    /**
     * Wraps extends the RangeTree::Builder providing persistency cache for DBZero instances
    */
    template <typename KeyT> class IndexBuilder: public RangeTree<KeyT, std::uint64_t>::Builder
    {
    public:
        using RangeTreeT = RangeTree<KeyT, std::uint64_t>;
        using LangToolkit = typename Config::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using super_t = typename RangeTree<KeyT, std::uint64_t>::Builder;        

        IndexBuilder();
        IndexBuilder(std::vector<std::uint64_t> &&null_values);
        
        void add(KeyT key, ObjectPtr obj_ptr);

        void addNull(ObjectPtr obj_ptr);

        // Flush and incRef to unique added objects
        void flush(RangeTreeT &index);

    private:
        typename LangToolkit::TypeManager &m_type_manager;

        // A cache of language objects held until flush/close is called
        // it's required to prevent unreferenced objects from being collected by GC
        // and to handle callbacks from the range-tree index
        mutable std::unordered_map<std::uint64_t, ObjectSharedPtr> m_object_cache;
    };

    template <typename KeyT> IndexBuilder<KeyT>::IndexBuilder()
        : super_t()
        , m_type_manager(LangToolkit::getTypeManager())
    {
    }
    
    template <typename KeyT> IndexBuilder<KeyT>::IndexBuilder(std::vector<std::uint64_t> &&null_values)
        : super_t(std::move(null_values))
        , m_type_manager(LangToolkit::getTypeManager())
    {
    }

    template <typename KeyT> void IndexBuilder<KeyT>::add(KeyT key, ObjectPtr obj_ptr)
    {
        auto obj_addr = m_type_manager.extractObject(obj_ptr).getAddress();
        super_t::add(key, obj_addr);
        // cache object locally
        if (m_object_cache.find(obj_addr) == m_object_cache.end()) {
            m_object_cache.emplace(obj_addr, obj_ptr);
        }
    }
    
    template <typename KeyT> void IndexBuilder<KeyT>::addNull(ObjectPtr obj_ptr)
    {
        auto obj_addr = m_type_manager.extractObject(obj_ptr).getAddress();
        super_t::addNull(obj_addr);
        // cache object locally
        if (m_object_cache.find(obj_addr) == m_object_cache.end()) {
            m_object_cache.emplace(obj_addr, obj_ptr);
        }
    }

    template <typename KeyT> void IndexBuilder<KeyT>::flush(RangeTreeT &index)
    {
        std::function<void(std::uint64_t)> add_callback = [&](std::uint64_t address) {
            auto it = m_object_cache.find(address);
            assert(it != m_object_cache.end());
            m_type_manager.extractObject(it->second.get()).incRef();
        };

        super_t::flush(index, &add_callback);
    }

}