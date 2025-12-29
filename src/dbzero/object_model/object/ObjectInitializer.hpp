// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <string>
#include <memory>
#include <vector>
#include <cassert>
#include <optional>
#include <functional>
#include "ValueTable.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/collections/vector/SparseBoolMatrix.hpp>
#include <dbzero/object_model/value/XValue.hpp>
#include "ValueTable.hpp"
#include "XValuesVector.hpp"
#include "lofi_store.hpp"

namespace db0

{
    
    class Fixture;
    
}

namespace db0::object_model

{

    class Class;
    class Object;
    class ObjectInitializer;
    using Fixture = db0::Fixture;

    /**
     * The purpose of this class is to hold Object initializers during the construction process.
     * We could simply keep 'initializer' as an Object member, but since this is a short-lived object, it would be a waste of space.
     * Also InitializerManager helps us reuse Initializer instances, saving on memory allocations.
    */
    class ObjectInitializerManager
    {
    public:
        ObjectInitializerManager() = default;

        template <typename T, typename... Args> 
        void addInitializer(T &object, Args&& ...args);
        
        // Close the initializer and retrieve object's class
        template <typename T>
        std::shared_ptr<Class> closeInitializer(const T &object);
        
        // Close the initializer if it exists
        template <typename T>
        std::shared_ptr<Class> tryCloseInitializer(const T &object);
        
        template <typename T>
        ObjectInitializer &getInitializer(const T &object) const
        {
            auto result = findInitializer(object);
            if (result) {
                return *result;
            }
            THROWF(InternalException) << "Initializer not found" << THROWF_END;
        }

        template <typename T>
        ObjectInitializer *findInitializer(const T &object) const;

    protected:
        friend class ObjectInitializer;
        void closeAt(std::uint32_t loc);

    private:
        mutable std::vector<std::unique_ptr<ObjectInitializer> > m_initializers;
        // number of active object initializers
        std::size_t m_active_count = 0;
        // number of non-null initializer instances
        std::size_t m_total_count = 0;
    };

    /**
     * Class to store status of the instance / member intialization process (corresponds to __init__)
     */
    class ObjectInitializer
    {
    public:
        using XValue = db0::object_model::XValue;
        using TypeInitializer = std::function<std::shared_ptr<Class>(db0::swine_ptr<Fixture> &)>;
        
        // loc - position in the initializer manager's array
        template <typename T>
        ObjectInitializer(ObjectInitializerManager &manager, std::uint32_t loc, T &object, std::shared_ptr<Class> type)
            : m_manager(manager)
            , m_loc(loc)
            , m_closed(false)
            , m_object_ptr(&object)
            , m_class(type)
            // NOTE: limit the dim-2 size to improve performance
            , m_has_value(lofi_store<2>::size())
        {        
        }

        // alternative constructor for lazy type initialization
        template <typename T>
        ObjectInitializer(ObjectInitializerManager &manager, std::uint32_t loc, T &object, TypeInitializer &&type_initializer)
            : m_manager(manager)
            , m_loc(loc)
            , m_closed(false)
            , m_object_ptr(&object)
            // NOTE: limit the dim-2 size to improve performance
            , m_has_value(lofi_store<2>::size())
            , m_type_initializer(std::move(type_initializer))
        {        
        }
        
        template <typename T>
        void init(T &object, std::shared_ptr<Class> type)
        {
            assert(m_closed);        
            m_closed = false;
            m_object_ptr = &object;
            m_class = type;
        }

        template <typename T>
        void init(T &object, TypeInitializer &&type_initializer)
        {
            assert(m_closed);
            assert(!m_class);
            m_closed = false;
            m_object_ptr = &object;
            m_type_initializer = std::move(type_initializer);

        }
        
        // @param mask required for lo-fi types (pack-2)
        void set(std::pair<std::uint32_t, std::uint32_t> loc, StorageClass storage_class, Value value, 
            std::uint64_t mask = 0);
        bool remove(std::pair<std::uint32_t, std::uint32_t> loc, std::uint64_t mask = 0);
        
        // Allows migrating initialization to other fixture (only for empty ObjectInitializer)
        // @return false if operation failed (exception not thrown)
        bool trySetFixture(db0::swine_ptr<Fixture> &fixture);
        
        /**
         * Collect and retrieve pos-vt / index-vt data
         * @return first / end pointers to the index-vt table
        */
        std::pair<const XValue*, const XValue*> getData(PosVT::Data &data, unsigned int &pos_vt_offset);
        
        /**
         * Finalize all initializations and prepare initializer for a next object
         */
        void close();
        
        inline bool closed() {
            return m_closed;
        }
        
        // Try pulling an existing initialization value from under a specific index
        // NOTE always the whole value is retrieved (no mask support)
        bool tryGetAt(std::pair<std::uint32_t, std::uint32_t> loc, std::pair<StorageClass, Value> &) const;
        
        template <typename T>
        bool operator==(const T &other) {
            return m_object_ptr == &other;
        }
        
        Class &getClass() const;
        
        std::shared_ptr<Class> getClassPtr() const;

        inline std::pair<std::uint32_t, std::uint32_t> getRefCounts() const {
            return m_ref_counts;
        }
        
        db0::swine_ptr<Fixture> getFixture() const;
        db0::swine_ptr<Fixture> tryGetFixture() const;

        // performs a deferred incRef on an actual object instance (the ref-count reflected upon creation)
        void incRef(bool is_tag);
        
        bool empty() const;
                
    protected:
        friend class ObjectInitializerManager;
        void reset();

        void operator=(std::uint32_t new_loc);
        
    private:
        // maximum size of the position-encoded value-block (pos-VT)
        static constexpr std::size_t POSVT_MAX_SIZE = 128;
        ObjectInitializerManager &m_manager;
        std::uint32_t m_loc = std::numeric_limits<std::uint32_t>::max();
        bool m_closed = true;
        // pointer to an implementation-specific type
        void *m_object_ptr = nullptr;
        mutable std::shared_ptr<Class> m_class;
        // indexed initialization values
        mutable XValuesVector m_values;
        // flags indicating values presence (for fast removal pruning)
        mutable SparseBoolMatrix m_has_value;
        std::pair<std::uint32_t, std::uint32_t> m_ref_counts = {0, 0};
        mutable db0::swine_ptr<Fixture> m_fixture;
        mutable TypeInitializer m_type_initializer;
    };
    
    template <typename T, typename... Args>
    void ObjectInitializerManager::addInitializer(T &object, Args&& ...args)
    {
        if (m_active_count < m_total_count) {
            auto loc = m_active_count++;
            m_initializers[loc]->init(object, std::forward<Args>(args)...);
            return;
        }
        
        for (;;) {
            if (m_total_count < m_initializers.size()) {
                auto loc = m_total_count++;
                m_initializers[loc].reset(new ObjectInitializer(*this, loc, object, std::forward<Args>(args)...));
                ++m_active_count;
                return;
            }
            // double the number of slots
            m_initializers.resize(std::max(1u, static_cast<unsigned int>(m_initializers.size())) << 1);
        }
    }
    
    template <typename T>
    std::shared_ptr<Class> ObjectInitializerManager::tryCloseInitializer(const T &object)
    {        
        for (auto i = 0u; i < m_active_count; ++i) {
            if (m_initializers[i]->operator==(object)) {
                auto result = m_initializers[i]->getClassPtr();
                closeAt(i);
                return result;
            }
        }
        return nullptr;
    }
    
    template <typename T>
    std::shared_ptr<Class> ObjectInitializerManager::closeInitializer(const T &object)
    {
        auto result = tryCloseInitializer(object);
        if (result) {
            return result;
        }
        THROWF(db0::InternalException) << "Initializer not found" << THROWF_END;
    }
    
    template <typename T>
    ObjectInitializer *ObjectInitializerManager::findInitializer(const T &object) const
    {
        for (auto i = 0u; i < m_active_count; ++i) {
            if (m_initializers[i]->operator==(object)) {
                // move to front to allow faster lookup the next time
                if (i != 0) {
                    std::swap(m_initializers[i], m_initializers[0]);
                    *(m_initializers[i]) = i;
                    *(m_initializers[0]) = 0;
                }
                return m_initializers[0].get();
            }
        }
        return nullptr;   
    }
        
}