#pragma once

#include <string>
#include <memory>
#include <vector>
#include <cassert>
#include <optional>
#include "ValueTable.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/object_model/value/XValue.hpp>
#include "ValueTable.hpp"

namespace db0 

{
    
    class Fixture;
    
}

namespace db0::object_model

{

    class Class;
    class Object;
    class ObjectInitializerManager;
    using Fixture = db0::Fixture;

    /**
     * Class to store status of the instance / member intialization process (corresponds to __init__)
     */
    class ObjectInitializer
    {
    public:
        using XValue = db0::object_model::XValue;
                
        // loc - position in the initializer manager's array
        ObjectInitializer(ObjectInitializerManager &, std::uint32_t loc, Object &object, std::shared_ptr<Class> db0_class);

        void init(Object &object, std::shared_ptr<Class> db0_class);

        void set(unsigned int at, StorageClass storage_class, Value value);

        void setInstanceKey(const char *str_key);
        
        /**
         * Collect and retrieve pos-vt / index-vt data
         * @return first / end pointers to the index-vt table
        */
        std::pair<const XValue*, const XValue*> getData(PosVT::Data &data);

        /**
         * Finalize all initializations and prepare initializer for a next object
         */
        void close();
        
        inline bool closed() {
            return m_closed;
        }
        
        // Try pulling an existing initialization value from under a specific index
        bool tryGetAt(unsigned int index, std::pair<StorageClass, Value> &) const;

        bool operator==(const Object &other) {
            return m_object_ptr == &other;
        }
        
        inline Class &getClass() {
            return *m_class;
        }

        std::shared_ptr<Class> getClassPtr() const {
            return m_class;
        }

        inline std::uint32_t getRefCount() const {
            return m_ref_count;
        }
        
        db0::swine_ptr<Fixture> getFixture() const;
        db0::swine_ptr<Fixture> tryGetFixture() const;

        // performs a deferred incRef on an actual object instance (the ref-count reflected upon creation)
        void incRef();
        
        bool empty() const;
        
        // Can only be executed on an empty initializer
        void setClass(std::shared_ptr<Class>);
        
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
        Object *m_object_ptr = nullptr;
        std::shared_ptr<Class> m_class;
        // indexed initialization values
        mutable std::vector<XValue> m_values;
        // number of m_values already sorted
        mutable std::size_t m_sorted_size = 0;
        // key to be assigned to instance post-initialization
        std::optional<std::string> m_instance_key;
        std::uint32_t m_ref_count = 0;

        // returns the number of unique elements extracted        
        std::uint32_t finalizeValues();
    };
    
    /**
     * The purpose of this class is to hold Object initializers during the construction process.
     * We could simply keep 'initializer' as an Object member, but since this is a short-lived object, it would be a waste of space.
     * Also InitializerManager helps us reuse Initializer instances, saving on memory allocations.
    */
    class ObjectInitializerManager
    {
    public:
        ObjectInitializerManager() = default;

        void addInitializer(Object &object, std::shared_ptr<Class> db0_class);
        
        /**
         * Close the initializer and retrieve object's class
        */
        std::shared_ptr<Class> closeInitializer(const Object &object);

        /**
         * Close the initializer if it exists
        */
        std::shared_ptr<Class> tryCloseInitializer(const Object &object);

        ObjectInitializer &getInitializer(const Object &object) const
        {
            auto result = findInitializer(object);
            if (result) {
                return *result;
            }
            THROWF(InternalException) << "Initializer not found" << THROWF_END;
        }

        ObjectInitializer *findInitializer(const Object &object) const;

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
    
}