#pragma once

#include <deque>
#include "PyTypeManager.hpp"
#include "PyWorkspace.hpp"
#include <Python.h>
#include "PyTypes.hpp"
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <optional>

namespace db0 

{

    class Fixture;

}

namespace db0::object_model

{

    class Object;
    class Class;
    class ClassFactory;
    struct EnumValue;

}

namespace db0::python 

{

    /**
     * Python specialized standard language toolkit
     * all of the implemented methods and types must be exposed for each new language integration
    */
    class PyToolkit
    {
    public:
        using ObjectPtr = typename PyTypes::ObjectPtr;
        using ObjectSharedPtr = typename PyTypes::ObjectSharedPtr;
        using TypeObjectPtr = typename PyTypes::TypeObjectPtr;
        using TypeObjectSharedPtr = typename PyTypes::TypeObjectSharedPtr;
        using TypeManager = db0::python::PyTypeManager;
        using PyWorkspace = db0::python::PyWorkspace;
        using ClassFactory = db0::object_model::ClassFactory;
        using Class = db0::object_model::Class;
        using EnumValue = db0::object_model::EnumValue;
        
        inline static TypeManager &getTypeManager() {
            return m_type_manager;    
        }

        inline static PyWorkspace &getPyWorkspace() {
            return m_py_workspace;
        }
        
        /**
         * Construct shared type from raw pointer (shared ownership)         
        */
        static ObjectSharedPtr make_shared(ObjectPtr);
        
        /**
         * Get type of a python object
         * @param py_class the python object instance
        */
        static std::string getTypeName(ObjectPtr py_object);

        /**
         * Get python type name (extracted from a type object)
         * @param py_type the python type object
        */
        static std::string getTypeName(TypeObjectPtr py_type);
        
        /**
         * Retrieve module name where the class is defined
         * @param py_class the python class object
        */
        static std::string getModuleName(TypeObjectPtr py_type);
        
        // Unload with optional instance_id & type validation
        static ObjectPtr unloadObject(db0::swine_ptr<Fixture> &, std::uint64_t address, const ClassFactory &,
            std::optional<std::uint32_t> instance_id = {}, std::shared_ptr<Class> expected_type = nullptr);

        // Unload with known type and optional instance_id validation
        static ObjectPtr unloadObject(db0::swine_ptr<Fixture> &, std::uint64_t address, std::shared_ptr<Class>,
            std::optional<std::uint32_t> instance_id = {});

        // Unload dbzero list instance with optional instance ID validation
        static ObjectPtr unloadList(db0::swine_ptr<Fixture> &, std::uint64_t address,
            std::optional<std::uint32_t> instance_id = {});

        static ObjectPtr unloadIndex(db0::swine_ptr<Fixture> &, std::uint64_t address,
            std::optional<std::uint32_t> instance_id = {});

        static ObjectPtr unloadSet(db0::swine_ptr<Fixture> &, std::uint64_t address,
            std::optional<std::uint32_t> instance_id = {});
        
        static ObjectPtr unloadDict(db0::swine_ptr<Fixture> &, std::uint64_t address,
            std::optional<std::uint32_t> instance_id = {});

        static ObjectPtr unloadTuple(db0::swine_ptr<Fixture> &, std::uint64_t address,
            std::optional<std::uint32_t> instance_id = {});

        // Unload dbzero block instance
        static ObjectPtr unloadBlock(db0::swine_ptr<Fixture> &fixture, std::uint64_t address);

        // Unload from serialized bytes
        static ObjectPtr unloadObjectIterator(db0::swine_ptr<Fixture> &fixture, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);
        
        static ObjectPtr unloadEnumValue(const EnumValue &);

        // generate UUID of a DBZero object
        static ObjectPtr getUUID(ObjectPtr py_object);
                
        // Try converting specific PyObject instance into a tag, possibly adding a new tag into the pool
        using StringPoolT = db0::pools::RC_LimitedStringPool;
        static std::uint64_t addTag(ObjectPtr py_object, StringPoolT &);
        
        static bool isString(ObjectPtr py_object);
        static bool isIterable(ObjectPtr py_object);        
        static bool isType(ObjectPtr py_object);
        static bool isMemoType(TypeObjectPtr py_type);
        static bool isMemoObject(ObjectPtr py_object);
        static bool isEnumValue(ObjectPtr py_object);
        static bool isFieldDef(ObjectPtr py_object);
        static ObjectSharedPtr getIterator(ObjectPtr py_object);
        static ObjectSharedPtr next(ObjectPtr py_object);
        // Get value associated fixture UUID (e.g. enum value)
        static std::uint64_t getFixtureUUID(ObjectPtr py_object);
        // Get scoped type's associated fixture UUID (or 0x0)
        static std::uint64_t getFixtureUUID(TypeObjectPtr py_type);
        // Get scoped type's associated prefix name (or nullptr if not defined)
        static const char *getPrefixName(TypeObjectPtr memo_type);
        // Get memo type associated type_id or nullptr if not defined
        static const char *getMemoTypeID(TypeObjectPtr memo_type);

        static bool isSingleton(TypeObjectPtr py_type);

        inline static void incRef(ObjectPtr py_object) {
            Py_INCREF(py_object);                
        }

        inline static void decRef(ObjectPtr py_object) {
            Py_DECREF(py_object);
        }

        inline static bool compare(ObjectPtr py_object1, ObjectPtr py_object2){
            return PyObject_RichCompareBool(py_object1, py_object2, Py_EQ);
        }
        
        static std::string getLastError();
        
    private:
        static TypeManager m_type_manager;
        static PyWorkspace m_py_workspace;
    };
    
}