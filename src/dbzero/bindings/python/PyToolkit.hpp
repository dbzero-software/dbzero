#pragma once

#include <Python.h>
#include <deque>
#include <optional>
#include <mutex>
#include "PyTypeManager.hpp"
#include "PyWorkspace.hpp"
#include "PyTypes.hpp"
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>

#define PY_API_FUNC auto __api_lock = db0::python::PyToolkit::lockApi();

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
        
        template <typename T> inline static PyWrapper<T> *getWrapperTypeOf(ObjectPtr ptr) {
            return static_cast<PyWrapper<T> *>(ptr);
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
         * Retrieve module name where the class is defined (may not be available e.g. for built-in types)
         * @param py_class the python class object
        */
        static std::optional<std::string> tryGetModuleName(TypeObjectPtr py_type);
        static std::string getModuleName(TypeObjectPtr py_type);
        
        // Unload with type resolution
        // optionally may use specific lang class (e.g. MemoBase)
        static ObjectSharedPtr unloadObject(db0::swine_ptr<Fixture>, std::uint64_t address, const ClassFactory &,
            TypeObjectPtr lang_class = nullptr);
        
        // Unload with known type & lang class
        // note that lang_class may be a base of the actual type (e.g. MemoBase)
        static ObjectSharedPtr unloadObject(db0::swine_ptr<Fixture>, std::uint64_t address,
            std::shared_ptr<Class>, TypeObjectPtr lang_class);
        
        static ObjectSharedPtr unloadList(db0::swine_ptr<Fixture>, std::uint64_t address);

        static ObjectSharedPtr unloadIndex(db0::swine_ptr<Fixture>, std::uint64_t address);

        static ObjectSharedPtr unloadSet(db0::swine_ptr<Fixture>, std::uint64_t address);
        
        static ObjectSharedPtr unloadDict(db0::swine_ptr<Fixture>, std::uint64_t address);

        static ObjectSharedPtr unloadTuple(db0::swine_ptr<Fixture>, std::uint64_t address);

        // Unload dbzero block instance
        static ObjectSharedPtr unloadBlock(db0::swine_ptr<Fixture>, std::uint64_t address);
        
        // Unload from serialized bytes
        static ObjectSharedPtr unloadObjectIterable(db0::swine_ptr<Fixture>, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);
        
        static ObjectSharedPtr unloadByteArray(db0::swine_ptr<Fixture>, std::uint64_t address);

        // Creates a new Python instance of EnumValue
        static ObjectSharedPtr makeEnumValue(const EnumValue &);
        
        // generate UUID of a DBZero object
        static ObjectPtr getUUID(ObjectPtr py_object);
        
        // Try converting specific PyObject instance into a tag, possibly adding a new tag into the pool        
        using StringPoolT = db0::pools::RC_LimitedStringPool;
        
        /**
         * Adds a new object or increase ref-count of the existing element
         * @param inc_ref - whether to increase ref-count of the existing element, note that for
         * newly created elements ref-count is always set to 1 (in such case inc_ref will be flipped from false to true)
        */
        static std::uint64_t addTagFromString(ObjectPtr py_object, StringPoolT &, bool &inc_ref);
        // Get existing tag or return 0x0 if not found
        static std::uint64_t getTagFromString(ObjectPtr py_object, StringPoolT &);
        
        static bool isString(ObjectPtr py_object);
        static bool isIterable(ObjectPtr py_object);
        static bool isType(ObjectPtr py_object);
        static bool isMemoType(TypeObjectPtr py_type);
        static bool isMemoObject(ObjectPtr py_object);
        static bool isEnumValue(ObjectPtr py_object);
        static bool isFieldDef(ObjectPtr py_object);
        static bool isClassObject(ObjectPtr py_object);
        static bool isTag(ObjectPtr py_object);
        
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
        
        static bool compare(ObjectPtr, ObjectPtr);

        static std::string getLastError();
        // indicate failed operation with a specific value/code
        static void setError(ObjectPtr err_obj, std::uint64_t err_value);
        
        static unsigned int getRefCount(ObjectPtr);
        
        // Extract keys (if present) from a Python dict object
        static std::optional<long> getLong(ObjectPtr py_object, const std::string &key);
        static std::optional<bool> getBool(ObjectPtr py_object, const std::string &key);
        static std::optional<std::string> getString(ObjectPtr py_object, const std::string &key);

        // block until lock acquired
        static std::unique_lock<std::recursive_mutex> lockApi();

    private:
        static PyWorkspace m_py_workspace;
        static TypeManager m_type_manager;
        static std::recursive_mutex m_api_mutex;        
    };
    
}