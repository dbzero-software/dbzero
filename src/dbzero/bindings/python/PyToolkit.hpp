// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <deque>
#include <optional>
#include <mutex>
#include "PyTypeManager.hpp"
#include "PyWorkspace.hpp"
#include "PyTypes.hpp"
#include "PyLocks.hpp"
#include "MemoObject.hpp"
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>
#include <dbzero/core/threading/SafeRMutex.hpp>

namespace db0

{

    class Fixture;
    class ProcessTimer;

}

namespace db0::object_model

{

    class Object;
    class Class;
    class ClassFactory;
    struct EnumValue;
    class LongWeakRef;

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
        using ObjectSharedExtPtr = typename PyTypes::ObjectSharedExtPtr;
        using TypeObjectPtr = typename PyTypes::TypeObjectPtr;
        using TypeObjectSharedPtr = typename PyTypes::TypeObjectSharedPtr;
        using TypeManager = db0::python::PyTypeManager;
        using PyWorkspace = db0::python::PyWorkspace;
        using ClassFactory = db0::object_model::ClassFactory;
        using Class = db0::object_model::Class;
        using EnumValue = db0::object_model::EnumValue;
        using LongWeakRef = db0::object_model::LongWeakRef;
        using Object = db0::object_model::Object;
        
        inline static TypeManager &getTypeManager() {
            static TypeManager type_manager;
            return type_manager;    
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
        static ObjectSharedPtr unloadObject(db0::swine_ptr<Fixture> &, Address, const ClassFactory &,
            TypeObjectPtr lang_class = nullptr, std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectSharedPtr tryUnloadObject(db0::swine_ptr<Fixture> &, Address, const ClassFactory &,
            TypeObjectPtr lang_class = nullptr, std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectSharedPtr unloadObject(db0::swine_ptr<Fixture> &, Address, TypeObjectPtr lang_class = nullptr,
            std::uint16_t instance_id = 0, AccessFlags = {});
        
        static bool isExistingObject(db0::swine_ptr<Fixture> &, Address, std::uint16_t instance_id = 0);
        
        static ObjectSharedPtr unloadExpiredRef(db0::swine_ptr<Fixture> &, const LongWeakRef &);
        
        /**
         * @param fixture prefix to unload from
         * @param addr the weak ref object's address (for cache)
         * @param obj_fixture_uuid the fixture UUID of the referenced object
         * @param obj_address the address of the referenced object
         */
        static ObjectSharedPtr unloadExpiredRef(db0::swine_ptr<Fixture> &, Address addr, std::uint64_t obj_fixture_uuid,
            UniqueAddress obj_address);
        
        // Unload with known type & lang class
        // note that lang_class may be a base of the actual type (e.g. MemoBase)
        static ObjectSharedPtr unloadObject(db0::swine_ptr<Fixture> &, Address address,
            std::shared_ptr<Class>, TypeObjectPtr lang_class, AccessFlags = {});
        
        static ObjectSharedPtr unloadList(db0::swine_ptr<Fixture>, Address, std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectSharedPtr unloadIndex(db0::swine_ptr<Fixture>, Address, std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectSharedPtr unloadSet(db0::swine_ptr<Fixture>, Address, std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectSharedPtr unloadDict(db0::swine_ptr<Fixture>, Address, std::uint16_t instance_id = 0, AccessFlags = {});
        static ObjectSharedPtr unloadTuple(db0::swine_ptr<Fixture>, Address, std::uint16_t instance_id = 0, AccessFlags = {});
        // Unload dbzero block instance
        static ObjectSharedPtr unloadBlock(db0::swine_ptr<Fixture>, Address, std::uint16_t instance_id = 0, AccessFlags = {});
        
        // Unload from serialized bytes
        static ObjectSharedPtr deserializeObjectIterable(db0::swine_ptr<Fixture>, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);
        static ObjectSharedPtr deserializeEnumValue(db0::swine_ptr<Fixture>, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);
        static ObjectSharedPtr deserializeEnumValueRepr(db0::swine_ptr<Fixture>, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);
        
        static ObjectSharedPtr unloadByteArray(db0::swine_ptr<Fixture>, Address, AccessFlags = {});
        
        // Creates a new Python instance of EnumValue
        static ObjectSharedPtr makeEnumValue(const EnumValue &);
        static ObjectSharedPtr makeEnumValueRepr(std::shared_ptr<EnumTypeDef> type_def, const char *str_value);
        
        // Create a tuple from a vector of objects
        static ObjectSharedPtr makeTuple(const std::vector<ObjectSharedPtr> &);
        static ObjectSharedPtr makeTuple(std::vector<ObjectSharedPtr> &&);
        // Extract raw elements from PyTuple (of a known size)
        static ObjectPtr *unpackTuple(ObjectPtr py_tuple);
        
        // generate UUID of a dbzero object
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
        
        static bool isValid(ObjectPtr py_object);
        static bool isString(ObjectPtr py_object);
        static bool isIterable(ObjectPtr py_object);
        static bool isSequence(ObjectPtr py_object);
        static bool isType(ObjectPtr py_object);
        // either memo or immutable type
        static bool isAnyMemoType(TypeObjectPtr py_type);
        static bool isAnyMemoObject(ObjectPtr py_object);
        static bool isMemoObject(ObjectPtr py_object);
        static bool isMemoImmutableObject(ObjectPtr py_object);
        static bool isEnumValue(ObjectPtr py_object);
        static bool isFieldDef(ObjectPtr py_object);
        static bool isClassObject(ObjectPtr py_object);
        static bool isTag(ObjectPtr py_object);
        
        static ObjectSharedPtr getIterator(ObjectPtr py_object);
        static ObjectSharedPtr next(ObjectPtr py_object);
        static std::size_t length(ObjectPtr py_object);
        static ObjectSharedPtr getItem(ObjectPtr py_object, std::size_t i);
        // Get value associated fixture UUID (e.g. enum value)
        static std::uint64_t getFixtureUUID(ObjectPtr py_object);
        // Get scoped type's associated fixture UUID (or 0x0)
        static std::uint64_t getFixtureUUID(TypeObjectPtr py_type);
        // Get scoped type's associated prefix name (or nullptr if not defined)
        static const char *getPrefixName(TypeObjectPtr memo_type);        
        // Get memo type associated type_id or nullptr if not defined
        static const char *getMemoTypeID(TypeObjectPtr memo_type);
        static const std::vector<std::string> &getInitVars(TypeObjectPtr memo_type);
        
        static bool isSingleton(TypeObjectPtr);
        // check if a memo type is marked with no_default_tags flag
        static bool isNoDefaultTags(TypeObjectPtr);
        static bool isNoCache(TypeObjectPtr);
        // type marked as immutable
        static bool isImmutable(TypeObjectPtr);
        static FlagSet<MemoOptions> getMemoFlags(TypeObjectPtr);
        
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
        
        // Check if the object has references from other language objects (other than LangCache)
        static bool hasLangRefs(ObjectPtr);
        // Check if there exist any references except specific number of external references
        // in practice this means that object has references either from python or internal buffers except ext_ref_count (e.g. LangCache)
        static bool hasAnyLangRefs(ObjectPtr, unsigned int ext_ref_count);

        // Check if any tag-references exist (i.e. are any tags assigned)
        // NOTE!!! this only works for memo objects
        static bool hasTagRefs(ObjectPtr);
        
        // Extract keys (if present) from a Python dict object
        static std::optional<long> getLong(ObjectPtr py_object, const std::string &key);
        static std::optional<unsigned long long> getUnsignedLongLong(ObjectPtr py_object, const std::string &key);
        static std::optional<unsigned int> getUnsignedInt(ObjectPtr py_object, const std::string &key);
        static std::optional<bool> getBool(ObjectPtr py_object, const std::string &key);
        static std::optional<std::string> getString(ObjectPtr py_object, const std::string &key);
        // Check if key exists in a Python dict object
        static bool hasKey(ObjectPtr py_object, const std::string &key);

        // Blocks until lock acquired
        static SafeRLock lockApi();
        // locks API from a Python context (releases GIL while waiting for the lock)
        static SafeRLock lockPyApi();

        // return base type of TypeObject
        static TypeObjectPtr getBaseType(TypeObjectPtr py_object);

        // return base type of Memo Type object instance or nullptr if base class is not MemoObjec
        static TypeObjectPtr getBaseMemoType(TypeObjectPtr py_object);
        
        // Check the interpreter's status (e.g. returned false if Python is defunct)
        static bool isValid();
        
        // Acquire the interpreter's GIL lock
        // NOTE: returns nullptr if Python not initialized / defunct
        static std::unique_ptr<GIL_Lock> ensureLocked();
        
        // decRef operation for memo objects
        // @return true if reference count was decremented to zero (!hasRefs)
        static bool decRefMemo(bool is_tag, ObjectPtr py_object);
        
    private:
        static PyWorkspace m_py_workspace;
        static SafeRMutex m_api_mutex;
    };
        
}