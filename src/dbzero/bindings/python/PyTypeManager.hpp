// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <functional>
#include <memory>
#include <dbzero/bindings/TypeId.hpp>
#include "PyTypes.hpp"
#include <dbzero/bindings/python/types/PyEnumType.hpp>
#include <dbzero/bindings/python/MemoTypeDecoration.hpp>
#include "MemoObject.hpp"

namespace db0

{

    class ProcessTimer;

}

namespace db0::object_model {

    class Object;
    class ObjectImmutableImpl;
    class ObjectAnyImpl;
    class Class;
    class List;
    class Set;
    class Tuple;
    class Dict;
    class TagSet;
    class Index;
    class ObjectIterable;
    struct EnumValue;
    struct EnumValueRepr;
    struct FieldDef;
    class TagDef;
    class ByteArray;
    class PyWeakProxy;
    
}

namespace db0::object_model::pandas {
    class Block;
}

namespace db0::python

{
    
    class MemoTypeDecoration;
    
    /**
     * The class dedicated to recognition of Python types
     */
    class PyTypeManager
    {
    public :
        using TypeId = db0::bindings::TypeId;
        using ObjectPtr = typename PyTypes::ObjectPtr;
        using ObjectSharedPtr = typename PyTypes::ObjectSharedPtr;
        using TypeObjectPtr = typename PyTypes::TypeObjectPtr;
        using TypeObjectSharedPtr = typename PyTypes::TypeObjectSharedPtr;
        using MemoObject = db0::python::MemoObject;
        using MemoImmutableObject = db0::python::MemoImmutableObject;
        using MemoAnyObject = db0::python::MemoAnyObject;
        using Object = db0::object_model::Object;
        using ObjectImmutableImpl = db0::object_model::ObjectImmutableImpl;
        using ObjectAnyImpl = db0::object_model::ObjectAnyImpl;
        using List = db0::object_model::List;
        using Set = db0::object_model::Set;
        using Tuple = db0::object_model::Tuple;
        using Dict = db0::object_model::Dict;
        using TagSet = db0::object_model::TagSet;
        using Index = db0::object_model::Index;
        using ObjectIterable = db0::object_model::ObjectIterable;
        using EnumValue = db0::object_model::EnumValue;
        using EnumValueRepr = db0::object_model::EnumValueRepr;
        using FieldDef = db0::object_model::FieldDef;
        using Class = db0::object_model::Class;
        using TagDef = db0::object_model::TagDef;
        using ByteArray = db0::object_model::ByteArray;

        PyTypeManager();
        ~PyTypeManager();
        
        /**
         * Add string to pool and return a managed pointer
        */
        const char *getPooledString(std::string);
        const char *getPooledString(const char *);
        
        // Recognize Python type of a specific object instance as TypeId (may return TypeId::UNKNOWN)
        TypeId getTypeId(ObjectPtr object_instance) const;
        std::optional<PyTypeManager::TypeId> tryGetTypeId(ObjectPtr ptr) const;
        TypeId getTypeId(TypeObjectPtr py_type) const;
        std::optional<PyTypeManager::TypeId> tryGetTypeId(TypeObjectPtr ptr) const;
        std::string getLangTypeName(TypeObjectPtr) const;
        
        // Retrieve a Python type object by TypeId (note that a dbzero extension type may be returned)
        // to return a native type use getTypeObject(asNative(TypeId))
        ObjectSharedPtr getTypeObject(TypeId) const;
        // If the mapping is not found, returns Py_None
        ObjectSharedPtr tryGetTypeObject(TypeId) const;
        
        // Extracts reference to Object or ObjectImmutableImpl from a memo instance
        template <typename MemoImplT>
        const typename MemoImplT::ExtT &extractObject(ObjectPtr memo_ptr) const;
        template <typename MemoImplT>
        typename MemoImplT::ExtT &extractMutableObject(ObjectPtr memo_ptr) const;
        
        template <typename MemoImplT>
        typename MemoImplT::ExtT *tryExtractMutableObject(ObjectPtr memo_ptr) const;
        
        // Extracts reference to common object part from a memo instance
        const ObjectAnyImpl &extractAnyObject(ObjectPtr) const;
        ObjectAnyImpl &extractMutableAnyObject(ObjectPtr) const;
        
        const ObjectAnyImpl *tryExtractObject(ObjectPtr memo_ptr) const;        

        const List &extractList(ObjectPtr list_ptr) const;
        List &extractMutableList(ObjectPtr list_ptr) const;
        const Set &extractSet(ObjectPtr set_ptr) const;
        Set &extractMutableSet(ObjectPtr set_ptr) const;
        std::int64_t extractInt64(ObjectPtr int_ptr) const;
        std::uint64_t extractUInt64(ObjectPtr) const;
        std::uint64_t extractUInt64(TypeId, ObjectPtr) const;
        const Tuple &extractTuple(ObjectPtr tuple_ptr) const;
        // e.g. for incRef
        Tuple &extractMutableTuple(ObjectPtr tuple_ptr) const;
        const Dict &extractDict(ObjectPtr dict_ptr) const;
        Dict &extractMutableDict(ObjectPtr dict_ptr) const;
        TagSet &extractTagSet(ObjectPtr tag_set_ptr) const;
        const Index &extractIndex(ObjectPtr index_ptr) const;
        Index &extractMutableIndex(ObjectPtr index_ptr) const;
        const EnumValue &extractEnumValue(ObjectPtr enum_value_ptr) const;
        const EnumValueRepr &extractEnumValueRepr(ObjectPtr enum_value_repr_ptr) const;
        ObjectIterable &extractObjectIterable(ObjectPtr) const;
        FieldDef &extractFieldDef(ObjectPtr) const;
        std::string extractString(ObjectPtr) const;
        TypeObjectPtr getTypeObject(ObjectPtr py_type) const;
        ObjectPtr getLangObject(TypeObjectPtr py_type) const;
        std::shared_ptr<const Class> extractConstClass(ObjectPtr py_class) const;
        const TagDef &extractTag(ObjectPtr py_tag) const;
        ByteArray &extractMutableByteArray(ObjectPtr) const;
        
        ObjectPtr getBadPrefixError() const;
        ObjectPtr getClassNotFoundError() const;
        ObjectPtr getReferenceError() const;        
        
        /**
         * Called with each new memo type
        */
        void addMemoType(TypeObjectPtr, const char *type_id, MemoTypeDecoration &&);
        MemoTypeDecoration &getMemoTypeDecoration(TypeObjectPtr);
        const MemoTypeDecoration &getMemoTypeDecoration(TypeObjectPtr) const;
        
        // Called to register each newly created db0.enum type
        void addEnum(PyEnum *);

        /**
         * Try finding Python type by a given name variant
        */
        TypeObjectPtr findType(const std::string &variant_name) const;
        ObjectSharedPtr tryFindEnum(const std::string &variant_name) const;
        ObjectSharedPtr tryFindEnum(const EnumDef &) const;        
        std::shared_ptr<EnumTypeDef> tryFindEnumTypeDef(const std::string &variant_name) const;
        std::shared_ptr<EnumTypeDef> tryFindEnumTypeDef(const EnumDef &) const;
        std::shared_ptr<EnumTypeDef> findEnumTypeDef(const EnumDef &) const;
        
        bool isNull(ObjectPtr) const;

        // Execute specific lambda for all memo types (language specific wrappers)
        // available within the current process's context
        void forAllMemoTypes(std::function<void(TypeObjectPtr)>) const;
        
        // get special MemoBase type
        TypeObjectSharedPtr tryGetMemoBaseType() const noexcept;
        TypeObjectSharedPtr getMemoBaseType() const;
        
        bool isMemoBase(TypeObjectPtr) const;

        bool isdbzeroType(ObjectPtr) const;

        bool isdbzeroTypeId(TypeId type_id) const;

        bool isSimplePyType(ObjectPtr) const;

        bool isSimplePyTypeId(TypeId type_id) const;
        
        // Decode either of: None, False or True from a lo-fi code
        ObjectSharedPtr getLangConstant(unsigned int) const;
        
        void close(db0::ProcessTimer *timer_ptr = nullptr);
        
    private:
        static std::vector<std::unique_ptr<std::string> > m_string_pool;
        // the registry of memo types, used for retrieving type decorators
        std::unordered_map<TypeObjectPtr, MemoTypeDecoration> m_type_registry;
        std::unordered_map<TypeId, ObjectSharedPtr> m_py_type_map;
        std::unordered_map<ObjectPtr, TypeId> m_id_map;
        // lang types by name variant
        // note that this cache may contain types not present in the ClassFactory yet
        std::unordered_map<std::string, TypeObjectSharedPtr> m_type_cache;        
        // lang enums by name variant
        std::unordered_map<std::string, ObjectSharedPtr> m_enum_cache;
        mutable ObjectSharedPtr m_py_bad_prefix_error;
        // error associated with missing / invalid type accessed (e.g. missing import)
        mutable ObjectSharedPtr m_py_class_not_found_error;
        // invalid reference error - e.g. UUID or weak proxy expired
        mutable ObjectSharedPtr m_py_reference_error;
        // identified reference to a MemoBase type
        TypeObjectPtr m_memo_base_type = nullptr;
        std::unordered_set<TypeId> m_dbzero_type_ids;
        std::unordered_set<TypeId> m_simple_py_type_ids;

        // Register a mapping from static type
        template <typename T> void addStaticType(T py_type, TypeId py_type_id);
        template <typename T> void addStaticdbzeroType(T py_type, TypeId py_type_id);
        template <typename T> void addStaticSimpleType(T py_type, TypeId py_type_id);
    };
    
    template <typename T> void PyTypeManager::addStaticType(T py_type, TypeId py_type_id)
    {  
        m_py_type_map[py_type_id] = reinterpret_cast<ObjectPtr>(py_type);
        m_id_map[reinterpret_cast<ObjectPtr>(py_type)] = py_type_id;
    }
    
    template <typename T> void PyTypeManager::addStaticdbzeroType(T py_type, TypeId py_type_id)
    {
        addStaticType(py_type, py_type_id);
        m_dbzero_type_ids.insert(py_type_id);        
    }
    
    template <typename T> void PyTypeManager::addStaticSimpleType(T py_type, TypeId py_type_id)
    {
        addStaticType(py_type, py_type_id);
        m_simple_py_type_ids.insert(py_type_id);
    }
    
    extern template const db0::object_model::Object &
    PyTypeManager::extractObject<MemoObject>(ObjectPtr) const;

    extern template const db0::object_model::ObjectImmutableImpl &
    PyTypeManager::extractObject<MemoImmutableObject>(ObjectPtr) const;

    extern template db0::object_model::Object &
    PyTypeManager::extractMutableObject<MemoObject>(ObjectPtr) const;

    extern template db0::object_model::ObjectImmutableImpl *
    PyTypeManager::tryExtractMutableObject<MemoImmutableObject>(ObjectPtr) const;

    extern template db0::object_model::Object *
    PyTypeManager::tryExtractMutableObject<MemoObject>(ObjectPtr) const;

}