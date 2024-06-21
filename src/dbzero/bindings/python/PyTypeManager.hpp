#pragma once

#include <unordered_map>
#include <list>
#include <vector>
#include <dbzero/bindings/TypeId.hpp>
#include "PyTypes.hpp"
#include "PyEnumType.hpp"

namespace db0::object_model {

    class Object;
    class List;
    class Set;
    class Tuple;
    class Dict;
    class TagSet;
    class Index;
    class ObjectIterator;
    struct EnumValue;
    struct FieldDef;
    
}

namespace db0::object_model::pandas {
    class Block;
}

namespace db0::python

{
    
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
        using Object = db0::object_model::Object;
        using List = db0::object_model::List;
        using Set = db0::object_model::Set;
        using Tuple = db0::object_model::Tuple;
        using Dict = db0::object_model::Dict;
        using TagSet = db0::object_model::TagSet;
        using PandasBlock = db0::object_model::pandas::Block;
        using Index = db0::object_model::Index;
        using ObjectIterator = db0::object_model::ObjectIterator;
        using EnumValue = db0::object_model::EnumValue;
        using FieldDef = db0::object_model::FieldDef;

        PyTypeManager();
        
        /**
         * Add string to pool and return a managed pointer
        */
        const char *getPooledString(std::string);
        const char *getPooledString(const char *);
        
        // Recognize Python type of a specific object instance as TypeId (may return TypeId::UNKNOWN)
        TypeId getTypeId(ObjectPtr object_instance) const;

        TypeId getTypeId(TypeObjectPtr py_type) const;
        
        /**
         * Extracts reference to DB0 object from a memo object
        */
        const Object &extractObject(ObjectPtr memo_ptr) const;
        Object &extractMutableObject(ObjectPtr memo_ptr) const;
        const Object *tryExtractObject(ObjectPtr memo_ptr) const;
        Object *tryExtractMutableObject(ObjectPtr memo_ptr) const;
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
        EnumValue extractEnumValue(ObjectPtr enum_value_ptr) const;
        ObjectIterator &extractObjectIterator(ObjectPtr) const;
        FieldDef &extractFieldDef(ObjectPtr) const;
        TypeObjectPtr getTypeObject(ObjectPtr py_type) const;
        
        ObjectPtr getBadPrefixError() const;
        
        /**
         * Extracts reference to DB0 Block
        */
        const PandasBlock &extractBlock(ObjectPtr memo_ptr) const;
        PandasBlock &extractMutableBlock(ObjectPtr memo_ptr) const;

        /**
         * Called with each new memo type
        */
        void addMemoType(TypeObjectPtr, const char *type_id);

        // Called to register each newly created db0.enum type
        void addEnum(PyEnum *);

        /**
         * Try finding Python type by a given name variant
        */
        TypeObjectPtr findType(const std::string &variant_name) const;

        bool isNull(ObjectPtr) const;

        void close();
        
    private:
        std::vector<std::string> m_string_pool;
        std::unordered_map<TypeId, ObjectPtr> m_py_type_map;
        std::unordered_map<ObjectPtr, TypeId> m_id_map;
        // lang types by name variant
        std::unordered_map<std::string, TypeObjectSharedPtr> m_type_cache;
        std::vector<ObjectSharedPtr> m_enum_cache;
        mutable ObjectSharedPtr m_py_bad_prefix_error;
        
        // Register a mapping from static type
        template <typename T> void addStaticType(T py_type, TypeId py_type_id);
    };
    
    template <typename T> void PyTypeManager::addStaticType(T py_type, TypeId py_type_id)
    {  
        m_py_type_map[py_type_id] = reinterpret_cast<ObjectPtr>(py_type);
        m_id_map[reinterpret_cast<ObjectPtr>(py_type)] = py_type_id;
    }
       
}