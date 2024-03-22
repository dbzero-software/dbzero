#pragma once

#include <unordered_map>
#include <list>
#include <vector>
#include <dbzero/bindings/TypeId.hpp>
#include "PyTypes.hpp"

namespace db0::object_model {

    class Object;
    class List;
    class Set;
    class Tuple;
    class Dict;
    class TagSet;
    class Index;

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
        
        PyTypeManager();
        
        /**
         * Add string to pool and return a managed pointer
        */
        const char *getPooledString(const std::string &);

        // Recognize Python type of a specific object instance as TypeId (may return TypeId::UNKNOWN)
        TypeId getTypeId(ObjectPtr object_instance) const;
        
        /**
         * Extracts reference to DB0 object from a memo object
        */
        Object &extractObject(ObjectPtr memo_ptr) const;
        List &extractList(ObjectPtr list_ptr) const;
        Set &extractSet(ObjectPtr set_ptr) const;
        std::int64_t extractInt64(ObjectPtr int_ptr) const;        
        Tuple &extractTuple(ObjectPtr tuple_ptr) const;
        Dict &extractDict(ObjectPtr dict_ptr) const;
        TagSet &extractTagSet(ObjectPtr tag_set_ptr) const;
        Index &extractIndex(ObjectPtr index_ptr) const;
        
        TypeObjectPtr getTypeObject(ObjectPtr py_type) const;
        
        /**
         * Extracts reference to DB0 Block
        */
        PandasBlock &extractBlock(ObjectPtr memo_ptr) const;

        /**
         * Called with each new memo type
        */
        void addMemoType(TypeObjectPtr, const char *type_id);

        /**
         * Try finding Python type by a given name variant
        */
        TypeObjectPtr findType(const std::string &variant_name) const;
        
    private:
        std::vector<std::string> m_string_pool;
        std::unordered_map<TypeId, ObjectPtr> m_py_type_map;
        std::unordered_map<ObjectPtr, TypeId> m_id_map;        
        // lang types by name variant
        std::unordered_map<std::string, TypeObjectSharedPtr> m_type_cache;
        
        // Register a mapping from static type
        template <typename T> void addStaticType(T py_type, TypeId py_type_id);
    };
    
    template <typename T> void PyTypeManager::addStaticType(T py_type, TypeId py_type_id)
    {  
        m_py_type_map[py_type_id] = reinterpret_cast<ObjectPtr>(py_type);
        m_id_map[reinterpret_cast<ObjectPtr>(py_type)] = py_type_id;
    }
       
}