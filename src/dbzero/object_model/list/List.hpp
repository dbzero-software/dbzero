#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/object_model/item/Item.hpp>    
#include <dbzero/workspace/GC0.hpp>
    
namespace db0
{

    class Fixture;

}

namespace db0::object_model

{

    using Fixture = db0::Fixture;
    void dropList(void *vptr);
    
    class List: public db0::ObjectBase<List, v_bvector<o_typed_item>, StorageClass::DB0_LIST>
    {
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<List, v_bvector<o_typed_item>, StorageClass::DB0_LIST>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using const_iterator = typename v_bvector<o_typed_item>::const_iterator;
        
        List(db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        void append(FixtureLock &, ObjectPtr lang_value);
        ObjectSharedPtr getItem(std::size_t i) const;
        ObjectSharedPtr pop(FixtureLock &, std::size_t index);
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        static List *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static List *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        List * copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture);
        size_t count(ObjectPtr lang_value);
        size_t index(ObjectPtr lang_value);

        // operators
        bool operator==(const List &) const;
        bool operator!=(const List &) const;

        // drop underlying DBZero representation
        void drop();

        void clear(FixtureLock &);

        void swapAndPop(FixtureLock &, const std::vector<uint64_t> &element_numbers);
        
    private:        
        // new lists can only be created via factory members
        List(db0::swine_ptr<Fixture> &);        
        List(db0::swine_ptr<Fixture> &, List& list);        
    };
    
}