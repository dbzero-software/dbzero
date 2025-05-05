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
        friend super_t;
        
        List(db0::swine_ptr<Fixture> &, Address);
        ~List();

        void append(FixtureLock &, ObjectPtr lang_value);
        ObjectSharedPtr getItem(std::size_t i) const;
        ObjectSharedPtr pop(FixtureLock &, std::size_t index);
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        static List *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static List *unload(void *at_ptr, db0::swine_ptr<Fixture> &, Address);
        List * copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const;
        size_t count(ObjectPtr lang_value) const;
        size_t index(ObjectPtr lang_value) const;

        // operators
        bool operator==(const List &) const;
        bool operator!=(const List &) const;
        
        void clear(FixtureLock &);

        void swapAndPop(FixtureLock &, const std::vector<uint64_t> &element_numbers);

        void moveTo(db0::swine_ptr<Fixture> &);

        void destroy() const;

        void clearMembers() const;
        
    private:        
        // new lists can only be created via factory members
        List(db0::swine_ptr<Fixture> &);
        List(db0::swine_ptr<Fixture> &, const List &);
        List(tag_no_gc, db0::swine_ptr<Fixture> &, const List &);
    };
    
}