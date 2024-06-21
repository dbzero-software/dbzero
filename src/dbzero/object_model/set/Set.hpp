#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp> 
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/object_model/item/Item.hpp>

namespace db0 {

    class Fixture;

}

namespace db0::object_model

{

    using Fixture = db0::Fixture;
    using set_item = db0::key_value<std::uint64_t, o_typed_item>;
    
    class Set: public db0::ObjectBase<Set, v_bindex<set_item>, StorageClass::DB0_SET>
    {
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<Set, v_bindex<set_item>, StorageClass::DB0_SET>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        
        void append(FixtureLock &, std::size_t key, ObjectPtr lang_value);
        bool remove(FixtureLock &, std::size_t key);
        ObjectSharedPtr getItem(std::size_t i) const;
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        static Set *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static Set *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        Set *copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const;

        // drop underlying DBZero representation
        void drop();
        Set::ObjectSharedPtr pop();
        bool has_item(PyObject * obj) const;

        void moveTo(db0::swine_ptr<Fixture> &);

    private:
        // new sets can only be created via factory members
        Set(db0::swine_ptr<Fixture> &);
        Set(db0::swine_ptr<Fixture> &, std::uint64_t address);
        Set(db0::swine_ptr<Fixture> &fixture, const Set &);
    };
    
}