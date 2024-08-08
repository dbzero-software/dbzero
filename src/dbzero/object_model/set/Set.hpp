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
    using TypedItem_Address = ValueT_Address<o_typed_item>;
    using SetIndex = CollectionIndex<o_typed_item, TypedItem_Address>;
    using set_item = db0::key_value<std::uint64_t, TypedIndexAddr<TypedItem_Address, SetIndex>>;
    
    struct [[gnu::packed]] o_set: public db0::o_fixed<o_set>
    {
        // common object header
        o_object_header m_header;
        std::uint64_t m_index_ptr = 0;
        std::uint64_t m_size = 0;
        std::uint64_t m_reserved[2] = {0, 0};
    };

    class Set: public db0::ObjectBase<Set, db0::v_object<o_set>, StorageClass::DB0_SET>
    {
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<Set, db0::v_object<o_set>, StorageClass::DB0_SET>;
        friend class db0::ObjectBase<Set, db0::v_object<o_set>, StorageClass::DB0_SET>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using const_iterator = typename db0::v_bindex<set_item>::const_iterator;
        
        Set(db0::swine_ptr<Fixture> &, std::uint64_t address);

        void operator=(Set &&);

        void append(FixtureLock &, std::size_t key, ObjectPtr lang_value);
        bool remove(FixtureLock &, std::size_t key, ObjectPtr key_value);
        ObjectSharedPtr getItem(std::size_t i, ObjectPtr key_value) const;
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        static Set *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static Set *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        // drop underlying DBZero representation
        void destroy();
        Set::ObjectSharedPtr pop();
        bool has_item(PyObject * obj) const;
        
        void clear();
        void insert(const Set &set);
        void moveTo(db0::swine_ptr<Fixture> &);

        std::size_t size() const;

        void commit() const;

        void detach() const;

        const_iterator begin() const;
        const_iterator end() const;
        
    private:
        db0::v_bindex<set_item> m_index;

        // new sets can only be created via factory members
        Set(db0::swine_ptr<Fixture> &);
        Set(db0::swine_ptr<Fixture> &, const Set& dict);      
        
        void append(db0::swine_ptr<Fixture> &, std::size_t key, ObjectPtr lang_value);
    };
    
}