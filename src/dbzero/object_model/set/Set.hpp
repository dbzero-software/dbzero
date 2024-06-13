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

    struct [[gnu::packed]] TypedItem_Ptr
    {
        std::uint64_t m_add;
    };

    union [[gnu::packed]] TypedItem_Address
    {
        TypedItem_Ptr as_ptr;
        o_typed_item as_value;
        
        TypedItem_Address(){
            as_ptr.m_add = 0;
        };
        TypedItem_Address(std::uint64_t addrs){
            as_ptr.m_add = addrs;
        }

        operator std::uint64_t() const{
            return as_ptr.m_add;
        }
        operator bool () const{
            return as_ptr.m_add != 0;
        }
        
        // binary compare
        bool operator!=(const TypedItem_Address &) const;
    };

    using Fixture = db0::Fixture;



    class SetIndex : public MorphingBIndex<o_typed_item, TypedItem_Address>{
        using super_t = MorphingBIndex<o_typed_item, TypedItem_Address>;
    public:
        SetIndex(Memspace &memspace)
            : MorphingBIndex<o_typed_item, TypedItem_Address>(memspace)
        {
        }
        
        SetIndex(Memspace &memspace, const o_typed_item & value)
            : MorphingBIndex<o_typed_item, TypedItem_Address>(memspace, value)
        {
            std::cerr << "TYPED ITEM " << value.m_storage_class << " " << value.m_value.m_store << std::endl;
        }
        
        SetIndex(Memspace& memspace, std::uint64_t addr, bindex::type type)
            : MorphingBIndex<o_typed_item, TypedItem_Address>(memspace, TypedItem_Address(addr), type)
        {
        }
    };

    struct [[gnu::packed]] TypedIndex
    {
        TypedItem_Address m_index_addres;
        bindex::type m_type;
        TypedIndex()
            : m_index_addres(0), m_type(bindex::empty)
        {
        }


        TypedIndex(TypedItem_Address index_addres, bindex::type type)
            : m_index_addres(index_addres), m_type(type)
        {
        }

        SetIndex getIndex(Memspace & memspace){
            return SetIndex(memspace, m_index_addres, m_type);
        }
        
    };

    using set_item = db0::key_value<std::uint64_t, TypedIndex>;

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
        ObjectSharedPtr getItem(std::size_t i, ObjectPtr key_value) const;
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        static Set *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static Set *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        Set *copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture);

        // drop underlying DBZero representation
        void drop();
        Set::ObjectSharedPtr pop();
        bool has_item(PyObject * obj);

        std::size_t size() const { return m_size; }
        void clear() { v_bindex::clear(); m_size = 0; }
        void insert(const Set &set);
    private:
        // new sets can only be created via factory members
        Set(db0::swine_ptr<Fixture> &);
        Set(db0::swine_ptr<Fixture> &, std::uint64_t address);
        Set(db0::swine_ptr<Fixture> &fixture, Set& set);
        std::size_t m_size = 0;
    };
    
}