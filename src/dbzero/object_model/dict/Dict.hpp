#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp> 
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/object_model/item/Pair.hpp>

namespace db0 {

    class Fixture;

}

namespace db0::object_model

{

    using Fixture = db0::Fixture;
    // using dict_item = db0::key_value<std::uint64_t, o_pair_item>;
    using PairItem_Address = ValueT_Address<o_pair_item>;
    using DictIndex = CollectionIndex<o_pair_item, PairItem_Address>;
    using dict_item = db0::key_value<std::uint64_t, TypedIndexAddr<PairItem_Address, DictIndex>>;
    
    struct [[gnu::packed]] o_dict: public db0::o_fixed<o_dict>
    {
        // common object header
        o_unique_header m_header;
        std::uint64_t m_index_ptr = 0;
        std::uint64_t m_size = 0;
        std::uint64_t m_reserved[2] = {0, 0};
    };

    class Dict: public db0::ObjectBase<Dict, db0::v_object<o_dict>, StorageClass::DB0_DICT>
    {
        GC0_Declare

    public:
        using super_t = db0::ObjectBase<Dict, db0::v_object<o_dict>, StorageClass::DB0_DICT>;
        friend class db0::ObjectBase<Dict, db0::v_object<o_dict>, StorageClass::DB0_DICT>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using const_iterator = typename db0::v_bindex<dict_item>::const_iterator;
        
        Dict(db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        void operator=(Dict &&);

        ObjectSharedPtr getItem(std::size_t hash, ObjectPtr key_value) const;
        void setItem(std::size_t hash, ObjectPtr key, ObjectPtr value);
        
        static Dict *makeNew(void *at_ptr, db0::swine_ptr<Fixture> &);
        static Dict *unload(void *at_ptr, db0::swine_ptr<Fixture> &, std::uint64_t address);
        
        bool has_item(PyObject * obj) const;

        Dict *copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const;

        ObjectSharedPtr pop(ObjectPtr obj);

        void moveTo(db0::swine_ptr<Fixture> &);

        std::size_t size() const;

        void clear();

        void commit() const;

        void detach() const;

        const_iterator begin() const;
        const_iterator end() const;
        
        void unrefMembers() const;

        void destroy() const;

    private:
        db0::v_bindex<dict_item> m_index;
        
        // new dicts can only be created via factory members
        explicit Dict(db0::swine_ptr<Fixture> &);
        explicit Dict(db0::swine_ptr<Fixture> &fixture, const Dict &);
        explicit Dict(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const Dict &);

        void initWith(const Dict &);
    };
    
}