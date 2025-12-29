// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp> 
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/object_model/item/Item.hpp>
#include <dbzero/core/utils/weak_vector.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <array>

namespace db0 {

    class Fixture;

}

namespace db0::object_model

{

    using Fixture = db0::Fixture;
    using TypedItem_Address = ValueT_Address<o_typed_item>;
    using SetIndex = CollectionIndex<o_typed_item, TypedItem_Address>;
    using set_item = db0::key_value<std::uint64_t, TypedIndexAddr<TypedItem_Address, SetIndex>>;
    class SetIterator;
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_set: public db0::o_fixed_versioned<o_set>
    {
        // common object header
        o_unique_header m_header;
        Address m_index_ptr = {};
        std::uint64_t m_size = 0;
        std::array<std::uint64_t, 2> m_reserved = {0, 0};

        bool hasRefs() const {
            return m_header.hasRefs();
        }
    };
DB0_PACKED_END    
    
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
        
        // as null placeholder
        Set() = default;
        explicit Set(db0::swine_ptr<Fixture> &, AccessFlags = {});
        explicit Set(tag_no_gc, db0::swine_ptr<Fixture> &, const Set &);
        explicit Set(db0::swine_ptr<Fixture> &, Address, AccessFlags = {});
        ~Set();
        
        void operator=(Set &&);
        
        void append(FixtureLock &, std::size_t key, ObjectSharedPtr lang_value);
        bool remove(FixtureLock &, std::size_t key, ObjectPtr key_value);
        ObjectSharedPtr getItem(std::size_t i, ObjectPtr key_value) const;
        
        Set::ObjectSharedPtr pop(FixtureLock &);
        bool hasItem(std::int64_t hash, ObjectPtr lang_key) const;
        
        void clear(FixtureLock &);
        void insert(const Set &set);
        void moveTo(db0::swine_ptr<Fixture> &);

        std::size_t size() const;
        
        void commit() const;
        void detach() const;

        // drop underlying dbzero representation
        void destroy();
        
        const_iterator begin() const;
        const_iterator end() const;
        
        void unrefMembers() const;
        
        std::shared_ptr<SetIterator> getIterator(ObjectPtr lang_set) const;

    protected:
        friend class SetIterator;
        const_iterator find(std::uint64_t key_hash) const;
        
    private:
        db0::v_bindex<set_item> m_index;
        mutable db0::weak_vector<SetIterator> m_iterators;
                
        void append(db0::swine_ptr<Fixture> &, std::size_t key, ObjectPtr lang_value);

        void restoreIterators();
    };
    
}