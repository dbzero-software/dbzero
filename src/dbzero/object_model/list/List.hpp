// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <unordered_set>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/ObjectBase.hpp>
#include <dbzero/object_model/item/Item.hpp>    
#include <dbzero/workspace/GC0.hpp>
#include <dbzero/core/utils/weak_vector.hpp>
    
namespace db0

{

    class Fixture;

}

namespace db0::object_model

{

    class ListIterator;
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
        
        // as null placeholder
        List() = default;
        List(db0::swine_ptr<Fixture> &, AccessFlags = {});
        List(db0::swine_ptr<Fixture> &, const List &);
        List(tag_no_gc, db0::swine_ptr<Fixture> &, const List &);
        List(db0::swine_ptr<Fixture> &, Address, AccessFlags = {});
        ~List();
        
        void append(FixtureLock &, ObjectSharedPtr lang_value);
        ObjectSharedPtr getItem(std::size_t i) const;
        ObjectSharedPtr pop(FixtureLock &, std::size_t index);
        void setItem(FixtureLock &, std::size_t i, ObjectPtr lang_value);
        
        List * copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const;
        size_t count(ObjectPtr lang_value) const;
        size_t index(ObjectPtr lang_value) const;

        // operators
        bool operator==(const List &) const;
        bool operator!=(const List &) const;
        
        void clear(FixtureLock &);

        void swapAndPop(FixtureLock &, const std::vector<uint64_t> &element_numbers);

        void moveTo(db0::swine_ptr<Fixture> &);

        void destroy();

        void clearMembers() const;

        std::shared_ptr<ListIterator> getIterator(ObjectPtr lang_list) const;
        
        void detach() const;
        
    private:        
        // the associated iterator
        // which must be invalidated / refreshed on any collection modification
        mutable db0::weak_vector<ListIterator> m_iterators;

        // try restoring all associated iterators
        void restoreIterators();
    };
    
}