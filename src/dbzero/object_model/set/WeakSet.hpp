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
#include "Set.hpp"
#include <array>

namespace db0 {

    class Fixture;

}

namespace db0::object_model

{

    class WeakSetIterator;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_weak_set: public db0::o_fixed_versioned<o_weak_set>
    {
        o_unique_header m_header;
        Address m_index_ptr = {};
        std::uint64_t m_size = 0;
        std::array<std::uint64_t, 2> m_reserved = {0, 0};

        bool hasRefs() const {
            return m_header.hasRefs();
        }
    };
DB0_PACKED_END

    class WeakSet: public db0::ObjectBase<WeakSet, db0::v_object<o_weak_set>, StorageClass::DB0_WEAK_SET>
    {
        GC0_Declare
    public:
        using super_t = db0::ObjectBase<WeakSet, db0::v_object<o_weak_set>, StorageClass::DB0_WEAK_SET>;
        friend class db0::ObjectBase<WeakSet, db0::v_object<o_weak_set>, StorageClass::DB0_WEAK_SET>;
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using const_iterator = typename db0::v_bindex<set_item>::const_iterator;

        WeakSet() = default;
        explicit WeakSet(db0::swine_ptr<Fixture> &, AccessFlags = {});
        explicit WeakSet(tag_no_gc, db0::swine_ptr<Fixture> &, const WeakSet &);
        explicit WeakSet(db0::swine_ptr<Fixture> &, Address, AccessFlags = {});
        ~WeakSet();

        void operator=(WeakSet &&);

        // Insert a memo object as a weak reference. Hash is the target's hash.
        void append(FixtureLock &, std::size_t key, ObjectSharedPtr lang_value);
        bool remove(FixtureLock &, std::size_t key, ObjectPtr key_value);
        // contains by actual instance
        bool hasItem(std::int64_t hash, ObjectPtr lang_key) const;

        void clear(FixtureLock &);
        void insert(const WeakSet &);
        void moveTo(db0::swine_ptr<Fixture> &);

        // Reports only non-expired references (full scan).
        std::size_t size() const;

        void commit() const;
        void detach() const;

        void destroy();

        const_iterator begin() const;
        const_iterator end() const;

        void unrefMembers() const;

        std::shared_ptr<WeakSetIterator> getIterator(ObjectPtr lang_set) const;

    protected:
        friend class WeakSetIterator;
        const_iterator find(std::uint64_t key_hash) const;

    private:
        db0::v_bindex<set_item> m_index;
        mutable db0::weak_vector<WeakSetIterator> m_iterators;

        void append(db0::swine_ptr<Fixture> &, std::size_t key, ObjectPtr lang_value);

        void restoreIterators();
    };

}
