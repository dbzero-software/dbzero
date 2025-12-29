// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Tuple.hpp"
#include "TupleIterator.hpp"
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model

{
    
    GC0_Define(Tuple)

    o_tuple::o_tuple(std::size_t size)
    {
        arrangeMembers()
            (o_micro_array<o_typed_item>::type(), size).ptr();
    }

    std::size_t o_tuple::size() const {
        return items().size();
    }

    std::size_t o_tuple::sizeOf() const
    {
        return sizeOfMembers()
            (o_micro_array<o_typed_item>::type());
    }

    std::size_t o_tuple::measure(std::size_t size)
    {
        return measureMembers()
            (o_micro_array<o_typed_item>::measure(size));
    }
    
    template <typename LangToolkit> o_typed_item createTupleItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, 
        StorageClass storage_class, FlagSet<AccessOptions> access_mode)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, storage_class, lang_value, access_mode) };
    }
    
    Tuple::Tuple(db0::swine_ptr<Fixture> &fixture, tag_new_tuple, std::size_t size, AccessFlags access_mode)
        : super_t(fixture, size, access_mode)
    {
    }

    Tuple::Tuple(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const Tuple &other)
        : super_t(tag_no_gc(), fixture, other.size())
    {
        for (std::size_t i = 0; i < other.size(); i++) {
            auto [storage_class, value] = getData()->items()[i];
            modify().items()[i] = { storage_class, value };
        }
    }
    
    Tuple::Tuple(db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode)
        : super_t(super_t::tag_from_address(), fixture, address, access_mode)
    {
    }
    
    Tuple::~Tuple()
    {
        // unregister needs to be called before destruction of members
        unregister();
    }
    
    Tuple::ObjectSharedPtr Tuple::getItem(std::size_t i) const
    {
        if (i >= getData()->size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto [storage_class, value] = getData()->items()[i];
        auto fixture = this->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value, 0, this->getMemberFlags());
    }
    
    void Tuple::setItem(FixtureLock &fixture, std::size_t i, ObjectSharedPtr lang_value)
    {
        // FIXME: is this method allowed ? (since tuples are immutable in Python)
        if (i >= getData()->size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(*lang_value);
        // NOTE: packed storage not supported for tuple items
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id, false);
        StorageClass storage_class;
        if (pre_storage_class == PreStorageClass::OBJECT_WEAK_REF) {
            storage_class = db0::getStorageClass(pre_storage_class, *fixture, *lang_value);
        } else {
            storage_class = db0::getStorageClass(pre_storage_class);
        }
        
        modify().items()[i] = createTupleItem<LangToolkit>(
            *fixture, type_id, *lang_value, storage_class, getMemberFlags()
        );
    }
    
    std::size_t Tuple::count(ObjectPtr lang_value) const
    {
        std::size_t count = 0;
        auto fixture = this->getFixture();
        for (auto &elem: this->const_ref().items()) {
            auto [elem_storage_class, elem_value] = elem;
            if (unloadMember<LangToolkit>(fixture, elem_storage_class, elem_value, 0, this->getMemberFlags()) == lang_value) {
                count += 1;
            }
        }
        return count;
    }
    
    std::size_t Tuple::index(ObjectPtr lang_value) const
    {
        std::size_t index = 0;
        auto fixture = this->getFixture();
        for (auto &elem: this->const_ref().items()){
            auto [elem_storage_class, elem_value] = elem;
            if (unloadMember<LangToolkit>(fixture, elem_storage_class, elem_value, 0, this->getMemberFlags()) == lang_value) {
                return index;
            }
            index += 1;
        }
        THROWF(db0::InputException) << "Item is not in a list ";
        return -1;
    }

    std::size_t Tuple::size() const {
        return getData()->size();
    }

    bool Tuple::operator==(const Tuple &tuple) const 
    {
        if (size() != tuple.size()) {
            return false;
        }
        return std::equal(begin(), end(), tuple.begin());
    }

    void Tuple::operator=(Tuple &&tuple) {
        super_t::operator=(std::move(tuple));
        assert(!tuple.hasInstance());
    }

    bool Tuple::operator!=(const Tuple &tuple) const {
        return !(*this == tuple);
    }
    
    void Tuple::destroy()
    {
        auto fixture = this->getFixture();
        for (auto &elem: this->getData()->items()) {
            auto [elem_storage_class, elem_value] = elem;
            unrefMember<LangToolkit>(fixture, elem_storage_class, elem_value);
        }
        super_t::destroy();
    }

    const o_typed_item *Tuple::begin() const {
        return this->getData()->items().begin();
    }

    const o_typed_item *Tuple::end() const {
        return this->getData()->items().end();
    }

    void Tuple::moveTo(db0::swine_ptr<Fixture> &fixture)
    {
        assert(hasInstance());
        super_t::moveTo(fixture);
    }
    
    std::shared_ptr<TupleIterator> Tuple::getIterator(ObjectPtr lang_tuple) const {
        return std::shared_ptr<TupleIterator>(new TupleIterator(begin(), this, lang_tuple));
    }

}
