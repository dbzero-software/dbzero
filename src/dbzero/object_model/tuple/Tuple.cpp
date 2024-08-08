#include "Tuple.hpp"
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model

{
    
    template <typename LangToolkit> o_typed_item createTupleItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, lang_value) };
    }
    
    GC0_Define(Tuple)
    Tuple::Tuple(std::size_t size, db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture, size)
    {
    }

    Tuple::Tuple(db0::swine_ptr<Fixture> &fixture, const Tuple & other)
        : super_t(fixture, other.size())
    {
        for (std::size_t i = 0; i < other.size(); i++) {
            auto [storage_class, value] = getData()->items()[i];
            modify().items()[i] = { storage_class, value };
        }
    }

    Tuple::Tuple(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }
    
    Tuple::ObjectSharedPtr Tuple::getItem(std::size_t i) const
    {
        if (i >= getData()->size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        auto [storage_class, value] = getData()->items()[i];
        auto fixture = this->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value);
    }
    
    void Tuple::setItem(FixtureLock &fixture, std::size_t i, ObjectPtr lang_value)
    {
        // FIXME: is this method allowed ? (since tuples are immutable in Python)
        if (i >= getData()->size()) {
            THROWF(db0::InputException) << "Index out of range: " << i;
        }
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        modify().items()[i] = createTupleItem<LangToolkit>(*fixture, type_id, lang_value, storage_class);
    }
    
    Tuple *Tuple::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::size_t size)
    {
        return new (at_ptr) Tuple(size, fixture);
    }
    
    Tuple *Tuple::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        return new (at_ptr) Tuple(fixture, address);
    }

    std::size_t Tuple::count(ObjectPtr lang_value) const
    {
        std::size_t count = 0;
        auto fixture = this->getFixture();
        for (auto &elem: this->const_ref().items()) {
            auto [elem_storage_class, elem_value] = elem;
            if (unloadMember<LangToolkit>(fixture, elem_storage_class, elem_value) == lang_value) {
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
            if (unloadMember<LangToolkit>(fixture, elem_storage_class, elem_value) == lang_value) {
                return index;
            }
            index += 1;
        }
        THROWF(db0::InputException) << "Item is not in a list ";
        return -1;
    }

    std::size_t Tuple::size() const
    {
        return getData()->size();
    }

    bool Tuple::operator==(const Tuple &tuple) const {
        return false;
    }

    void Tuple::operator=(Tuple &&tuple) {
        super_t::operator=(std::move(tuple));
        assert(!tuple.hasInstance());
    }

    bool Tuple::operator!=(const Tuple &tuple) const {
        return false;
    }
    
    void Tuple::drop() {
        v_object<o_tuple>::destroy();
    }

    const o_typed_item *Tuple::begin() const {
        return this->getData()->items().begin();
    }

    const o_typed_item *Tuple::end() const {
        return this->getData()->items().end();
    }

    void Tuple::moveTo(db0::swine_ptr<Fixture> &fixture) {
        assert(hasInstance());
        super_t::moveTo(fixture);
    }
    
}
