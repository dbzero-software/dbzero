#include "Set.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>


namespace db0::object_model

{

    GC0_Define(Set)

    template <typename LangToolkit> o_typed_item createTypedItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, lang_value) };
    }
    
    template <typename LangToolkit> set_item createSetItem(db0::swine_ptr<Fixture> &fixture, std::uint64_t key, 
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { key, createTypedItem<LangToolkit>(fixture, type_id, lang_value, storage_class) };
    }
    
    Set::Set(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
    {
    }
    
    Set::Set(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }
    
    Set::Set(db0::swine_ptr<Fixture> &fixture, Set& set)
        : super_t(fixture)
    {
        bulkInsert(set.begin(), set.end());
    }
    
    void Set::append(FixtureLock &fixture, std::size_t key, ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        if (v_bindex::find(key) == end()) {
            // recognize type ID from language specific object
            auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
            auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
            v_bindex::insert(createSetItem<LangToolkit>(*fixture, key, type_id, lang_value, storage_class));
        }
    }
    
    bool Set::remove(FixtureLock &, std::size_t key)
    {
        auto it = v_bindex::find(key);
        if (it == end()) {
            return false;
        }
        v_bindex::erase(it);
        return true;
    }

    Set::ObjectSharedPtr Set::getItem(std::size_t key) const
    {
        auto iter = find(key);
        if (iter != end() ){
            auto [key, item] = *iter;
            auto [storage_class, value] = item;
            auto fixture = this->getFixture();
            return unloadMember<LangToolkit>(fixture, storage_class, value);
        }
        THROWF(db0::InputException) << "Item not found";
        return nullptr;
    }
    
    void Set::setItem(FixtureLock &fixture, std::size_t key, ObjectPtr lang_value)
    {
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        v_bindex<set_item>::insert(createSetItem<LangToolkit>(*fixture, key, type_id, lang_value, storage_class));
    }
    
    Set *Set::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Set(fixture);
    }
    
    Set *Set::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Set(fixture, address);
    }

    Set * Set::copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Set(fixture, *this);
    }

    void Set::drop() {
        v_bindex<set_item>::destroy();
    }

    Set::ObjectSharedPtr Set::pop()
    {
        auto iter = begin();
        if (iter != end()) {
            auto [key, item] = *iter;
            auto [storage_class, value] = item;
            auto fixture = this->getFixture();
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
            v_bindex::erase(iter);
            return member;
        } else {
            return nullptr;
        }
    }

    bool Set::has_item(ObjectPtr obj) 
    {
        auto hash = PyObject_Hash(obj);
        auto iter = find(hash);
        return iter != end();
    }

}
