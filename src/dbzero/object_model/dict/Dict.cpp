
#include "Dict.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>


namespace db0::object_model

{

    GC0_Define(Dict)

    template <typename LangToolkit> o_typed_item createTypedItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, lang_value) };
    }

    template <typename LangToolkit> dict_item createDictItem(const Dict &dict, std::uint64_t hash, 
        o_typed_item key, o_typed_item value)
    {
        return { hash, o_pair_item(key, value) };
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
    {
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
    {
    }

    Dict::Dict(db0::swine_ptr<Fixture> &fixture, const Dict& dict)
        : super_t(fixture)
    {
        bulkInsert(dict.begin(), dict.end());
    }
    
    void Dict::setItem(std::size_t hash, ObjectPtr key, ObjectPtr value)
    {
        using TypeId = db0::bindings::TypeId;
        auto type_id = LangToolkit::getTypeManager().getTypeId(value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        auto fixture = this->getFixture();
        db0::FixtureLock lock(fixture);
        auto value_item = createTypedItem<LangToolkit>(*lock, type_id, value, storage_class);
        auto it = v_bindex::find(hash);
        if (it == end()) {
            // recognize type ID from language specific object
            auto type_id = LangToolkit::getTypeManager().getTypeId(key);
            auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
            auto key_item = createTypedItem<LangToolkit>(*lock, type_id, key, storage_class);
            v_bindex::insert(createDictItem<LangToolkit>(*lock, hash, key_item, value_item));
        } else {
            it.modifyItem().value.m_second = value_item;
        }           
    }

    Dict::ObjectSharedPtr Dict::getItem(std::size_t key) const
    {
        auto iter = find(key);
        if (iter != end()) {
            auto [key, item] = *iter;
            auto [storage_class, value] = item.m_second;
            auto fixture = this->getFixture();
            return unloadMember<LangToolkit>(fixture, storage_class, value);
        }
        THROWF(db0::InputException) << "Item not found" << THROWF_END;        
    }
    
    Dict *Dict::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Dict(fixture);
    }
    
    Dict *Dict::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Dict(fixture, address);
    }

    bool Dict::has_item(ObjectPtr obj) const 
    {
        auto hash = PyObject_Hash(obj);
        auto iter = find(hash);
        return iter != end();
    }

    Dict *Dict::copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const {
        return new (at_ptr) Dict(fixture, *this);
    }

    Dict::ObjectSharedPtr Dict::pop(ObjectPtr obj)
    {
        auto hash = PyObject_Hash(obj);
        auto iter = find(hash);
        if (iter != end()) {
            auto [key, item] = *iter;
            auto [storage_class, value] = item.m_second;
            auto fixture = this->getFixture();
            db0::FixtureLock lock(fixture);
            auto member = unloadMember<LangToolkit>(*lock, storage_class, value);
            v_bindex::erase(iter);
            return member;
        } else {
            return nullptr;
        }
    }

    void Dict::moveTo(db0::swine_ptr<Fixture> &) {
        throw std::runtime_error("Not implemented");
    }

}
