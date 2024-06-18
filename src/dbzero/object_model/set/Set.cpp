#include "Set.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <object.h>

namespace db0::object_model

{

    GC0_Define(Set)

    template <typename LangToolkit> o_typed_item createTypedItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, lang_value) };
    }
    
    template <typename LangToolkit> set_item createSetItem(Memspace & memspace, db0::swine_ptr<Fixture> &fixture, std::uint64_t key, 
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {
        
        auto item = createTypedItem<LangToolkit>(fixture, type_id, lang_value, storage_class);
        SetIndex bindex(memspace, item);
        auto address = bindex.getAddress();
        auto typed_index = TypedIndex(address, bindex::itty);
        return { key, typed_index };
    }

    void Set::insert(const Set &set){
        for (auto [key, address] : set) {
            auto fixture = this->getMutableFixture();
            auto memspace = this->getMemspace();
            auto bindex = address.getIndex(memspace);
            auto it = bindex.beginJoin(1);
            while(!it.is_end()) {
                auto [storage_class, value] = (*it);
                auto member = unloadMember<LangToolkit>(*fixture, storage_class, value);
                append(fixture, key, member.get());
                ++it;
            }
        }
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
        insert(set);
    }
    
    void Set::append(FixtureLock &fixture, std::size_t key, ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        auto iter = v_bindex::find(key);
                    // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        if (iter == end()) {
            m_size++;

            auto set_it= createSetItem<LangToolkit>(this->getMemspace(),*fixture, key, type_id, lang_value, storage_class);
            v_bindex<set_item>::insert(set_it);

        } else {
            auto item = getItem(key, lang_value);
            if(item == nullptr) {
                m_size++;
                auto [key, address] = *iter;
                auto memspace = this->getMemspace();
                auto bindex = address.getIndex(memspace);
                auto item = createTypedItem<LangToolkit>(*fixture, type_id, lang_value, storage_class);
                bindex.insert(item);
                auto new_address = bindex.getAddress();
                if(new_address != address.m_index_address){
                    auto new_typed_index = TypedIndex(new_address, bindex.getIndexType());
                    v_bindex::erase(iter);
                    v_bindex::insert({key, new_typed_index});
                }
            }
        }
    }
    
    bool Set::remove(FixtureLock &, std::size_t key)
    {
        auto it = v_bindex::find(key);
        if (it == end()) {
            return false;
        }
        v_bindex::erase(it);
        m_size -= 1;
        return true;
    }

    Set::ObjectSharedPtr Set::getItem(std::size_t key, ObjectPtr key_value) const
    {
        auto iter = find(key);
        if (iter != end() ){
            auto [key, address] = *iter;
            auto memspace = this->getMemspace();
            auto bindex = address.getIndex(memspace);
            auto it = bindex.beginJoin(1);
            auto fixture = this->getFixture(); 
            auto type_id = LangToolkit::getTypeManager().getTypeId(key_value);
            while(!it.is_end()){
                auto [storage_class, value] = *it;
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
                if (LangToolkit::compare(key_value, member.get())) {
                    return member;
                }
                ++it;
            }
        }
        return nullptr;
    }
    
    void Set::setItem(FixtureLock &fixture, std::size_t key, ObjectPtr lang_value)
    {
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        v_bindex<set_item>::insert(createSetItem<LangToolkit>(this->getMemspace(), *fixture, key, type_id, lang_value, storage_class));
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
            auto [key, address] = *iter;
            auto bindex = address.getIndex(this->getMemspace());
            auto it = bindex.beginJoin(1);
            auto [storage_class, value] = *it;
            auto fixture = this->getFixture();
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
            if(bindex.size() == 1) {
                v_bindex::erase(iter);
                bindex.destroy();
            } else {
                bindex.erase(*it);
            }
            m_size -= 1;
            return member;
        } else {
            return nullptr;
        }
    }

    bool Set::has_item(ObjectPtr obj) 
    {
        auto hash = PyObject_Hash(obj);
        auto item = getItem(hash, obj);
        return item != nullptr;
    }

    void Set::moveTo(db0::swine_ptr<Fixture> &) {
        throw std::runtime_error("Not implemented");
    }
    
}
