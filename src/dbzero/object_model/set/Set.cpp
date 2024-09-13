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
    
    template <typename LangToolkit> set_item createSetItem(db0::swine_ptr<Fixture> &fixture, std::uint64_t key, 
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {        
        auto item = createTypedItem<LangToolkit>(fixture, type_id, lang_value, storage_class);
        SetIndex bindex(*fixture, item);
        return { key, bindex };
    }
    
    Set::Set(db0::swine_ptr<Fixture> &fixture)
        : super_t(fixture)
        , m_index(*fixture)
    {
        modify().m_index_ptr = m_index.getAddress();
    }

    Set::Set(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const Set &set)
        : super_t(tag_no_gc(), fixture)
        , m_index(*fixture)
    {
        modify().m_index_ptr = m_index.getAddress();
        for(auto [hash, address] : set) {
            auto bindex = address.getIndex(this->getMemspace());
            auto bindex_copy = SetIndex(bindex);
            m_index.insert(set_item(hash, bindex_copy));
            
        }
        modify().m_size = set.size();
    }

    Set::Set(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_index(myPtr((*this)->m_index_ptr))
    {
    }

    void Set::operator=(Set && other)
    {
        unrefMembers();
        super_t::operator=(std::move(other));
        m_index = std::move(other.m_index);
        assert(!other.hasInstance());
    }

    void Set::insert(const Set &set)
    {
        for (auto [key, address] : set) {
            auto fixture = this->getFixture();
            auto bindex = address.getIndex(*fixture);
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = (*it);
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
                append(fixture, key, member.get());
                ++it;
            }
        }
    }

    void Set::append(db0::FixtureLock &lock, std::size_t key, ObjectPtr lang_value) {
        append(*lock, key, lang_value);
    }

    void Set::append(db0::swine_ptr<Fixture> &fixture, std::size_t key, ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        auto iter = m_index.find(key);
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        auto storage_class = TypeUtils::m_storage_class_mapper.getStorageClass(type_id);
        if (iter == m_index.end()) {
            ++modify().m_size;
            auto set_it = createSetItem<LangToolkit>(fixture, key, type_id, lang_value, storage_class);            
            m_index.insert(set_it);
        } else {
            auto item = getItem(key, lang_value);
            if (item == nullptr) {                
                ++modify().m_size;
                auto [key, address] = *iter;
                auto bindex = address.getIndex(*fixture);
                auto item = createTypedItem<LangToolkit>(fixture, type_id, lang_value, storage_class);
                bindex.insert(item);
                if (bindex.getAddress() != address.m_index_address) {
                    // auto new_typed_index = TypedIndex<TypedItem_Address, SetIndex>(new_address, bindex.getIndexType());
                    m_index.erase(iter);
                    m_index.insert({key, bindex});
                }            
            }
        }
    }
    
    bool Set::remove(FixtureLock &, std::size_t key, ObjectPtr key_value)
    {
        auto iter = m_index.find(key);
        if (iter == m_index.end()) {
            return false;
        }
        auto [it_key, address] = *iter;
        auto bindex = address.getIndex(this->getMemspace());

        auto it = bindex.beginJoin(1);
        auto fixture = this->getFixture(); 
        while (!it.is_end()) {
            auto [storage_class, value] = *it;
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
            if (LangToolkit::compare(key_value, member.get())) {
                if (bindex.size() == 1) {
                    m_index.erase(iter);
                    unrefMember<LangToolkit>(fixture, storage_class, value);                 
                    bindex.destroy();
                } else {
                    bindex.erase(*it);
                }
                --modify().m_size;
                return true;
            }
            ++it;
        }
        return false;
    }

    Set::ObjectSharedPtr Set::getItem(std::size_t hash_key, ObjectPtr key_value) const
    {
        auto iter = m_index.find(hash_key);
        if (iter == m_index.end()) {
            return nullptr;            
        }
        
        auto [key, address] = *iter;        
        auto fixture = this->getFixture(); 
        auto bindex = address.getIndex(*fixture);
        auto it = bindex.beginJoin(1);        
        while (!it.is_end()) {
            auto [storage_class, value] = *it;
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
            if (LangToolkit::compare(key_value, member.get())) {
                return member;
            }
            ++it;
        }
        return nullptr;
    }
    
    Set *Set::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture) {
        return new (at_ptr) Set(fixture);
    }
    
    Set *Set::unload(void *at_ptr, db0::swine_ptr<Fixture> &fixture, std::uint64_t address) {
        return new (at_ptr) Set(fixture, address);
    }

    void Set::destroy() const
    {
        unrefMembers();        
        m_index.destroy();
        super_t::destroy();
    }

    Set::ObjectSharedPtr Set::pop()
    {
        auto iter = m_index.begin();
        if (iter == m_index.end()) {
            return nullptr;
        }

        auto [key, address] = *iter;
        auto bindex = address.getIndex(this->getMemspace());
        auto it = bindex.beginJoin(1);
        auto [storage_class, value] = *it;
        auto fixture = this->getFixture();
        auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
        if (bindex.size() == 1) {
            m_index.erase(iter);
            bindex.destroy();
        } else {
            bindex.erase(*it);
        }
        --modify().m_size;
        return member;
    }

    bool Set::has_item(ObjectPtr obj) const
    {
        auto hash = PyObject_Hash(obj);
        auto item = getItem(hash, obj);
        return item != nullptr;
    }

    void Set::moveTo(db0::swine_ptr<Fixture> &fixture) {
        assert(hasInstance());
        if(this->size() > 0) {
            THROWF(db0::InputException) << "Set with items cannot be moved to another fixture";
        }
        super_t::moveTo(fixture);
    }
    
    std::size_t Set::size() const { 
        return (*this)->m_size;
    }

    void Set::clear()
    {
        unrefMembers();
        m_index.clear();
        modify().m_size = 0; 
    }

    Set::const_iterator Set::begin() const {
        return m_index.begin();
    }

    Set::const_iterator Set::end() const {
        return m_index.end();
    }

    void Set::commit() const
    {
        m_index.commit();
        super_t::commit();
    }
    
    void Set::detach() const
    {
        // FIXME: can be removed when GC0 calls commit-op
        commit();
        m_index.detach();
        super_t::detach();
    }

    void Set::unrefMembers() const
    {
        auto fixture = this->getFixture();
        for (auto [_, address] : m_index) {
            auto bindex = address.getIndex(this->getMemspace());
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = *it;
                unrefMember<LangToolkit>(fixture, storage_class, value);
                ++it;
            }
        }
    }

}
