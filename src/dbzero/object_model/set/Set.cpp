// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Set.hpp"
#include "SetIterator.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <object.h>

namespace db0::object_model

{

    namespace py = db0::python;
    GC0_Define(Set)
    
    template <typename LangToolkit> o_typed_item createTypedItem(db0::swine_ptr<Fixture> &fixture,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class, AccessFlags access_mode)
    {
        return { storage_class, createMember<LangToolkit>(fixture, type_id, storage_class, lang_value, access_mode) };
    }
    
    template <typename LangToolkit> set_item createSetItem(db0::swine_ptr<Fixture> &fixture, std::uint64_t key, 
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class, AccessFlags access_mode)
    {        
        auto item = createTypedItem<LangToolkit>(
            fixture, type_id, lang_value, storage_class, access_mode
        );
        SetIndex bindex(*fixture, item);
        return { key, bindex };
    }

    Set::Set(db0::swine_ptr<Fixture> &fixture, AccessFlags access_mode)
        : super_t(fixture, access_mode)
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
    
    Set::Set(db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode)
        : super_t(super_t::tag_from_address(), fixture, address, access_mode)
        , m_index(myPtr((*this)->m_index_ptr))
    {
    }

    Set::~Set()
    {
        // unregister needs to be called before destruction of members
        unregister();
    }
    
    void Set::operator=(Set && other)
    {
        unrefMembers();
        super_t::operator=(std::move(other));
        m_index = std::move(other.m_index);
        assert(!other.hasInstance());
        restoreIterators();
    }

    void Set::insert(const Set &set)
    {
        for (auto [key, address] : set) {
            auto fixture = this->getFixture();
            auto bindex = address.getIndex(*fixture);
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = (*it);
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
                append(fixture, key, member.get());
                ++it;
            }
        }
        restoreIterators();
    }

    void Set::append(db0::FixtureLock &lock, std::size_t key, ObjectSharedPtr lang_value) 
    {
        append(*lock, key, *lang_value);
        restoreIterators();
    }
    
    void Set::append(db0::swine_ptr<Fixture> &fixture, std::size_t key, ObjectPtr lang_value)
    {
        using TypeId = db0::bindings::TypeId;
        auto iter = m_index.find(key);
        // recognize type ID from language specific object
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        // NOTE: packed storage not supported for set keys
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id, false);
        StorageClass storage_class;
        if (pre_storage_class == PreStorageClass::OBJECT_WEAK_REF) {
            storage_class = db0::getStorageClass(pre_storage_class, fixture, lang_value);
        } else {
            storage_class = db0::getStorageClass(pre_storage_class);
        }
        
        bool is_modified = false;
        if (iter == m_index.end()) {
            ++modify().m_size;
            auto set_it = createSetItem<LangToolkit>(
                fixture, key, type_id, lang_value, storage_class, getMemberFlags()
            );
            m_index.insert(set_it);
            is_modified = true;
        } else {
            auto item = getItem(key, lang_value);
            if (item == nullptr) {                
                ++modify().m_size;
                auto [key, address] = *iter;
                auto bindex = address.getIndex(*fixture);
                auto item = createTypedItem<LangToolkit>(
                    fixture, type_id, lang_value, storage_class, getMemberFlags()
                );
                bindex.insert(item);
                if (bindex.getAddress() != address.m_index_address) {
                    // auto new_typed_index = TypedIndex<TypedItem_Address, SetIndex>(new_address, bindex.getIndexType());
                    m_index.erase(iter);
                    m_index.insert({key, bindex});
                }
                is_modified = true;                
            }
        }

        if (is_modified) {
            restoreIterators();
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
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
            if (LangToolkit::compare(key_value, member.get())) {
                if (bindex.size() == 1) {
                    m_index.erase(iter);
                    unrefMember<LangToolkit>(fixture, storage_class, value);                 
                    bindex.destroy();
                } else {
                    bindex.erase(*it);
                }
                --modify().m_size;
                restoreIterators();
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
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
            if (LangToolkit::compare(key_value, member.get())) {
                return member;
            }
            ++it;
        }
        return nullptr;
    }
        
    void Set::destroy()
    {
        unrefMembers();
        m_index.destroy();
        super_t::destroy();
    }

    Set::ObjectSharedPtr Set::pop(FixtureLock &)
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
        auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
        if (bindex.size() == 1) {
            m_index.erase(iter);
            bindex.destroy();
        } else {
            bindex.erase(*it);
        }
        --modify().m_size;
        restoreIterators();
        return member;
    }
    
    bool Set::hasItem(std::int64_t hash_key, ObjectPtr key_value) const
    {
        auto iter = m_index.find(hash_key);
        if (iter == m_index.end()) {
            return false;
        }
        
        auto [key, address] = *iter;        
        auto fixture = this->getFixture(); 
        auto bindex = address.getIndex(*fixture);
        auto it = bindex.beginJoin(1);        
        while (!it.is_end()) {
            auto [storage_class, value] = *it;
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, getMemberFlags());
            if (LangToolkit::compare(key_value, member.get())) {
                return true;
            }
            ++it;
        }
        return false;
    }
    
    void Set::moveTo(db0::swine_ptr<Fixture> &fixture) 
    {
        assert(hasInstance());
        if (this->size() > 0) {
            THROWF(db0::InputException) << "Set with items cannot be moved to another fixture";
        }
        super_t::moveTo(fixture);
    }
    
    std::size_t Set::size() const { 
        return (*this)->m_size;
    }
    
    void Set::clear(FixtureLock &)
    {
        unrefMembers();
        m_index.clear();
        modify().m_size = 0; 
        restoreIterators();
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
        // detach all associated iterators
        m_iterators.forEach([](SetIterator &iter) {
            iter.detach();
        });
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

    std::shared_ptr<SetIterator> Set::getIterator(ObjectPtr lang_set) const 
    {
        auto iter = std::shared_ptr<SetIterator>(new SetIterator(m_index.begin(), this, lang_set));
        m_iterators.push_back(iter);
        return iter;
    }

    void Set::restoreIterators()
    {
        if (m_iterators.cleanup()) {
            return;
        }
        m_iterators.forEach([](SetIterator &iter) {
            iter.restore();
        });
    }

    Set::const_iterator Set::find(std::uint64_t key_hash) const {
        return m_index.find(key_hash);
    }
    
}
