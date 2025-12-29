// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.


#include "Dict.hpp"
#include "DictIterator.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/value.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    namespace py = db0::python;
    GC0_Define(Dict)
    
    template <typename LangToolkit> o_typed_item createTypedItem(db0::swine_ptr<Fixture> &fixture,
        typename LangToolkit::ObjectPtr lang_value, AccessFlags access_mode)
    {
        auto type_id = LangToolkit::getTypeManager().getTypeId(lang_value);
        // NOTE: packed storage not supported for dict keys/values
        auto pre_storage_class = TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id, false);
        StorageClass storage_class;
        if (pre_storage_class == PreStorageClass::OBJECT_WEAK_REF) {
            storage_class = db0::getStorageClass(pre_storage_class, fixture, lang_value);
        } else {
            storage_class = db0::getStorageClass(pre_storage_class);
        }

        return {
            storage_class, 
            createMember<LangToolkit>(fixture, type_id, storage_class, lang_value, access_mode)
        };
    }
    
    template <typename LangToolkit> dict_item createDictItem(db0::swine_ptr<Fixture> &fixture, std::uint64_t hash,
        o_typed_item key, o_typed_item value, AccessFlags)
    {
        // FIXME: currently unable to pass access flags since MorphingBIndex doesn't support it
        DictIndex bindex(*fixture, o_pair_item(key, value));
        return { hash, bindex };
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture, AccessFlags access_mode)
        : super_t(fixture, access_mode)
        , m_index(*fixture)
    {
        modify().m_index_ptr = m_index.getAddress();
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture, Address address, AccessFlags access_mode)
        : super_t(super_t::tag_from_address(), fixture, address, access_mode)
        , m_index(myPtr((*this)->m_index_ptr))
    {
    }
    
    Dict::Dict(db0::swine_ptr<Fixture> &fixture, const Dict &dict)
        : super_t(fixture)
        , m_index(*fixture)
    {
        initWith(dict);
    }

    Dict::Dict(tag_no_gc, db0::swine_ptr<Fixture> &fixture, const Dict &dict)
        : super_t(tag_no_gc(), fixture)
        , m_index(*fixture)
    {
        initWith(dict);
    }

    Dict::~Dict()
    {
        // unregister needs to be called before destruction of members
        unregister();
    }

    void Dict::operator=(Dict &&other)
    {
        clear();
        super_t::operator=(std::move(other));
        m_index = std::move(other.m_index);
        assert(!other.hasInstance());
        restoreIterators();
    }
    
    void Dict::initWith(const Dict &dict)
    {
        modify().m_index_ptr = m_index.getAddress();
        for (auto [hash, address] : dict) {
            auto bindex = address.getIndex(this->getMemspace());
            auto bindex_copy = DictIndex(bindex);
            m_index.insert(dict_item(hash, bindex_copy));            
        }
        modify().m_size = dict.size();
    }
    
    void Dict::setItem(FixtureLock &fixture, std::uint64_t key_hash, ObjectPtr key, ObjectPtr value)
    {
        using TypeId = db0::bindings::TypeId;
        if (value == nullptr) {
            // remove the item if value is nullptr (for e.g when del is called)
            pop(key_hash, key);
            restoreIterators();
            return;
        }
        auto member_flags = getMemberFlags();
        auto key_item = createTypedItem<LangToolkit>(*fixture, key, member_flags);
        auto value_item = createTypedItem<LangToolkit>(*fixture, value, member_flags);

        auto it = m_index.find(key_hash);
        if (it == m_index.end()) {
            m_index.insert(
                createDictItem<LangToolkit>(*fixture, key_hash, key_item, value_item, member_flags)
            );
            ++modify().m_size;
        } else {
            auto address = (*it).value;
            auto bindex = address.getIndex(**fixture);
            auto it_join = bindex.beginJoin(1);
            while (!it_join.is_end()) {
                auto [storage_class, value] = (*it_join).m_first;
                auto member = unloadMember<LangToolkit>(*fixture, storage_class, value, 0, member_flags);
                if (LangToolkit::compare(key, member.get())) {
                    bindex.erase(*it_join);
                    unrefMember<LangToolkit>(*fixture, storage_class, value);
                    --modify().m_size;
                    break;
                }
                ++it_join;
            }
            ++modify().m_size;
            bindex.insert(o_pair_item(key_item, value_item));
            auto new_address = bindex.getAddress();
            if (new_address != address.m_index_address) {
                it.modifyItem().value.m_index_address = new_address;
                it.modifyItem().value.m_type = bindex.getIndexType();
            }
        }
        restoreIterators();
    }
    
    Dict::ObjectSharedPtr Dict::getItem(std::uint64_t key_hash, ObjectPtr key_value) const
    {
        auto fixture = this->getFixture();
        auto iter = m_index.find(key_hash);
        if (iter != m_index.end()) {
            auto [key, address] = *iter;            
            auto bindex = address.getIndex(*fixture);
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = (*it).m_first;
                auto member = unloadMember<LangToolkit>(
                    fixture, storage_class, value, 0, this->getMemberFlags()
                );
                if (LangToolkit::compare(key_value, member.get())) {
                    auto [storage_class, value] = (*it).m_second;
                    return unloadMember<LangToolkit>(
                        fixture, storage_class, value, 0, this->getMemberFlags()
                    );
                }
                ++it;
            }
        }
        return {};
    }
            
    bool Dict::hasItem(std::int64_t key_hash, ObjectPtr key_value) const
    {   
        auto fixture = this->getFixture();
        auto iter = m_index.find(key_hash);
        if (iter != m_index.end()) {
            auto [key, address] = *iter;            
            auto bindex = address.getIndex(*fixture);
            auto it = bindex.beginJoin(1);
            while (!it.is_end()) {
                auto [storage_class, value] = (*it).m_first;
                auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, this->getMemberFlags());
                if (LangToolkit::compare(key_value, member.get())) {
                    // a matching key was found
                    return true;
                }
                ++it;
            }
        }
        return false;
    }

    Dict *Dict::copy(void *at_ptr, db0::swine_ptr<Fixture> &fixture) const {
        return new (at_ptr) Dict(fixture, *this);
    }
    
    Dict::ObjectSharedPtr Dict::pop(int64_t hash, ObjectPtr obj)
    {        
        auto iter = m_index.find(hash);
        if (iter == m_index.end()) {
            return nullptr;
        }

        auto [key, address] = *iter;
        auto bindex = address.getIndex(this->getMemspace());
        auto it = bindex.beginJoin(1);
        auto [key_storage_class, key_value] = (*it).m_first;
        auto [storage_class, value] = (*it).m_second;
        auto fixture = this->getFixture();        
        
        auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, this->getMemberFlags());

        unrefMember<LangToolkit>(fixture, key_storage_class, key_value);
        unrefMember<LangToolkit>(fixture, storage_class, value);
        
        if (bindex.size() == 1) {
            bindex.destroy();
            m_index.erase(iter);            
        } else {
            // NOTE: morping-bindex address my be subject to change on erase
            bindex.erase(*it);
            auto new_address = bindex.getAddress();
            if (new_address != address.m_index_address) {
                iter.modifyItem().value.m_index_address = new_address;
                iter.modifyItem().value.m_type = bindex.getIndexType();
            }            
        }
        --modify().m_size;
        restoreIterators();
        return member;        
    }
    
    void Dict::moveTo(db0::swine_ptr<Fixture> &fixture)
    {
        if (this->size() > 0) {
            THROWF(db0::InputException) << "Dict with items cannot be moved to another fixture";
        }
        assert(hasInstance());
        super_t::moveTo(fixture);
    }

    std::size_t Dict::size() const {
        return (*this)->m_size;
    }

    void Dict::clear()
    {
        unrefMembers();
        m_index.clear();
        modify().m_size = 0;
        restoreIterators();
    }
    
    void Dict::commit() const
    {
        m_index.commit();
        super_t::commit();        
    }
    
    void Dict::detach() const
    {
        // FIXME: can be removed when GC0 calls commit-op
        commit();
        m_index.detach();
        // detach all associated iterators
        m_iterators.forEach([](DictIterator &iter) {
            iter.detach();
        });
        super_t::detach();
    }
    
    Dict::const_iterator Dict::begin() const {
        return m_index.begin();
    }

    Dict::const_iterator Dict::end() const {
        return m_index.end();
    }
    
    void Dict::destroy()
    {
        unrefMembers();
        m_index.destroy();
        super_t::destroy();
    }
    
    void Dict::unrefMembers() const
    {
        auto fixture = this->getFixture();
        for (auto [_, address] : m_index) {
            auto bindex = address.getIndex(this->getMemspace());
            auto it = bindex.beginJoin(1);
            for (; !it.is_end(); ++it) {
                auto [storage_class_1, value_1] = (*it).m_first;
                unrefMember<LangToolkit>(fixture, storage_class_1, value_1);
                auto [storage_class_2, value_2] = (*it).m_second;
                unrefMember<LangToolkit>(fixture, storage_class_2, value_2);
            }
        }
    }
    
    std::shared_ptr<DictIterator> Dict::getIterator(ObjectPtr lang_dict) const
    {
        auto iter = std::shared_ptr<DictIterator>(new DictIterator(m_index.begin(), this, lang_dict));
        m_iterators.push_back(iter);
        return iter;
    }
    
    void Dict::restoreIterators()
    {
        if (m_iterators.cleanup()) {
            return;
        }
        m_iterators.forEach([](DictIterator &iter) {
            iter.restore();
        });
    }
    
    Dict::const_iterator Dict::find(std::uint64_t key_hash) const {
        return m_index.find(key_hash);
    }

}
