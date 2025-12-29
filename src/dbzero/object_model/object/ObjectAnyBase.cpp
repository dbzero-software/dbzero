// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectAnyBase.hpp"

namespace db0::object_model

{
        
    ObjectInitializerManager InitManager::instance;
    
    template <typename T, typename ImplT>
    ObjectAnyBase<T, ImplT>::ObjectAnyBase(tag_as_dropped, UniqueAddress addr, unsigned int ext_refs)
        : m_flags { ObjectOptions::DROPPED }
        , m_ext_refs(ext_refs)
        , m_unique_address(addr)        
    {
    }
    
    template <typename T, typename ImplT>
    db0::swine_ptr<Fixture> ObjectAnyBase<T, ImplT>::tryGetFixture() const
    {
        if (!this->hasInstance()) {
            if (isDropped()) {
                return {};
            }
            // retrieve from the initializer
            return InitManager::instance.getInitializer(*this).tryGetFixture();
        }
        return super_t::tryGetFixture();
    }

    template <typename T, typename ImplT>
    db0::swine_ptr<Fixture> ObjectAnyBase<T, ImplT>::getFixture() const
    {
        auto fixture = this->tryGetFixture();
        if (!fixture) {
            THROWF(db0::InternalException) << "Object is no longer accessible";
        }
        return fixture;
    }

    template <typename T, typename ImplT>
    Memspace &ObjectAnyBase<T, ImplT>::getMemspace() const {
        return *getFixture();
    }

    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::setFixture(db0::swine_ptr<Fixture> &new_fixture)
    {        
        if (this->hasInstance()) {
            THROWF(db0::InputException) << "set_prefix failed: object already initialized";
        }
        
        if (!InitManager::instance.getInitializer(*this).trySetFixture(new_fixture)) {
            // signal problem with PyErr_BadPrefix
            auto fixture = this->getFixture();
            LangToolkit::setError(LangToolkit::getTypeManager().getBadPrefixError(), fixture->getUUID());
        }
    }
    
    template <typename T, typename ImplT>
    Address ObjectAnyBase<T, ImplT>::getAddress() const
    {
        assert(!isDefunct());
        if (!this->hasInstance()) {            
            THROWF(db0::InternalException) << "Object instance does not exist yet (did you forget to use db0.materialized(self) in constructor ?)";
        }
        return super_t::getAddress();
    }

    template <typename T, typename ImplT>
    UniqueAddress ObjectAnyBase<T, ImplT>::getUniqueAddress() const
    {
        if (this->hasInstance()) {
            return super_t::getUniqueAddress();
        } else {
            // NOTE: defunct objects don't have a valid address (not assigned yet)
            assert(m_flags[ObjectOptions::DROPPED]);
            return m_unique_address;
        }
    }

    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::incRef(bool is_tag)
    {
        if (this->hasInstance()) {
            super_t::incRef(is_tag);
        } else {
            // incRef with the initializer
            InitManager::instance.getInitializer(*this).incRef(is_tag);
        }
    }
    
    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::decRef(bool is_tag)
    {
        // this operation is a potentially silent mutation
        _touch();
        super_t::decRef(is_tag);        
    }
    
    template <typename T, typename ImplT>
    bool ObjectAnyBase<T, ImplT>::hasAnyRefs() const {
        return (*this)->hasAnyRefs();
    }

    template <typename T, typename ImplT>
    bool ObjectAnyBase<T, ImplT>::hasTagRefs() const {
        return this->hasInstance() && (*this)->m_header.m_ref_counter.getFirst() > 0;
    }

    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::touch()
    {
        if (this->hasInstance() && !this->isDefunct()) {
            // NOTE: for already modified and small objects we may skip "touch"
            if (!super_t::isModified() || this->span() > 1) {
                // NOTE: large objects i.e. with span > 1 must always be marked with a silent mutation flag
                // this is because the actual change may be missed if performed on a different-then the 1st DP
                this->_touch();
            }            
        }
    }

    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::_touch()
    {
        if (!m_touched) {
            // mark the 1st byte of the object as modified (forced-diff)
            // this is always the 1st DP occupied by the object
            this->modify(0, 1);
            m_touched = true;
        }
    }

    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::addExtRef() const {
        ++m_ext_refs;
    }

    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::removeExtRef() const
    {
        assert(m_ext_refs > 0);
        --m_ext_refs;
    }
    
    template <typename T, typename ImplT>
    void ObjectAnyBase<T, ImplT>::setDefunct() const {
        m_flags.set(ObjectOptions::DEFUNCT);
    }
    
    template <typename T, typename ImplT>
    Class &ObjectAnyBase<T, ImplT>::getType() {
        return this->m_type ? *this->m_type : InitManager::instance.getInitializer(*this).getClass();
    }

    template class ObjectAnyBase<o_object_base, ObjectAnyImpl>;
    template class ObjectAnyBase<o_object, Object>;
    template class ObjectAnyBase<o_immutable_object, ObjectImmutableImpl>;
    
}
