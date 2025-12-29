// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectTagManager.hpp"
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{

    ObjectTagManager::ObjectTagManager(ObjectPtr const *memo_ptr, std::size_t nargs)
        : m_info(memo_ptr[0])
        , m_info_vec_ptr((nargs > 1) ? (new ObjectInfo[nargs - 1]) : nullptr)
        , m_info_vec_size(nargs - 1)
        , m_access_mode(m_info.m_access_mode)
        , m_fixtures(m_info.getFixture())
    {
        assert(nargs > 0);
        for (std::size_t i = 1; i < nargs; ++i) {
            m_info_vec_ptr[i - 1] = ObjectInfo(memo_ptr[i]);
            m_fixtures.add(m_info_vec_ptr[i - 1].getFixture());
            if (m_info_vec_ptr[i - 1].m_access_mode != AccessType::READ_WRITE) {
                m_access_mode = AccessType::READ_ONLY;                
            }            
        }
    }
    
    ObjectTagManager::ObjectTagManager()
        : m_empty(true)
    {
    }

    ObjectTagManager::~ObjectTagManager()
    {
        if (!m_empty) {
            if (m_info_vec_ptr) {
                delete[] m_info_vec_ptr;
            }
        }
    }
    
    ObjectTagManager *ObjectTagManager::makeNew(void *at_ptr, ObjectPtr const *memo_ptr, std::size_t nargs)
    {
        if (nargs == 0) {
            // construct as empty
            return new (at_ptr) ObjectTagManager();
        }
        return new (at_ptr) ObjectTagManager(memo_ptr, nargs);    
    }
    
    ObjectTagManager::ObjectInfo::ObjectInfo(ObjectPtr memo_ptr)
        : m_lang_ptr(memo_ptr)
        , m_object_ptr(&ObjectTagManager::LangToolkit::getTypeManager().extractAnyObject(memo_ptr))
        , m_tag_index_ptr(&m_object_ptr->getFixture()->get<TagIndex>())
        , m_type(m_object_ptr->getClassPtr())
        , m_access_mode(m_object_ptr->getFixture()->getAccessType())
        , m_has_tags(LangToolkit::hasTagRefs(memo_ptr))
    {
    }    
    
    void ObjectTagManager::ObjectInfo::add(ObjectPtr const *args, Py_ssize_t nargs)
    {
        assert(m_tag_index_ptr);
        assert(m_access_mode == AccessType::READ_WRITE);
        m_tag_index_ptr->addTags(m_lang_ptr.get(), args, nargs);
        // assign default tags (only when adding the first tag)
        if (!m_has_tags) {
            auto type = m_type;
            while (type) {
                // also add type as tag (once)
                m_tag_index_ptr->addTag(m_lang_ptr.get(), type->getAddress(), true);
                type = type->tryGetBaseClass();
            }
            m_has_tags = true;
        }
    }
    
    void ObjectTagManager::ObjectInfo::remove(ObjectPtr const *args, Py_ssize_t nargs)
    {
        assert(m_access_mode == AccessType::READ_WRITE);
        m_tag_index_ptr->removeTags(m_lang_ptr.get(), args, nargs);
    }

    void ObjectTagManager::add(ObjectPtr const *args, Py_ssize_t nargs)
    {
        if (m_empty) {
            return;
        }

        if (m_access_mode != AccessType::READ_WRITE) {
            THROWF(db0::InputException) << "ObjectTagManager: cannot add tags to read-only object";
        }
        m_info.add(args, nargs);
        for (std::size_t i = 0; i < m_info_vec_size; ++i) {
            m_info_vec_ptr[i].add(args, nargs);
        }
        onUpdated(); 
    }
    
    void ObjectTagManager::remove(ObjectPtr const *args, Py_ssize_t nargs)
    {
        if (m_empty) {
            return;
        }
        
        if (m_access_mode != AccessType::READ_WRITE) {
            THROWF(db0::InputException) << "ObjectTagManager: cannot add tags to read-only object";
        }
        m_info.remove(args, nargs);
        for (std::size_t i = 0; i < m_info_vec_size; ++i) {
            m_info_vec_ptr[i].remove(args, nargs);
        }        
        onUpdated();
    }
    
    db0::swine_ptr<Fixture> ObjectTagManager::ObjectInfo::getFixture() const {
        return m_object_ptr ? m_object_ptr->getFixture() : db0::swine_ptr<Fixture>();
    }
 
    void ObjectTagManager::onUpdated()
    {
        if (m_on_updated) {
            return;
        }
        m_on_updated = true;
        for (std::size_t i = 0; i < m_fixtures.size(); ++i) {
            auto fx = m_fixtures[i].lock();
            if (fx) {
                fx->onUpdated();
            }
        }
    }

}