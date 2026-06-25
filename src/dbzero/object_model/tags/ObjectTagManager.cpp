// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectTagManager.hpp"
#include "ObjectIterator.hpp"
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <Python.h>

namespace db0::object_model

{
    ObjectTagManager::ObjectTagManager(ObjectPtr const *memo_ptr, std::size_t nargs)
        : m_empty(nargs == 0)
        , m_info_vec_ptr((nargs > 1) ? (new ObjectInfo[nargs - 1]) : nullptr)
        , m_info_vec_size(nargs > 0 ? nargs - 1 : 0)
    {
        if (m_empty) {
            return;
        }
        if (nargs > 0) {
            m_info = ObjectInfo(memo_ptr[0]);
            m_access_mode = m_info.m_access_mode;
            m_fixtures.add(m_info.getFixture());
        }
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
        , m_fixture(ObjectTagManager::LangToolkit::getTypeManager().extractObjectFixture(memo_ptr))
        , m_tag_index_ptr(&m_fixture->get<TagIndex>())
        , m_type(&LangToolkit::getMemoType(memo_ptr))
        , m_access_mode(m_fixture->getAccessType())
        , m_is_embedded(LangToolkit::isEmbeddedMemoObject(memo_ptr))
        , m_has_tags(!m_is_embedded && LangToolkit::hasTagRefs(memo_ptr))
    {
    }    

    void ObjectTagManager::ObjectInfo::add(ObjectPtr const *args, Py_ssize_t nargs)
    {
        assert(m_tag_index_ptr);
        auto &tag_index = *m_tag_index_ptr;
        assert(m_access_mode == AccessType::READ_WRITE);

        tag_index.addTags(m_lang_ptr.get(), args, nargs);
        // assign default tags (only when adding the first tag)
        if (!m_has_tags) {
            auto type = m_type;
            while (type) {
                // also add type as tag (once)
                tag_index.addTag(m_lang_ptr.get(), type->getAddress(), true);
                type = type->getBaseClassPtr();
            }
            m_has_tags = true;
        }
    }
    
    void ObjectTagManager::ObjectInfo::remove(ObjectPtr const *args, Py_ssize_t nargs)
    {
        assert(m_access_mode == AccessType::READ_WRITE);
        assert(m_tag_index_ptr);
        auto &tag_index = *m_tag_index_ptr;

        tag_index.removeTags(m_lang_ptr.get(), args, nargs);
    }

    void ObjectTagManager::add(ObjectPtr const *args, Py_ssize_t nargs)
    {
        if (m_empty) {
            return;
        }

        if (m_access_mode != AccessType::READ_WRITE) {
            THROWF(db0::InputException) << "ObjectTagManager: cannot add tags to read-only object";
        }
        if (!!m_info.m_lang_ptr) {
            m_info.add(args, nargs);
        }
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
        if (!!m_info.m_lang_ptr) {
            m_info.remove(args, nargs);
        }
        for (std::size_t i = 0; i < m_info_vec_size; ++i) {
            m_info_vec_ptr[i].remove(args, nargs);
        }
        onUpdated();
    }
    
    db0::swine_ptr<Fixture> ObjectTagManager::ObjectInfo::getFixture() const {
        return m_fixture;
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
