#include "ObjectTagManager.hpp"
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{

    ObjectTagManager::ObjectInfo::ObjectInfo(ObjectPtr memo_ptr)
        : m_lang_ptr(memo_ptr)
        , m_object_ptr(&ObjectTagManager::LangToolkit::getTypeManager().extractObject(memo_ptr))
        , m_tag_index_ptr(&m_object_ptr->getFixture()->get<TagIndex>())
        , m_type(m_object_ptr->getClassPtr())
    {
    }

    ObjectTagManager::ObjectTagManager(ObjectPtr const *memo_ptr, std::size_t nargs)
        : m_info(memo_ptr[0])
        , m_info_vec_ptr((nargs > 1) ? (new ObjectInfo[nargs - 1]) : nullptr)
        , m_info_vec_size(nargs - 1)
    {
        assert(nargs > 0);
        for (std::size_t i = 1; i < nargs; ++i) {
            m_info_vec_ptr[i - 1] = ObjectInfo(memo_ptr[i]);
        }
    }

    ObjectTagManager::~ObjectTagManager()
    {
        if (m_info_vec_ptr) {
            delete[] m_info_vec_ptr;
        }
    }
        
    ObjectTagManager *ObjectTagManager::makeNew(void *at_ptr, ObjectPtr const *memo_ptr, std::size_t nargs) {
        return new (at_ptr) ObjectTagManager(memo_ptr, nargs);
    }

    void ObjectTagManager::ObjectInfo::add(ObjectPtr const *args, Py_ssize_t nargs)
    {
        assert(m_tag_index_ptr);
        m_tag_index_ptr->addTags(m_lang_ptr.get(), args, nargs);
        if (m_type) {
            // also add type as tag (once)
            m_tag_index_ptr->addTag(m_lang_ptr.get(), m_type->getAddress());
            m_type = nullptr;
        }
    }

    void ObjectTagManager::ObjectInfo::remove(ObjectPtr const *args, Py_ssize_t nargs) {
        m_tag_index_ptr->removeTags(m_lang_ptr.get(), args, nargs);
    }

    void ObjectTagManager::add(ObjectPtr const *args, Py_ssize_t nargs) 
    {
        m_info.add(args, nargs);
        for (std::size_t i = 0; i < m_info_vec_size; ++i) {
            m_info_vec_ptr[i].add(args, nargs);
        }
    }

    void ObjectTagManager::remove(ObjectPtr const *args, Py_ssize_t nargs)
    {
        m_info.remove(args, nargs);
        for (std::size_t i = 0; i < m_info_vec_size; ++i) {
            m_info_vec_ptr[i].remove(args, nargs);
        }
    }
    
}