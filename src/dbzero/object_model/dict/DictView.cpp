#include "DictView.hpp"
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <cassert>

namespace db0::object_model

{
   
    DictView::DictView(const Dict *dict, ObjectPtr lang_dict, IteratorType type)
        : m_collection(dict) 
        , m_type(type)
        , m_lang_dict_shared_ptr(lang_dict)
    {
    }
    
    DictIterator *DictView::begin(void *at_ptr) const
    {
        return new (at_ptr) DictIterator(
            m_collection->begin(), m_collection, m_lang_dict_shared_ptr.get(), m_type);
    }

    std::size_t DictView::size() const {
        return m_collection->size();
    }
    
    DictView *DictView::makeNew(
        void *at_ptr, const Dict *dict_ptr, ObjectPtr lang_dict, IteratorType type) 
    {
        return new (at_ptr) DictView(dict_ptr, lang_dict, type);
    }

}