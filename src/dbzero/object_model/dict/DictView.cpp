// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DictView.hpp"
#include "DictIterator.hpp"
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
    
    std::shared_ptr<DictIterator> DictView::getIterator() const
    {
        return std::shared_ptr<DictIterator>(new DictIterator(
            m_collection->begin(), m_collection, *m_lang_dict_shared_ptr, m_type)
        );
    }
    
    std::size_t DictView::size() const {
        return m_collection->size();
    }
    
}