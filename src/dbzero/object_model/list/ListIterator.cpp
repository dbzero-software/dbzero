// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ListIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    ListIterator::ListIterator(List::const_iterator iterator, const List *ptr, ObjectPtr lang_list_ptr)
        : BaseIterator<ListIterator, List>(iterator, ptr, lang_list_ptr)
    {
    }
    
    ListIterator::ObjectSharedPtr ListIterator::next()
    {
        assureAttached();
        assert(!is_end());
        auto fixture = m_collection->getFixture();
        auto [storage_class, value] = *m_iterator;
        ++m_iterator;
        ++m_index;        
        return unloadMember<LangToolkit>(fixture, storage_class, value, 0, m_member_flags);
    }
    
    void ListIterator::restore()
    {
        if (m_index <this->m_collection->size()) {
            m_iterator = this->m_collection->begin(m_index);
        } else {
            // restore as end iterator
            m_iterator = this->m_collection->end();
        }
    }
    
}