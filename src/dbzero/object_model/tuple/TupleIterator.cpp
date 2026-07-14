// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TupleIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    TupleIterator::TupleIterator(Tuple::const_iterator iterator, const Tuple *ptr, ObjectPtr lang_tuple_ptr)
        : BaseIterator<TupleIterator, Tuple>(iterator, ptr, lang_tuple_ptr)
    {
    }
    
    TupleIterator::ObjectSharedPtr TupleIterator::next()
    {
        auto [storage_class, value] = *m_iterator;
        ++m_iterator;
        auto fixture = m_collection->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value, 0, m_member_flags);
    }
    
    bool TupleIterator::is_end() const
    {        
        return m_iterator == m_collection->getData()->items().end();
    }
    
}