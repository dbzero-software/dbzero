// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SetIterator.hpp"
#include "Set.hpp"
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    SetIterator::SetIterator(Set::const_iterator iterator, const Set *ptr, ObjectPtr lang_set_ptr)
        : BaseIterator<SetIterator, Set>(iterator, ptr, lang_set_ptr)
    {
        setJoinIterator();
    }

    void SetIterator::setJoinIterator()
    {
        assureAttached();
        if (m_iterator != m_collection->end()) {
            auto [key, address] = *m_iterator;
            m_current_hash = key;
            auto fixture = m_collection->getFixture();
            m_index = address.getIndex(m_collection->getMemspace());
            m_join_iterator = m_index.beginJoin(1);
            assert(!m_join_iterator.is_end());
            m_current_key = *m_join_iterator;
        } else {
            m_is_end = true;
        }
    }
    
    SetIterator::ObjectSharedPtr SetIterator::next()
    {
        assureAttached();
        auto fixture = m_collection->getFixture();
        auto item = *m_join_iterator;
        auto [storage_class, value] = item;

        auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, m_member_flags);
        ++m_join_iterator;
        if (m_join_iterator.is_end()) {
            ++m_iterator;
            setJoinIterator();
        }
        return member;
    }

    void SetIterator::restore()
    {
        // restore as end
        if (m_is_end) {
            m_iterator = m_collection->end();
            return;
        }
        m_iterator = m_collection->find(m_current_hash);
        if (m_iterator == m_collection->end()) {
            m_is_end = true;
            return;
        }
        
        auto [key, address] = *m_iterator;
        m_current_hash = key;
        auto fixture = m_collection->getFixture();
        m_index = address.getIndex(m_collection->getMemspace());
        m_join_iterator = m_index.beginJoin(1);
        if (m_join_iterator.join(m_current_key)) {
            m_current_key = *m_join_iterator;
        } else {
            ++m_iterator;
            setJoinIterator();
        }
    }
    
}