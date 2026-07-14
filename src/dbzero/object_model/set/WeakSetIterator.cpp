// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "WeakSetIterator.hpp"
#include "WeakSet.hpp"
#include <dbzero/bindings/python/MemoExpiredRef.hpp>
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    WeakSetIterator::WeakSetIterator(WeakSet::const_iterator iterator, const WeakSet *ptr, ObjectPtr lang_set_ptr)
        : BaseIterator<WeakSetIterator, WeakSet>(iterator, ptr, lang_set_ptr)
    {
        setJoinIterator();
        prefetchNext();
    }

    void WeakSetIterator::setJoinIterator()
    {
        assureAttached();
        if (m_iterator != m_collection->end()) {
            auto [key, address] = *m_iterator;
            m_current_hash = key;
            m_index = address.getIndex(m_collection->getMemspace());
            m_join_iterator = m_index.beginJoin(1);
            assert(!m_join_iterator.is_end());
            m_current_key = *m_join_iterator;
        } else {
            m_inner_end = true;
        }
    }

    void WeakSetIterator::prefetchNext()
    {
        m_pending.reset();
        auto fixture = m_collection->getFixture();
        while (!m_inner_end && m_iterator != m_collection->end()) {
            auto item = *m_join_iterator;
            auto [storage_class, value] = item;
            auto member = unloadMember<LangToolkit>(fixture, storage_class, value, 0, m_member_flags);
            // step to next underlying entry
            ++m_join_iterator;
            if (m_join_iterator.is_end()) {
                ++m_iterator;
                setJoinIterator();
            } else {
                m_current_key = *m_join_iterator;
            }
            if (!db0::python::MemoExpiredRef_Check(member.get())) {
                m_pending = member;
                return;
            }
        }
    }

    bool WeakSetIterator::is_end() const
    {
        return !m_pending;
    }

    WeakSetIterator::ObjectSharedPtr WeakSetIterator::next()
    {
        assureAttached();
        auto result = m_pending;
        prefetchNext();
        return result;
    }

    void WeakSetIterator::restore()
    {
        if (m_inner_end) {
            m_iterator = m_collection->end();
            return;
        }
        m_iterator = m_collection->find(m_current_hash);
        if (m_iterator == m_collection->end()) {
            m_inner_end = true;
            m_pending.reset();
            return;
        }

        auto [key, address] = *m_iterator;
        m_current_hash = key;
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
