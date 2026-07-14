// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DictIterator.hpp"
#include <cassert>
#include "Dict.hpp"
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0::object_model

{

    DictIterator::DictIterator(
        Dict::const_iterator iterator, const Dict * ptr, ObjectPtr lang_dict, IteratorType type)
        : BaseIterator<DictIterator, Dict>(iterator, ptr, lang_dict)
        , m_type(type)        
    {
        setJoinIterator();
    }
    
    void DictIterator::setJoinIterator()
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
    
    void DictIterator::iterNext()
    {
        assureAttached();
        ++m_join_iterator;
        if (m_join_iterator.is_end()) {
            ++m_iterator;
            setJoinIterator();
        } else {
            m_current_key = *m_join_iterator;
        }
    }
    
    DictIterator::DictItem DictIterator::nextItem()
    {
        assureAttached();
        auto fixture = m_collection->getFixture();
        auto [key, value] = *m_join_iterator;
        
        iterNext();
        return {
            unloadMember<LangToolkit>(fixture, key, 0, m_member_flags),
            unloadMember<LangToolkit>(fixture, value, 0, m_member_flags)
        };
    }
    
    DictIterator::ObjectSharedPtr DictIterator::nextValue()
    {        
        assureAttached();
        auto value = (*m_join_iterator).m_second;        
        iterNext();
        auto fixture = m_collection->getFixture();
        return unloadMember<LangToolkit>(fixture, value, 0, m_member_flags);
    }
    
    DictIterator::ObjectSharedPtr DictIterator::nextKey()
    {
        assureAttached();
        auto key = (*m_join_iterator).m_first;
        iterNext();
        auto fixture = m_collection->getFixture();
        return unloadMember<LangToolkit>(fixture, key, 0, m_member_flags);
    }
    
    DictIterator::ObjectSharedPtr DictIterator::next()
    {        
        switch (m_type) {
            case VALUES: {
                return nextValue();
            }
            
            case KEYS: {
                return nextKey();
            }
            
            case ITEMS: {
                auto item = nextItem();
                return Py_OWN(PySafeTuple_Pack(item.key, item.value));
            }
            
            default: {
                assert(false);
                THROWF(InternalException) << "Unknown iterator type" << THROWF_END;
            }
        }
    }
    
    void DictIterator::restore()
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