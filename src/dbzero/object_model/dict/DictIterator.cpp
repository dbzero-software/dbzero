#include "DictIterator.hpp"
#include "Dict.hpp"
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <cassert>

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
        auto fixture = m_collection->getFixture();
        auto [key, value] = *m_join_iterator;

        DictItem dict_item(
            unloadMember<LangToolkit>(fixture, key).get(),
            unloadMember<LangToolkit>(fixture, value).get());
        iterNext();
        return dict_item;
    }
    
    DictIterator::ObjectSharedPtr DictIterator::nextValue()
    {
        auto fixture = m_collection->getFixture();
        auto value = unloadMember<LangToolkit>(fixture, (*m_join_iterator).m_second);
        iterNext();
        return value;
    }

    DictIterator::ObjectSharedPtr DictIterator::nextKey() 
    {
        auto fixture = m_collection->getFixture();
        auto key = unloadMember<LangToolkit>(fixture, (*m_join_iterator).m_first);
        iterNext();
        return key;
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
                DictItem item = nextItem();
                return ObjectSharedPtr(PyTuple_Pack(2, item.key.get(), item.value.get()), false);
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