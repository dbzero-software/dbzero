#include "DictIterator.hpp"
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
            auto fixture = m_collection->getFixture();
            auto index = address.getIndex(m_collection->getMemspace());
            m_index = index;
            m_join_iterator = m_index.beginJoin(1);
        }
    }
    
    void DictIterator::iterNext()
    {
        ++m_join_iterator;
        if (m_join_iterator.is_end()) {
            ++m_iterator;
            setJoinIterator();
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
    
}