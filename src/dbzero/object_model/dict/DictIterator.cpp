#include "DictIterator.hpp"
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::object_model

{

    void DictIterator::getJoinIterator()
    {
        if (m_iterator != m_collection->end())
        {
            auto [key, address] = *m_iterator;
            auto fixture = m_collection->getFixture();
            m_index = address.getIndex(m_collection->getMemspace());
            m_join_iterator = m_index.beginJoin(1);
        }
    }

    DictIterator::DictIterator(Dict::iterator iterator, Dict * ptr, IteratorType type) 
        : PyObjectIterator<DictIterator, Dict>(iterator, ptr)
        , m_type(type) 
    {
        getJoinIterator();
    }

    DictIterator::DictItem DictIterator::nextItem()
    {

        auto fixture = m_collection->getFixture();
        auto [key, value] = *m_join_iterator;

        DictItem dict_item(unloadMember<LangToolkit>(fixture, key),
                           unloadMember<LangToolkit>(fixture, value));
        ++m_join_iterator;
        if (m_join_iterator.is_end())
        {
            ++m_iterator;
            getJoinIterator();
        }
        return dict_item;
    }

    DictIterator::ObjectSharedPtr DictIterator::nextValue() {
        DictItem item = nextItem();
        return item.value;
    }

    DictIterator::ObjectSharedPtr DictIterator::nextKey() {
        DictItem item = nextItem();
        return item.key;
    }

    DictIterator::ObjectSharedPtr DictIterator::next() 
    {
        DictItem item = nextItem();
        switch(m_type){
            case VALUES: {
                return item.value;
            }

            case KEYS: {
                return item.key;
            }

            case ITEMS: {
                return PyTuple_Pack(2, item.key.get(), item.value.get());
            }
        }
        return item.key;
    }

}