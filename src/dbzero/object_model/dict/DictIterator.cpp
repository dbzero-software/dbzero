#include "DictIterator.hpp"
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::object_model

{
    DictIterator::DictIterator(Dict::iterator iterator, Dict * ptr, IteratorType type) 
        : PyObjectIterator<DictIterator, Dict>(iterator, ptr)
        , m_type(type) 
    {
    }

    DictIterator::DictItem DictIterator::nextItem()
    {
        auto [key, item] = *m_iterator;
        auto fixture = m_collection->getFixture();
        DictItem dict_item(unloadMember<LangToolkit>(fixture, item.m_first),
                           unloadMember<LangToolkit>(fixture, item.m_second));
        ++m_iterator;
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