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
                throw std::runtime_error("Not implemented");
            }
                /* FIXME: this code should return Python tuple - no dbzero mutation:

                auto *tuple_object = db0::python::TupleObject_new(&db0::python::TupleObjectType, NULL, NULL);
                auto fixture = db0::python::PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
                db0::object_model::Tuple::makeNew(&tuple_object->ext(), *fixture, 2);
                tuple_object->ext().setItem(0, item.key.get());
                tuple_object->ext().setItem(1, item.value.get());    
                return tuple_object;
                */
        }
        return item.key;
    }

}