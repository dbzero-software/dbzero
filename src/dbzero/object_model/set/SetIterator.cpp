#include "SetIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>
namespace db0::object_model

{
    SetIterator::SetIterator(Set::iterator iterator, Set * ptr) : PyObjectIterator<SetIterator, Set>(iterator, ptr) {
    }

    SetIterator::ObjectSharedPtr SetIterator::next(){

        auto [key, item] = *m_iterator;
        auto [storage_class, value] = item;
        ++m_iterator;
        return unloadMember<LangToolkit>(*m_collection, storage_class, value);
    }

}