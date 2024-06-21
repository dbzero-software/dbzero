#include "SetIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>
namespace db0::object_model

{
    SetIterator::SetIterator(Set::const_iterator iterator, const Set *ptr)
        : PyObjectIterator<SetIterator, Set>(iterator, ptr)
    {
    }

    SetIterator::ObjectSharedPtr SetIterator::next() 
    {
        auto [key, item] = *m_iterator;
        auto [storage_class, value] = item;
        ++m_iterator;
        auto fixture = m_collection->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value);
    }

}