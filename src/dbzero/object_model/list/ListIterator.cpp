#include "ListIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>
namespace db0::object_model

{

    ListIterator::ListIterator(List::const_iterator iterator, const List *ptr)
        : PyObjectIterator<ListIterator, List>(iterator, ptr) 
    {
    }

    ListIterator::ObjectSharedPtr ListIterator::next() 
    {
        auto [storage_class, value] = *m_iterator;
        ++m_iterator;
        auto fixture = m_collection->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value);
    }

}