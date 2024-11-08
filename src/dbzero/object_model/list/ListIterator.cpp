#include "ListIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    ListIterator::ListIterator(List::const_iterator iterator, const List *ptr, ObjectPtr lang_list_ptr)
        : PyObjectIterator<ListIterator, List>(iterator, ptr, lang_list_ptr)
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