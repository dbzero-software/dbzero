#include "ListIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    ListIterator::ListIterator(List::const_iterator iterator, const List *ptr, ObjectPtr lang_list_ptr)
        : BaseIterator<ListIterator, List>(iterator, ptr, lang_list_ptr)
    {
    }
    
    ListIterator::ObjectSharedPtr ListIterator::next()
    {
        auto fixture = m_collection->getFixture();
        auto [storage_class, value] = *m_iterator;
        ++m_iterator;
        ++m_index;        
        return unloadMember<LangToolkit>(fixture, storage_class, value, 0, m_member_flags);
    }
    
    void ListIterator::restore()
    {
        m_index = std::min(m_index, this->m_collection->size());
        // NOTE: may set the iterator as end
        m_iterator = this->m_collection->begin(m_index);
    }

}