#include "SetIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    SetIterator::SetIterator(Set::const_iterator iterator, const Set *ptr, ObjectPtr lang_set_ptr)
        : BaseIterator<SetIterator, Set>(iterator, ptr, lang_set_ptr)
    {
        setJoinIterator();
    }

    void SetIterator::setJoinIterator()
    {
        if (m_iterator != m_collection->end()) {
            auto [key, address] = *m_iterator;
            auto fixture = m_collection->getFixture();
            m_index = address.getIndex(m_collection->getMemspace());
            m_join_iterator = m_index.beginJoin(1);
        }
    }
    
    SetIterator::ObjectSharedPtr SetIterator::next()
    {
        auto fixture = m_collection->getFixture();
        auto item = *m_join_iterator;
        auto [storage_class, value] = item;

        auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
        ++m_join_iterator;
        if (m_join_iterator.is_end()) {
            ++m_iterator;
            setJoinIterator();
        }
        return member;
    }

}