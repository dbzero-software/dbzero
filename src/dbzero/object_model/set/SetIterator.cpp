#include "SetIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>
namespace db0::object_model

{
    SetIterator::SetIterator(Set::iterator iterator, Set * ptr) 
        : PyObjectIterator<SetIterator, Set>(iterator, ptr) 
    {
        getJoinIterator();
    }

    void SetIterator::getJoinIterator()
    {
        if (m_iterator != m_collection->end())
        {
            auto [key, address] = *m_iterator;
            auto fixture = m_collection->getFixture();
            std::cerr << "ADDRESS getJoinIterator" << address.m_type << " " << address.m_index_address.as_value.m_value.m_store << std::endl;
            m_index = address.getIndex(m_collection->getMemspace());
            m_join_iterator = m_index.beginJoin(1);
            auto item = *m_join_iterator;
            auto [storage_class, value] = item;

            std::cerr << "NEXT " << storage_class << " " << value.m_store << std::endl;
        }
    }

    SetIterator::ObjectSharedPtr SetIterator::next()
    {
        std::cerr << "SIZE " << m_collection->size() << std::endl;
        auto fixture = m_collection->getFixture();
        auto item = *m_join_iterator;
        auto [storage_class, value] = item;

        std::cerr << "NEXT " << storage_class << " " << value.m_store << std::endl;
        auto member = unloadMember<LangToolkit>(fixture, storage_class, value);
        ++m_join_iterator;
        if (m_join_iterator.is_end())
        {
            ++m_iterator;
            getJoinIterator();
        }
        return member;
    }

}