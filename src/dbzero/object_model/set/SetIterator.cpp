#include "SetIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>
namespace db0::object_model

{
    SetIterator::SetIterator(Set::iterator iterator, Set * ptr) 
        : PyObjectIterator<SetIterator, Set>(iterator, ptr) 
    {
    }

    SetIterator::ObjectSharedPtr SetIterator::next() 
    {
        auto [key, address] = *m_iterator;
        auto fixture = m_collection->getFixture();
        std::cerr << "ADDRESS " << address.m_type << " " << address.m_index_addres.as_value.m_value.m_store << std::endl;
        auto bindex = address.getIndex(m_collection->getMemspace());
        auto it = bindex.beginJoin(1);
        auto item = *it;
        auto [storage_class, value] = item;
        std::cerr << "NEXT " << storage_class << " " << value.m_store << std::endl;
        ++m_iterator;
        return unloadMember<LangToolkit>(fixture, storage_class, value);
    }

}