#include "TupleIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>
namespace db0::object_model

{
    TupleIterator::TupleIterator(Tuple::iterator iterator, Tuple * ptr) 
        : PyObjectIterator<TupleIterator, Tuple>(iterator, ptr) 
    {
    }

    TupleIterator::ObjectSharedPtr TupleIterator::next() {
        auto [storage_class, value] = *m_iterator;
        ++m_iterator;
        auto fixture = m_collection->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value);
    }
    
    bool TupleIterator::is_end() {
        return m_iterator == m_collection->getData()->end();
    }
    
}