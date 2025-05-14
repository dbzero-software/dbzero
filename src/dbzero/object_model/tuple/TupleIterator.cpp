#include "TupleIterator.hpp"
#include <dbzero/object_model/value/Member.hpp>

namespace db0::object_model

{

    TupleIterator::TupleIterator(Tuple::const_iterator iterator, const Tuple *ptr, ObjectPtr lang_tuple_ptr)
        : BaseIterator<TupleIterator, Tuple>(iterator, ptr, lang_tuple_ptr)
    {
    }
    
    TupleIterator::ObjectSharedPtr TupleIterator::next()
    {
        auto [storage_class, value] = *m_iterator;
        ++m_iterator;
        auto fixture = m_collection->getFixture();
        return unloadMember<LangToolkit>(fixture, storage_class, value);
    }
    
    bool TupleIterator::is_end() const {
        return m_iterator == m_collection->getData()->items().end();
    }
    
}