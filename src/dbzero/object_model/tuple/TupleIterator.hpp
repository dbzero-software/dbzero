#pragma once

#include <memory>
#include "Tuple.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/iterators/BaseIterator.hpp>


namespace db0::object_model

{

    class TupleIterator : public BaseIterator<TupleIterator, Tuple>
    {
    public:
        ObjectSharedPtr next() override;
        TupleIterator(Tuple::const_iterator iterator, const Tuple *ptr, ObjectPtr lang_tuple_ptr);

        bool is_end() const;
    };
    
}