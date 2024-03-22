#include <dbzero/bindings/python/PyToolkit.hpp>
#include "Tuple.hpp"
#include <dbzero/object_model/iterators/PyObjectIterator.hpp>
#include<memory>

namespace db0::object_model

{

    /**
     * Wraps full-text index iterator to retrieve sequence of well-known type objects
    */
    class TupleIterator : public PyObjectIterator<TupleIterator, Tuple>
    {
        public:
            ObjectSharedPtr next() override;
            TupleIterator(Tuple::iterator iterator, Tuple * ptr);

            bool is_end();
    };

}