#include <dbzero/bindings/python/PyToolkit.hpp>
#include "Set.hpp"
#include <dbzero/object_model/iterators/PyObjectIterator.hpp>
#include<memory>

namespace db0::object_model

{

    /**
     * Wraps full-text index iterator to retrieve sequence of well-known type objects
    */
    class SetIterator : public PyObjectIterator<SetIterator, Set>
    {
        public:
            ObjectSharedPtr next() override;
            SetIterator(Set::iterator iterator, Set * ptr);
    };

}