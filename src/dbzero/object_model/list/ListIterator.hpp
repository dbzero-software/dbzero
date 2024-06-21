#include <dbzero/bindings/python/PyToolkit.hpp>
#include "List.hpp"
#include <dbzero/object_model/iterators/PyObjectIterator.hpp>
#include<memory>

namespace db0::object_model

{

    /**
     * Wraps full-text index iterator to retrieve sequence of well-known type objects
    */
    class ListIterator : public PyObjectIterator<ListIterator, List>
    {
    public:
        ObjectSharedPtr next() override;
        ListIterator(List::const_iterator iterator, const List *ptr);
    };

}