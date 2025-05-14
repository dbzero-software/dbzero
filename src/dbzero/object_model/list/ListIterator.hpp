#pragma once

#include <memory>
#include "List.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/iterators/BaseIterator.hpp>


namespace db0::object_model

{

    class ListIterator : public BaseIterator<ListIterator, List>
    {
    public:
        ObjectSharedPtr next() override;
        
    protected:
        friend class List;
        ListIterator(List::const_iterator iterator, const List *ptr, ObjectPtr lang_list_ptr);
    };
    
}