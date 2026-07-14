// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
                
        // try restoring the iterator after the related collection is modified
        // NOTE: may render the iterator as end
        void restore() override;

    protected:
        friend class List;

        // NOTE: list iterator is always created from index = 0
        ListIterator(List::const_iterator iterator, const List *ptr, ObjectPtr lang_list_ptr);

    private:
        // index required to refresh the iterator after related collection is modified
        std::uint64_t m_index = 0;
    };
    
}