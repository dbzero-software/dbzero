// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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

        bool is_end() const;
        
        void restore() override {
            // does nothing, tuple is immutable
        }

    protected:
        friend class Tuple;
        TupleIterator(Tuple::const_iterator iterator, const Tuple *ptr, ObjectPtr lang_tuple_ptr);
    };
    
}