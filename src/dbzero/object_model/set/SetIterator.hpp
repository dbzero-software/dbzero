#pragma once
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
            SetIndex::joinable_const_iterator m_join_iterator;
            SetIndex m_index;
            void getJoinIterator();
        public:
            ObjectSharedPtr next() override;
            SetIterator(Set::iterator iterator, Set * ptr);
    };

}