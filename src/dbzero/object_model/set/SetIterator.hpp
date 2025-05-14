#pragma once

#include <memory>
#include "Set.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/iterators/BaseIterator.hpp>

namespace db0::object_model

{
    
    class SetIterator : public BaseIterator<SetIterator, Set>
    {
    public:
        ObjectSharedPtr next() override;        

    protected:
        friend class Set;
        SetIterator(Set::const_iterator iterator, const Set * ptr, ObjectPtr lang_set_ptr);

    private:
        SetIndex::joinable_const_iterator m_join_iterator;
        SetIndex m_index;
        void setJoinIterator();
    };
    
}