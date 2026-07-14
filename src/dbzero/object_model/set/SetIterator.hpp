// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include "Set.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/iterators/BaseIterator.hpp>
#include <dbzero/object_model/item/Pair.hpp>

namespace db0::object_model

{
    
    class SetIterator : public BaseIterator<SetIterator, Set>
    {
    public:
        ObjectSharedPtr next() override;        
                
    protected:
        friend class Set;
        SetIterator(Set::const_iterator iterator, const Set * ptr, ObjectPtr lang_set_ptr);
        
        void restore() override;

    private:
        SetIndex m_index;
        SetIndex::joinable_const_iterator m_join_iterator;
        // a reference to the current same-hash array (unless end)
        std::uint64_t m_current_hash = 0;
        o_typed_item m_current_key;
        bool m_is_end = false;
        
        void setJoinIterator();        
    };
    
}