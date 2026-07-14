// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include "Dict.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/iterators/BaseIterator.hpp>

namespace db0::object_model

{

    enum IteratorType {
        ITEMS = 0,
        VALUES = 1,
        KEYS = 2
    };
    
    class DictIterator : public BaseIterator<DictIterator, Dict>
    {
    public:

        struct DictItem
        {
            ObjectSharedPtr key;
            ObjectSharedPtr value;
        };
        
        ObjectSharedPtr next() override;

        DictItem nextItem();
        ObjectSharedPtr nextValue();
        ObjectSharedPtr nextKey();
        
        // Restore the iterator after related collection was modified
        void restore() override;

    protected: 
        friend class Dict;
        friend class DictView;
        DictIterator(Dict::const_iterator iterator, const Dict *, ObjectPtr lang_dict_ptr,
            IteratorType type = IteratorType::KEYS);
        
    private:
        // the currently iterated-over same-hash array
        DictIndex m_index;
        DictIndex::joinable_const_iterator m_join_iterator;
        IteratorType m_type = IteratorType::KEYS;
        // a reference to the current same-hash array (unless end)
        std::uint64_t m_current_hash = 0;
        o_pair_item m_current_key;
        bool m_is_end = false;
        
        void setJoinIterator();
        // advance iterator's position
        void iterNext();
    };
    
}