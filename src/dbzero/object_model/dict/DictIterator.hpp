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
        DictIterator(Dict::const_iterator iterator, const Dict *, ObjectPtr lang_dict_ptr,
            IteratorType type = IteratorType::KEYS);

        struct DictItem
        {
            DictItem(ObjectPtr p_key, ObjectPtr p_value) 
                : key(p_key)
                , value(p_value) 
            {
            }

            ObjectSharedPtr key;
            ObjectSharedPtr value;
        };

        ObjectSharedPtr next() override;

        DictItem nextItem();
        ObjectSharedPtr nextValue();
        ObjectSharedPtr nextKey();
        
    private:
        DictIndex::joinable_const_iterator m_join_iterator;
        IteratorType m_type = IteratorType::KEYS;
        DictIndex m_index;
        
        void setJoinIterator();
        // advance iterator's position
        void iterNext();
    };
    
}