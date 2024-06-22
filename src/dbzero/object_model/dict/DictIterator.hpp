#pragma once
#include <dbzero/bindings/python/PyToolkit.hpp>
#include "Dict.hpp"
#include <dbzero/object_model/iterators/PyObjectIterator.hpp>
#include<memory>

namespace db0::object_model

{

    enum IteratorType {
        ITEMS = 0,
        VALUES = 1,
        KEYS = 2
    };

    /**
     * Wraps full-text index iterator to retrieve sequence of well-known type objects
    */
    class DictIterator : public PyObjectIterator<DictIterator, Dict>
    {
        DictIndex::joinable_const_iterator m_join_iterator;
        IteratorType m_type;
        DictIndex m_index;
        void setJoinIterator();
        public:

        struct DictItem {

            DictItem(ObjectSharedPtr p_key, ObjectSharedPtr p_value) : key(p_key), value(p_value) {
            }

            ObjectSharedPtr key;
            ObjectSharedPtr value;
        };

        DictItem nextItem();
        ObjectSharedPtr nextValue();
        ObjectSharedPtr nextKey();
        ObjectSharedPtr next() override;

        DictIterator(Dict::const_iterator iterator, const Dict *, IteratorType type = IteratorType::KEYS);
    };

    class DictView 
    {
    public:
        DictView(const Dict *dict, IteratorType type);

        DictIterator *begin(void *at_ptr) const;
        std::size_t size() const;

        static DictView *makeNew(void *at_ptr, const Dict *, IteratorType type);
    
    private:
        const Dict *m_collection;
        IteratorType m_type;
    };

}