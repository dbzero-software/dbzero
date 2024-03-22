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
        IteratorType m_type;
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

            DictIterator(Dict::iterator iterator, Dict * ptr, IteratorType type = IteratorType::KEYS);

            bool is_end();
    };

    class DictView {

        public:

            DictView(Dict * dict, IteratorType type) : m_collection(dict) , m_type(type){
            }

            DictIterator *begin(void *at_ptr) {
                return new (at_ptr) DictIterator(m_collection->begin(), m_collection, m_type);
            }

            Py_ssize_t size() {
                return m_collection->size();
            }

            static DictView *makeNew(void *at_ptr, Dict * dict , IteratorType type)
            {
                return new (at_ptr) DictView(dict, type);
            }

        private:
            Dict * m_collection;
            IteratorType m_type;
    };

}