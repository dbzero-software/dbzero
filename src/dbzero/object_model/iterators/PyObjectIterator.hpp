#include <dbzero/bindings/python/PyToolkit.hpp>
#include<memory>

namespace db0::object_model

{

    /**
     * Wraps full-text index iterator to retrieve sequence of well-known type objects
    */
   
    template<typename ClassT, typename CollectionT>
    class PyObjectIterator
    {
        public:
            using ThisType = PyObjectIterator<ClassT, CollectionT>;
            using LangToolkit = db0::python::PyToolkit;
            using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
            using IteratorT = typename CollectionT::iterator;
            virtual ObjectSharedPtr next() = 0;
            static PyObjectIterator *makeNew(void *at_ptr, IteratorT iterator, CollectionT * ptr){
                return new (at_ptr) ClassT(iterator, ptr);
            }

            PyObjectIterator(IteratorT iterator, CollectionT * ptr) 
                : m_iterator(iterator), m_collection(ptr) {
                }
            

            inline bool operator==(const ThisType &other) const {
                return m_iterator == other.m_iterator;
            }

            inline bool operator!=(const ThisType &other) const {
                return !(m_iterator == other.m_iterator);
            }

            ClassT end() {
                return ClassT(m_collection->end(), m_collection);
            }

        protected:

            IteratorT m_iterator;
            CollectionT *m_collection;
    };

}