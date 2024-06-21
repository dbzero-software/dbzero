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
        using IteratorT = typename CollectionT::const_iterator;
        virtual ObjectSharedPtr next() = 0;
        
        static PyObjectIterator *makeNew(void *at_ptr, IteratorT iterator, const CollectionT *ptr) {
            return new (at_ptr) ClassT(iterator, ptr);
        }

        PyObjectIterator(IteratorT iterator, const CollectionT *ptr)
            : m_iterator(iterator)
            , m_collection(ptr) 
        {
        }
        
        inline bool operator==(const ThisType &other) const {
            return m_iterator == other.m_iterator;
        }

        inline bool operator!=(const ThisType &other) const {
            return !(m_iterator == other.m_iterator);
        }

        ClassT end() const {
            return ClassT(m_collection->end(), m_collection);
        }

    protected:
        IteratorT m_iterator;
        const CollectionT *m_collection;
    };

}