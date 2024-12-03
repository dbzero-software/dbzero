#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/workspace/Config.hpp>
#include <memory>

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
        using LangToolkit = db0::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;
        using IteratorT = typename CollectionT::const_iterator;
        virtual ObjectSharedPtr next() = 0;
        
        static PyObjectIterator *makeNew(void *at_ptr, IteratorT iterator, const CollectionT *ptr,
            ObjectPtr lang_collection_ptr)
        {        
            return new (at_ptr) ClassT(iterator, ptr, lang_collection_ptr);
        }
        
        PyObjectIterator(IteratorT iterator, const CollectionT *ptr, ObjectPtr lang_collection_ptr)
            : m_iterator(iterator)
            , m_collection(ptr)
            , m_lang_collection_shared_ptr(lang_collection_ptr)
        {
        }
        
        inline bool operator==(const ThisType &other) const {
            return m_iterator == other.m_iterator;
        }

        inline bool operator!=(const ThisType &other) const {
            return !(m_iterator == other.m_iterator);
        }

        bool is_end() const {
            return m_iterator.is_end();
        }

    protected:
        IteratorT m_iterator;
        const CollectionT *m_collection;
        // reference to persist collections' related language specific object
        ObjectSharedPtr m_lang_collection_shared_ptr;
    };

}