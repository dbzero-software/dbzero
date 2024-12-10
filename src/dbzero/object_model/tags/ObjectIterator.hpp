#pragma once

#include <memory>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/core/collections/full_text/SortedIterator.hpp>
#include <dbzero/core/collections/full_text/IteratorFactory.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include "QueryObserver.hpp"
#include "ObjectIterable.hpp"

namespace db0::object_model

{
    
    class ClassFactory;
    class QueryObserver;
    using Object = db0::object_model::Object;
    using Serializable = db0::Serializable;

    /**
     * Full-text query result iterator over unspecified type objects
     * all objects must be from the same prefix
    */
    class ObjectIterator: public ObjectIterable
    {
    public:
        // Construct from a full-text query iterator
        // NOTE: lang_type is required here because results may need to be accessed as MemoBase
        // even if their exact type is unknown (this is for the model-less access to data)
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&, std::shared_ptr<Class> = nullptr,
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {});
        
        // Construct from a sorted iterator
        ObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&, std::shared_ptr<Class> = nullptr,
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {});
        
        // Construct from IteratorFactory (specialized on first use)
        ObjectIterator(db0::swine_ptr<Fixture>, std::shared_ptr<IteratorFactory> factory, std::shared_ptr<Class> = nullptr, 
            TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> & = {});
                
        /**
         * Retrieve next object from the iterator         
         * @return nullptr if end of iteration reached
        */
        ObjectSharedPtr next();
        
    protected:
        friend class ObjectIterable;
        std::unique_ptr<BaseIterator> m_base_iterator;
        // iterator_ptr valid both in case of m_query_iterator and m_sorted_iterator
        BaseIterator *m_iterator_ptr = nullptr;       
        bool m_initialized = false;

        // iter constructor
        ObjectIterator(db0::swine_ptr<Fixture>, const ClassFactory &, std::unique_ptr<QueryIterator> &&,
            std::unique_ptr<SortedIterator> &&, std::shared_ptr<IteratorFactory>, std::vector<std::unique_ptr<QueryObserver> > &&,
            std::vector<FilterFunc> &&filters, std::shared_ptr<Class>, TypeObjectPtr lang_type);
        
        void assureInitialized();
        void assureInitialized() const;

        /**
         * Unload object by address (must be from this iterator)
        */
        virtual ObjectSharedPtr unload(std::uint64_t address) const;
    };
    
}