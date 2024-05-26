#pragma once

#include "ObjectIterator.hpp"
#include <dbzero/object_model/class/Class.hpp>

namespace db0::object_model

{
    
    using Class = db0::object_model::Class;
    using Object = db0::object_model::Object;

    /**
     * ObjectIterator for a well-known type objects
    */
    class TypedObjectIterator: public ObjectIterator
    {
    public:
        using LangToolkit = Object::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using TypeObjectPtr = LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = LangToolkit::TypeObjectSharedPtr;
        
        // full-text query iterator (KeyT must be std::uint64_t)
        using QueryIterator = FT_Iterator<std::uint64_t>;
        using SortedIterator = db0::SortedIterator<std::uint64_t>;
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;

        // Construct from a full-text query iterator
        TypedObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&ft_query_iterator, 
            std::shared_ptr<Class> type, std::vector<std::unique_ptr<QueryObserver> > && = {});
        
        // Construct from a sorted iterator
        TypedObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&sorted_iterator, 
            std::shared_ptr<Class> type, std::vector<std::unique_ptr<QueryObserver> > && = {});
        
        /**
         * Start over iteration, place iterator object in the given memory location
        */
        TypedObjectIterator *iter(void *at_ptr, std::unique_ptr<QueryIterator> &&ft_query_iterator,
            std::shared_ptr<Class> type) const;
        
        ObjectPtr unload(std::uint64_t address) const override;
        
        static TypedObjectIterator *makeNew(void *at_ptr, db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&,
            std::shared_ptr<Class> type, std::vector<std::unique_ptr<QueryObserver> > && = {});
        
        /**
         * Retrieves associated language specific class (raw pointer)
        */
        TypeObjectSharedPtr getLangClass() const;
                
        std::shared_ptr<Class> getType() const;

    private:
        std::shared_ptr<Class> m_type;
    };

}
