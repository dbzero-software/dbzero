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
        using TypeObjectPtr = LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = LangToolkit::TypeObjectSharedPtr;
        
        // full-text query iterator (KeyT must be std::uint64_t)
        using QueryIterator = FT_Iterator<std::uint64_t>;
        using SortedIterator = db0::SortedIterator<std::uint64_t>;
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;

        // Construct from a full-text query iterator
        TypedObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<QueryIterator> &&ft_query_iterator, 
            std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> &filters = {});
        
        // Construct from a sorted iterator
        TypedObjectIterator(db0::swine_ptr<Fixture>, std::unique_ptr<SortedIterator> &&sorted_iterator,
            std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > && = {}, 
            const std::vector<FilterFunc> &filters = {});
        
        // Clone with additional filters
        std::unique_ptr<ObjectIterator> iter(const std::vector<FilterFunc> & = {}) const override;
        std::unique_ptr<TypedObjectIterator> iterTyped(const std::vector<FilterFunc> & = {}) const;

        // Create with user provided arguments
        std::unique_ptr<TypedObjectIterator> makeTypedIter(std::unique_ptr<QueryIterator> &&ft_query_iterator,
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {}) const;
        std::unique_ptr<TypedObjectIterator> makeTypedIter(std::unique_ptr<SortedIterator> &&sorted_iterator,
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {}) const;
        
    protected:
        // iter constructor
        TypedObjectIterator(db0::swine_ptr<Fixture>, const ClassFactory &, std::shared_ptr<Class>, TypeObjectPtr,
            std::unique_ptr<QueryIterator> &&, std::unique_ptr<SortedIterator> &&, std::shared_ptr<IteratorFactory>,
            std::vector<std::unique_ptr<QueryObserver> > &&, std::vector<FilterFunc> &&filters);

        ObjectSharedPtr unload(std::uint64_t address) const override;
        
    private:
        std::shared_ptr<Class> m_type;        
    };

}
