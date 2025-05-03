#pragma once

#include "ObjectIterable.hpp"

namespace db0::object_model

{

    // SplitIterable splits the results into a tuple of instances
    // possibly from different snapshots (e.g. for diff-comparison)
    class SplitIterable: public ObjectIterable
    {
    public:
        using LangToolkit = Object::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;
        using TypeObjectPtr = LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = LangToolkit::TypeObjectSharedPtr;

        // full-text query iterator (KeyT must be std::uint64_t)
        using QueryIterator = FT_Iterator<UniqueAddress>;
        using SortedIterator = db0::SortedIterator<UniqueAddress>;
        using IteratorFactory = db0::IteratorFactory<UniqueAddress>;
        // a common base for full-text and sorted iterators
        using BaseIterator = db0::FT_IteratorBase;
        using FilterFunc = std::function<bool(ObjectPtr)>;
        
        /**
         * @param split_fixtures - a vector of fixtures to be used for the split
         */
        SplitIterable(db0::swine_ptr<Fixture>, const std::vector<db0::swine_ptr<Fixture> > &split_fixtures, std::unique_ptr<QueryIterator> &&, 
            std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {});
        
        static ObjectIterable &makeNew(void *at_ptr, db0::swine_ptr<Fixture>, const std::vector<db0::swine_ptr<Fixture> > &split_fixtures, 
            std::unique_ptr<QueryIterator> &&, std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, 
            std::vector<std::unique_ptr<QueryObserver> > && = {}, const std::vector<FilterFunc> & = {});
        
        ObjectIterator &makeIter(void *at_ptr, const std::vector<FilterFunc> & = {}) const override;
        
    private:
        const std::vector<db0::swine_ptr<Fixture> > m_split_fixtures;
    };
    
}
