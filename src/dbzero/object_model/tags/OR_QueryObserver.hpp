#pragma once

#include "QueryObserver.hpp"
#include <dbzero/core/collections/full_text/FT_ORXIterator.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/object_model/config.hpp>

namespace db0::object_model

{

    // OR-query + observer builder
    class OR_QueryObserverBuilder
    {
    public:
        using LangToolkit = Config::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        OR_QueryObserverBuilder(bool is_exclusive = false);

        // Adds a decorated iterator
        void add(std::unique_ptr<db0::FT_Iterator<std::uint64_t> > &&, ObjectSharedPtr decoration);
        
        // Release query iterator + observer
        std::pair<std::unique_ptr<db0::FT_Iterator<std::uint64_t> >, std::unique_ptr<QueryObserver> > 
        release(int direction = -1, bool lazy_init = false);

    private:
        db0::FT_OR_ORXIteratorFactory<std::uint64_t> m_factory;
        // the mappings for decorations (must be complete)
        std::unordered_map<const void *, ObjectSharedPtr> m_decorations;
    };

    class OR_QueryObserver: public QueryObserver
    {
    public:        
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        ObjectPtr getDecoration() const override;

    protected:
        friend class OR_QueryObserverBuilder;

        OR_QueryObserver(const FT_JoinORXIterator<std::uint64_t> *iterator_ptr,
            std::unordered_map<const void *, ObjectSharedPtr> &&decorations);

    private:
        // the observed iterator
        const FT_JoinORXIterator<std::uint64_t> *m_iterator_ptr;
        // the mappings for decorations (must be complete)
        std::unordered_map<const void *, ObjectSharedPtr> m_decorations;
    };
    
}