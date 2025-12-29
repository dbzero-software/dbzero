// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "QueryObserver.hpp"
#include <dbzero/core/collections/full_text/FT_ORXIterator.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{

    // OR-query + observer builder
    class OR_QueryObserverBuilder
    {
    public:
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        OR_QueryObserverBuilder(bool is_exclusive = false);

        // Adds a decorated iterator
        void add(std::unique_ptr<db0::FT_Iterator<UniqueAddress> > &&, ObjectSharedPtr decoration);
        
        // Release query iterator + observer
        std::pair<std::unique_ptr<db0::FT_Iterator<UniqueAddress> >, std::unique_ptr<QueryObserver> > 
        release(int direction = -1, bool lazy_init = false);

    private:
        db0::FT_OR_ORXIteratorFactory<UniqueAddress> m_factory;
        // the mappings for decorations (must be complete)
        std::unordered_map<std::uint64_t, ObjectSharedPtr> m_decorations;
    };
    
    class OR_QueryObserver: public QueryObserver
    {
    public:        
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        ObjectPtr getDecoration() const override;
        std::unique_ptr<QueryObserver> rebase(const FT_IteratorBase &) const override;

    protected:
        friend class OR_QueryObserverBuilder;

        OR_QueryObserver(const FT_JoinORXIterator<UniqueAddress> *iterator_ptr,
            std::unordered_map<std::uint64_t, ObjectSharedPtr> &&decorations);
        
        OR_QueryObserver(const FT_JoinORXIterator<UniqueAddress> *iterator_ptr,
            const std::unordered_map<std::uint64_t, ObjectSharedPtr> &decorations);

    private:
        // the observed iterator
        const FT_JoinORXIterator<UniqueAddress> *m_iterator_ptr;
        // the mappings for decorations (must be complete)
        std::unordered_map<std::uint64_t, ObjectSharedPtr> m_decorations;
    };
    
}