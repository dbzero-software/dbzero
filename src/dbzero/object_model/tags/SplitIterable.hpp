// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ObjectIterable.hpp"
#include <vector>

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
        SplitIterable(db0::swine_ptr<Fixture>, std::vector<db0::swine_ptr<Fixture> > &split_fixtures, std::unique_ptr<QueryIterator> &&, 
            std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {});
                
        SplitIterable(db0::swine_ptr<Fixture>, std::vector<db0::swine_ptr<Fixture> > &split_fixtures, std::unique_ptr<SortedIterator> &&,
            std::shared_ptr<Class> = nullptr, TypeObjectPtr lang_type = nullptr, std::vector<std::unique_ptr<QueryObserver> > && = {},
            const std::vector<FilterFunc> & = {});
        
        virtual ~SplitIterable();
        
        std::shared_ptr<ObjectIterator> iter() const override;
        
    protected:
        friend class SplitIterator;
        mutable std::vector<db0::weak_swine_ptr<Fixture> > m_split_fixtures;
        
        // iter constructor
        SplitIterable(db0::swine_ptr<Fixture>, std::vector<db0::swine_ptr<Fixture> > &split_fixtures,
            const ClassFactory &, std::unique_ptr<QueryIterator> &&, std::unique_ptr<SortedIterator> &&, std::shared_ptr<IteratorFactory>, 
            std::vector<std::unique_ptr<QueryObserver> > &&, std::vector<FilterFunc> &&filters, std::shared_ptr<Class>, 
            TypeObjectPtr lang_type, const SliceDef & = {});

        void init(std::vector<db0::swine_ptr<Fixture> > &split_fixtures);
    };
    
}
