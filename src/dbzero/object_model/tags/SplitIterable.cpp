// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SplitIterable.hpp"
#include "SplitIterator.hpp"
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0::object_model

{
    
    SplitIterable::SplitIterable(db0::swine_ptr<Fixture> fixture, std::vector<db0::swine_ptr<Fixture> > &split_fixtures,
        std::unique_ptr<QueryIterator> &&query, std::shared_ptr<Class> type, TypeObjectPtr lang_type, 
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
        : ObjectIterable(fixture, std::move(query), type, lang_type, std::move(query_observers), filters) 
    {
        init(split_fixtures);
    }    

    SplitIterable::SplitIterable(db0::swine_ptr<Fixture> fixture, std::vector<db0::swine_ptr<Fixture> > &split_fixtures,
        std::unique_ptr<SortedIterator> &&sorted_query, std::shared_ptr<Class> type, TypeObjectPtr lang_type, 
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters)
        : ObjectIterable(fixture, std::move(sorted_query), type, lang_type, std::move(query_observers), filters)        
    {
        init(split_fixtures);
    }
    
    SplitIterable::SplitIterable(db0::swine_ptr<Fixture> fixture, std::vector<db0::swine_ptr<Fixture> > &split_fixtures,
        const ClassFactory &class_factory, std::unique_ptr<QueryIterator> &&query, std::unique_ptr<SortedIterator> &&sorted_query, 
        std::shared_ptr<IteratorFactory> factory, std::vector<std::unique_ptr<QueryObserver> > &&query_observers, 
        std::vector<FilterFunc> &&filters, std::shared_ptr<Class> type, TypeObjectPtr lang_type, const SliceDef &slice_def)
        : ObjectIterable(fixture, class_factory, std::move(query), std::move(sorted_query), std::move(factory),
            std::move(query_observers), std::move(filters), type, lang_type, slice_def)
    {
        init(split_fixtures);
    }
    
    SplitIterable::~SplitIterable()
    {
    }
    
    void SplitIterable::init(std::vector<db0::swine_ptr<Fixture> > &split_fixtures)
    {
        for (auto &fixture : split_fixtures) {
            m_split_fixtures.emplace_back(fixture);
        }
    }
    
    std::shared_ptr<ObjectIterator> SplitIterable::iter() const {
        return std::make_shared<SplitIterator>(*this);
    }
    
}