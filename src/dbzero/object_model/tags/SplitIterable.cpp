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

    void SplitIterable::init(std::vector<db0::swine_ptr<Fixture> > &split_fixtures)
    {
        for (auto &fixture : split_fixtures) {
            m_split_fixtures.emplace_back(fixture);
        }
    }
    
    SplitIterable &SplitIterable::makeNew(void *at_ptr, db0::swine_ptr<Fixture> fixture,
        std::vector<db0::swine_ptr<Fixture> > &split_fixtures, 
        std::unique_ptr<QueryIterator> &&query, std::shared_ptr<Class> type, TypeObjectPtr lang_type,
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters)
    {
        return *new(at_ptr) SplitIterable(fixture, split_fixtures, std::move(query), type, lang_type,
            std::move(query_observers), filters);
    }
    
    ObjectIterator &SplitIterable::makeIter(void *at_ptr, const std::vector<FilterFunc> &filters) const
    {
        auto fixture = getFixture();
        std::vector<FilterFunc> new_filters(this->m_filters);
        new_filters.insert(new_filters.end(), filters.begin(), filters.end());
        
        std::vector<db0::swine_ptr<Fixture> > locked_fixtures;
        for (auto &weak_fixture : m_split_fixtures) {
            auto fixture = weak_fixture.lock();
            if (!fixture) {
                THROWF(db0::InputException) << "ObjectIterator is no longer accessible (prefix or snapshot closed)" << THROWF_END;
            }
            locked_fixtures.emplace_back(fixture);
        }

        // no query observers for sorted iterator
        if (m_sorted_iterator) {
            auto sorted_iterator = beginSorted();
            return *new (at_ptr) SplitIterator(fixture, locked_fixtures, std::move(sorted_iterator), m_type, 
                m_lang_type.get(), {}, new_filters, m_slice_def);
        }
        
        std::unique_ptr<QueryIterator> query_iterator;
        // note that query observers are not collected for the sorted iterator
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        if (m_query_iterator || m_factory) {
            query_iterator = beginFTQuery(query_observers, -1);
        }
        // query observers are not collected for the sorted iterator       
        return *new (at_ptr) SplitIterator(fixture, locked_fixtures, std::move(query_iterator), m_type, m_lang_type.get(),
            std::move(query_observers), new_filters, m_slice_def);
    }
    
}