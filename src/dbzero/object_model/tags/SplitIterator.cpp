#include "SplitIterator.hpp"

namespace db0::object_model

{

    SplitIterator::SplitIterator(db0::swine_ptr<Fixture> fixture, std::vector<db0::swine_ptr<Fixture> > &split_fixtures, 
        std::unique_ptr<QueryIterator> &&query, std::shared_ptr<Class> type, TypeObjectPtr lang_type, 
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters,
        const SliceDef &slice_def)
        : ObjectIterator(fixture, std::move(query), type, lang_type, std::move(query_observers), filters, slice_def)
    {
        init(split_fixtures);
    }
    
    SplitIterator::SplitIterator(db0::swine_ptr<Fixture> fixture, std::vector<db0::swine_ptr<Fixture> > &split_fixtures,
        std::unique_ptr<SortedIterator> &&sorted_query, std::shared_ptr<Class> type, TypeObjectPtr lang_type, 
        std::vector<std::unique_ptr<QueryObserver> > &&query_observers, const std::vector<FilterFunc> &filters, 
        const SliceDef &slice_def)
        : ObjectIterator(fixture, std::move(sorted_query), type, lang_type, std::move(query_observers), filters, slice_def)
    {
        init(split_fixtures);
    }
    
    void SplitIterator::init(std::vector<db0::swine_ptr<Fixture> > &split_fixtures)
    {
        for (auto &fixture : split_fixtures) {
            m_split_fixtures.emplace_back(fixture);
        }
    }

    ObjectIterator &SplitIterator::makeIter(void *at_ptr, const std::vector<FilterFunc> &filters) const
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
    
    SplitIterator::ObjectSharedPtr SplitIterator::unload(Address address) const
    {
        if (m_temp.size() < m_split_fixtures.size()) {
            m_temp.resize(m_split_fixtures.size());
        }
        for (std::size_t i = 0; i < m_split_fixtures.size(); ++i) {
            auto fixture = m_split_fixtures[i].lock();
            if (!fixture) {
                THROWF(db0::InputException) << "ObjectIterator is no longer accessible (prefix or snapshot closed)" << THROWF_END;
            }
            m_temp[i] = ObjectIterator::unload(fixture, address);
        }
        
        return LangToolkit::makeTuple(m_temp);
    }

}
