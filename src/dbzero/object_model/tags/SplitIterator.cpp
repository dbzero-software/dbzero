// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SplitIterator.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>

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
    
    SplitIterator::~SplitIterator()
    {
    }
    
    void SplitIterator::init(std::vector<db0::swine_ptr<Fixture> > &split_fixtures)
    {
        for (auto &fixture : split_fixtures) {
            m_split_fixtures.emplace_back(fixture);
            m_class_factories.emplace_back(&getClassFactory(*fixture));
        }
    }
    
    SplitIterator::SplitIterator(const SplitIterable &other)
        : ObjectIterator(other)
        , m_split_fixtures(other.m_split_fixtures)        
    {
        for (auto &weak_fixture : m_split_fixtures) {
            auto fixture = weak_fixture.lock();
            if (!fixture) {
                THROWF(db0::InputException)
                    << "ObjectIterator is no longer accessible (prefix or snapshot closed)" << THROWF_END;
            }
            m_class_factories.emplace_back(&getClassFactory(*fixture));
        }
    }
    
    SplitIterator::ObjectSharedPtr SplitIterator::unload(Address address) const
    {           
        if (m_temp.size() != m_split_fixtures.size()) {
            m_temp.resize(m_split_fixtures.size());
        }        
        
        for (std::size_t i = 0; i < m_split_fixtures.size(); ++i) {
            auto fixture = m_split_fixtures[i].lock();
            if (!fixture) {
                THROWF(db0::InputException)
                    << "ObjectIterator is no longer accessible (prefix or snapshot closed)" << THROWF_END;
            }
            
            auto &class_factory = *m_class_factories[i];
            // NOTE: unload using fixture-specific class factory
            m_temp[i] = LangToolkit::unloadObject(fixture, address, class_factory, m_lang_type.get());
        }
        
        return LangToolkit::makeTuple(std::move(m_temp));
    }
    
}
