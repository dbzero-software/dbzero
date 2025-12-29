// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ObjectIterator.hpp"
#include "ObjectIterable.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/core/collections/full_text/FT_Serialization.hpp>
#include <dbzero/core/collections/range_tree/RT_Serialization.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0::object_model

{
    
    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<QueryIterator> &&ft_query_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers,
        const std::vector<FilterFunc> &filters, const SliceDef &slice_def)
        : ObjectIterable(fixture, std::move(ft_query_iterator), type, lang_type, {}, filters)
        , m_iterator_ptr(m_query_iterator.get())
        , m_decoration(std::move(query_observers))
        , m_slice(m_iterator_ptr, slice_def)
    {
    }

    ObjectIterator::ObjectIterator(db0::swine_ptr<Fixture> fixture, std::unique_ptr<SortedIterator> &&sorted_iterator,
        std::shared_ptr<Class> type, TypeObjectPtr lang_type, std::vector<std::unique_ptr<QueryObserver> > &&query_observers, 
        const std::vector<FilterFunc> &filters, const SliceDef &slice_def)
        : ObjectIterable(fixture, std::move(sorted_iterator), type, lang_type, {}, filters)
        , m_iterator_ptr(m_sorted_iterator.get())
        , m_decoration(std::move(query_observers))
        , m_slice(m_iterator_ptr, slice_def)        
    {
    }
    
    ObjectIterator::ObjectIterator(const ObjectIterable &other, const std::vector<FilterFunc> &filters)
        : ObjectIterable(other, filters)
        , m_iterator_ptr(getIteratorPtr())
        , m_decoration(std::move(m_query_observers))
        , m_slice(m_iterator_ptr, m_slice_def)    
    {
    }
    
    ObjectIterator::Decoration::Decoration(std::vector<std::unique_ptr<QueryObserver> > &&query_observers)
        : m_query_observers(std::move(query_observers))
        , m_decorators(m_query_observers.size())
    {
    }
    
    ObjectIterator::ObjectSharedPtr ObjectIterator::next()
    {
        for (;;) {
            if (!m_slice.isEnd()) {
                // Collect decorators if any exist
                if (!m_decoration.empty()) {
                    auto it = m_decoration.m_query_observers.begin();
                    for (auto &decor: m_decoration.m_decorators) {
                        decor = (*it)->getDecoration();
                        ++it;
                    }
                }
                db0::UniqueAddress addr;
                m_slice.next(&addr);
                auto obj_ptr = unload(addr);
                // check filters if any                
                for (auto &filter: m_filters) {
                    if (!filter(obj_ptr.get())) {
                        obj_ptr = nullptr;
                        break;
                    }
                }
                if (obj_ptr.get()) {
                    return obj_ptr;
                }
            } else {
                return nullptr;
            }
        }
    }
    
    std::size_t ObjectIterator::skip(std::size_t count)
    {
        std::size_t skipped = 0;
        while (skipped < count && !m_slice.isEnd()) {
            m_slice.next();
            ++skipped;
        }
        return skipped;
    }

    ObjectIterator::ObjectSharedPtr ObjectIterator::unload(db0::swine_ptr<Fixture> &fixture, Address address) const
    {
        // unload as typed if class is known
        if (m_type && m_lang_type.get()) {
            return LangToolkit::unloadObject(fixture, address, m_type, m_lang_type.get(), m_access_mode);
        } else {
            // Memo type or lang type is missing. We try to resolve both here.
            return LangToolkit::unloadObject(fixture, address, m_class_factory, m_lang_type.get(), 0, m_access_mode);
        }
    }
    
    ObjectIterator::ObjectSharedPtr ObjectIterator::unload(Address address) const
    {
        auto fixture = getFixture();
        return unload(fixture, address);
    }

}
