// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "OR_QueryObserver.hpp"
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0::object_model

{
    
    OR_QueryObserverBuilder::OR_QueryObserverBuilder(bool is_exclusive)
        : m_factory(is_exclusive)        
    {
    }
    
    void OR_QueryObserverBuilder::add(std::unique_ptr<db0::FT_Iterator<UniqueAddress> > &&query, 
        ObjectSharedPtr decoration)
    {
        if (query) {
            m_decorations[query->getUID()] = decoration;
        }
        m_factory.add(std::move(query));
    }
    
    std::pair<std::unique_ptr<db0::FT_Iterator<UniqueAddress> >, std::unique_ptr<QueryObserver> >
    OR_QueryObserverBuilder::release(int direction, bool lazy_init)
    {        
        FT_JoinORXIterator<UniqueAddress> *or_iterator_ptr;
        auto iterator = m_factory.releaseSpecial(direction, or_iterator_ptr, lazy_init);
        auto query_observer = std::unique_ptr<OR_QueryObserver>(new OR_QueryObserver(or_iterator_ptr, std::move(m_decorations)));
        return std::make_pair(std::move(iterator), std::move(query_observer));
    }
    
    OR_QueryObserver::OR_QueryObserver(const FT_JoinORXIterator<UniqueAddress> *iterator_ptr,
        std::unordered_map<std::uint64_t, ObjectSharedPtr> &&decorations)
        : m_iterator_ptr(iterator_ptr)
        , m_decorations(std::move(decorations))
    {        
    }
    
    OR_QueryObserver::OR_QueryObserver(const FT_JoinORXIterator<UniqueAddress> *iterator_ptr,
        const std::unordered_map<std::uint64_t, ObjectSharedPtr> &decorations)
        : m_iterator_ptr(iterator_ptr)
        , m_decorations(decorations)
    {    
    }
    
    OR_QueryObserver::ObjectPtr OR_QueryObserver::getDecoration() const
    {        
        assert(m_iterator_ptr && "Split query iterator not set");
        auto it = m_decorations.find(m_iterator_ptr->getInnerUID());
        if (it == m_decorations.end()) {
            assert(false);
            THROWF(db0::InternalException) << "Split query decoration not found";            
        }
        return it->second.get();
    }
    
    std::unique_ptr<QueryObserver> OR_QueryObserver::rebase(const FT_IteratorBase &new_base) const
    {
        return std::unique_ptr<QueryObserver>(new OR_QueryObserver(
            reinterpret_cast<const FT_JoinORXIterator<UniqueAddress> *>(new_base.find(m_iterator_ptr->getUID())),
            m_decorations)
        );
    }
    
}