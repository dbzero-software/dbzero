// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <cassert>
#include "IteratorGroup.hpp"

namespace db0

{
    
    template <typename KeyT, typename KeyStorageT>
    IteratorGroup<KeyT, KeyStorageT>::IteratorGroup(std::list<std::unique_ptr<FT_IteratorT> > &&iterators)
        : m_iterators(std::move(iterators))
        , m_group(m_iterators.size())
    {
        assert(m_group.size() > 1);
        std::size_t i = 0;
        for (auto &it: m_iterators) {
            m_group[i].m_iterator = it.get();
            ++i;
        }
    }
    
    template <typename KeyT, typename KeyStorageT>
	IteratorGroup<KeyT, KeyStorageT>::IteratorGroup(std::unique_ptr<FT_IteratorT> &&it_1, 
        std::unique_ptr<FT_IteratorT> &&it_2)
        : m_group(2)
    {
        m_group[0].m_iterator = it_1.get();
        m_group[1].m_iterator = it_2.get();
        m_iterators.push_back(std::move(it_1)); 
        m_iterators.push_back(std::move(it_2));
    }

    template <typename KeyT, typename KeyStorageT> 
    std::size_t IteratorGroup<KeyT, KeyStorageT>::size() const {  
        return m_group.size();
    }

    template <typename KeyT, typename KeyStorageT>
    bool IteratorGroup<KeyT, KeyStorageT>::empty() const {  
        return m_group.empty();
    }

    template <typename KeyT, typename KeyStorageT>
    const typename IteratorGroup<KeyT, KeyStorageT>::GroupItem &IteratorGroup<KeyT, KeyStorageT>::front() const {
        return m_group.front();
    }

    template <typename KeyT, typename KeyStorageT>
    typename IteratorGroup<KeyT, KeyStorageT>::GroupItem &IteratorGroup<KeyT, KeyStorageT>::front() {
        return m_group.front();
    }

    template <typename KeyT, typename KeyStorageT>
    typename IteratorGroup<KeyT, KeyStorageT>::iterator IteratorGroup<KeyT, KeyStorageT>::swapFront(iterator it) 
    {
        assert(it != m_group.begin());
        std::swap(*it, m_group.front());
        return m_group.begin();
    }

    template <typename KeyT, typename KeyStorageT>
    bool IteratorGroup<KeyT, KeyStorageT>::GroupItem::nextKey(int direction, KeyStorageT *buf_ptr)
    {
        assert(!m_iterator->isEnd());
        if (direction < 0) {
            --(*m_iterator);
        } else {
            ++(*m_iterator);
        }
        if (m_iterator->isEnd()) {
            return false;
        }
        if (buf_ptr) {
            m_iterator->getKey(*buf_ptr);
        }
        return true;
    }

    template <typename KeyT, typename KeyStorageT>
    bool IteratorGroup<KeyT, KeyStorageT>::GroupItem::nextUniqueKey(int direction, KeyStorageT *buf_ptr)
    {
        assert(!m_iterator->isEnd());
        KeyStorageT next_key;
        m_iterator->getKey(next_key);
        for (;;) {
            if (direction < 0) {
                --(*m_iterator);
            } else {
                ++(*m_iterator);
            }
            if (m_iterator->isEnd()) {
                return false;
            }
            if (m_iterator->swapKey(next_key)) {
                if (buf_ptr) {
                    *buf_ptr = next_key;
                }
                return true;
            }
        }
        return false;
    }

    template class IteratorGroup<UniqueAddress>;
    template class IteratorGroup<std::uint64_t>;
    
    // Cartesian product specific extern template instantiations
    template class IteratorGroup<const UniqueAddress*, CP_Vector<UniqueAddress> >;
    template class IteratorGroup<const std::uint64_t*, CP_Vector<std::uint64_t> >;

}