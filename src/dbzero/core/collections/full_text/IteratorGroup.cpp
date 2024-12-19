#include <cassert>
#include "IteratorGroup.hpp"

namespace db0

{
    
    template <typename KeyT>
    IteratorGroup<KeyT>::IteratorGroup(std::list<std::unique_ptr<FT_Iterator<KeyT> > > &&iterators)
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
    
    template <typename KeyT>
	IteratorGroup<KeyT>::IteratorGroup(std::unique_ptr<FT_Iterator<KeyT> > &&it_1, std::unique_ptr<FT_Iterator<KeyT> > &&it_2)
        : m_group(2)
    {
        m_group[0].m_iterator = it_1.get();
        m_group[1].m_iterator = it_2.get();
        m_iterators.push_back(std::move(it_1)); 
        m_iterators.push_back(std::move(it_2));
    }

    template <typename KeyT>    
    std::size_t IteratorGroup<KeyT>::size() const {  
        return m_group.size();
    }

    template <typename KeyT>    
    bool IteratorGroup<KeyT>::empty() const {  
        return m_group.empty();
    }

    template <typename KeyT>    
    const typename IteratorGroup<KeyT>::GroupItem &IteratorGroup<KeyT>::front() const {
        return m_group.front();
    }
    
    template <typename KeyT>
    typename IteratorGroup<KeyT>::GroupItem &IteratorGroup<KeyT>::front() {
        return m_group.front();
    }

    template <typename KeyT>
    typename IteratorGroup<KeyT>::iterator IteratorGroup<KeyT>::swapFront(iterator it) {
        assert(it != m_group.begin());
        std::swap(*it, m_group.front());
        return m_group.begin();
    }

    template <typename KeyT>
    bool IteratorGroup<KeyT>::GroupItem::nextKey(int direction, KeyT *buf_ptr)
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
            *buf_ptr = m_iterator->getKey();
        }
        return true;
    }
    
    template <typename KeyT>
    bool IteratorGroup<KeyT>::GroupItem::nextUniqueKey(int direction, KeyT *buf_ptr)
    {
        assert(!m_iterator->isEnd());
        auto last_key = m_iterator->getKey();
        for (;;) {
            if (direction < 0) {
                --(*m_iterator);
            } else {
                ++(*m_iterator);
            }
            if (m_iterator->isEnd()) {
                return false;
            }
            auto next_key = m_iterator->getKey();
            if (next_key != last_key) {
                if (buf_ptr) {
                    *buf_ptr = next_key;
                }
                return true;
            }
        }
        return false;
    }

    template class IteratorGroup<std::uint64_t>;

}