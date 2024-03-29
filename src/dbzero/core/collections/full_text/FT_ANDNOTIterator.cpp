#include "FT_ANDNOTIterator.hpp"
#include "FT_ORXIterator.hpp"
#include <dbzero/core/utils/heap_utils.hpp>

namespace db0 

{

    template<typename key_t>
    void FT_ANDNOTIterator<key_t>::updateWithHeap() 
    {
        if (m_direction > 0) {
            std::make_heap(m_subtrahends_heap.begin(), m_subtrahends_heap.end(), ForwardHeapCompare());
            if (!inResult(getBaseIterator().getKey(), 1)) {
                next(1);
            }
        } else {
            std::make_heap(m_subtrahends_heap.begin(), m_subtrahends_heap.end(), BackwardHeapCompare());
            if (!inResult(getBaseIterator().getKey(), -1)) {
                next(-1);
            }
        }
    }

    template<typename key_t>
    bool FT_ANDNOTIterator<key_t>::inResult(const key_t &key, int direction) 
    {
        if (direction > 0) {
            while(!m_subtrahends_heap.empty()) {
                HeapItem &item = m_subtrahends_heap.front();
                if(item.key == key) {
                    return false;
                } else if(item.key > key) {
                    break;
                }
                if(item.it->join(key, 1)) {
                    item.key = item.it->getKey();
                    update_heap_front(m_subtrahends_heap.begin(), m_subtrahends_heap.end(), ForwardHeapCompare());
                } else {
                    std::pop_heap(m_subtrahends_heap.begin(), m_subtrahends_heap.end(), ForwardHeapCompare());
                    m_subtrahends_heap.pop_back();
                }
            }
            return true;
        } else {
            assert(m_direction < 0);
            while(!m_subtrahends_heap.empty()) {
                HeapItem &item = m_subtrahends_heap.front();
                if(item.key == key) {
                    return false;
                } else if(item.key < key) {
                    break;
                }
                if(item.it->join(key, -1)) {
                    item.key = item.it->getKey();
                    update_heap_front(m_subtrahends_heap.begin(), m_subtrahends_heap.end(), BackwardHeapCompare());
                } else {
                    std::pop_heap(m_subtrahends_heap.begin(), m_subtrahends_heap.end(), BackwardHeapCompare());
                    m_subtrahends_heap.pop_back();
                }
            }
            return true;
        }
    }
    
    template<typename key_t>
    bool FT_ANDNOTIterator<key_t>::next(int direction, void *buf)
    {
        assert(!this->isEnd());
        if (buf) {
            auto key = getBaseIterator().getKey();
            std::memcpy(buf, &key, sizeof(key));
        }        
        if (direction > 0) {
            auto &it = getBaseIterator();
            bool is_not_end;
            do {
                ++it;
                is_not_end = !it.isEnd();
            } while(is_not_end && !inResult(it.getKey(), 1));
            return is_not_end;
        } else {
            assert(direction < 0);
            auto &it = getBaseIterator();
            bool is_not_end;
            do {
                --it;
                is_not_end = !it.isEnd();
            } while(is_not_end && !inResult(it.getKey(), -1));
            return is_not_end;
        }
    }

    template<typename key_t>
    void FT_ANDNOTIterator<key_t>::next(void *buf)
    {
        this->next(-1, buf);
    }
    
    template<typename key_t>
    FT_ANDNOTIterator<key_t>::FT_ANDNOTIterator(std::vector<std::unique_ptr<FT_Iterator<key_t>>> &&inner_iterators,
        int direction, bool lazy_init)        
        : m_direction(direction)
        , m_joinable(std::move(inner_iterators))
    {
        if (m_joinable.empty()) {
            THROWF(db0::InputException) << "Needed at least 1 inner-iterator";
        }

        // defer initialization if lazy init requested
        if (!lazy_init) {
            if (getBaseIterator().isEnd()) {
                // Query is empty. We can stop here.
                return;
            }

            m_subtrahends_heap.reserve(m_joinable.size() - 1);
            for (auto it = std::next(m_joinable.begin()); it != m_joinable.end(); ++it) {
                auto &joined = **it;
                if (!joined.isEnd()) {
                    HeapItem &item = m_subtrahends_heap.emplace_back();
                    item.it = &joined;
                    item.key = joined.getKey();
                }
            }
            updateWithHeap();
        }
    }

    template<typename key_t>
    FT_Iterator<key_t>& FT_ANDNOTIterator<key_t>::getBaseIterator() {
        return *m_joinable.front();
    }

    template<typename key_t>
    const FT_Iterator<key_t>& FT_ANDNOTIterator<key_t>::getBaseIterator() const {
        return *m_joinable.front();
    }

    template<typename key_t>
    std::ostream& FT_ANDNOTIterator<key_t>::dump(std::ostream &os) const 
    {
        os << "ANDNOT@" << this << '[';
        auto it = m_joinable.begin();
        for(auto end = --m_joinable.end(); it != end; ++it) {
            (*it)->dump(os);
            os << ',';
        }
        (*it)->dump(os);
        os << ']';
        return os;
    }

    template<typename key_t>
    bool FT_ANDNOTIterator<key_t>::equal(const FT_IteratorBase &other) const 
    {
        if (this->typeId() != other.typeId()) {
            return false;
        }
        const FT_ANDNOTIterator &andnot_it = static_cast<const FT_ANDNOTIterator&>(other);
        if (m_joinable.size() != andnot_it.m_joinable.size()) {
            return false;
        }
        
        auto this_joinable_it = m_joinable.begin();
        auto other_joinable_it = andnot_it.m_joinable.begin();
        // first iterators must be the same
        if (!(*this_joinable_it)->equal(**other_joinable_it)) {
            return false;
        }
        ++this_joinable_it;
        ++other_joinable_it;
        // compare rest of iterators (order doesn't matter)
        if(std::is_permutation(this_joinable_it, m_joinable.end(),
                               other_joinable_it, andnot_it.m_joinable.end(),
            [](const std::unique_ptr<FT_Iterator<key_t>> &a, const std::unique_ptr<FT_Iterator<key_t>> &b) {
                return a->equal(*b);
            }))
        {
            return true;
        } else {
            return false;
        }
    }

    template<typename key_t>
    const FT_IteratorBase* FT_ANDNOTIterator<key_t>::find(const FT_IteratorBase &it) const 
    {
        if (this->equal(it)) {
            return this;
        }
        for (const std::unique_ptr<FT_Iterator<key_t>> &sub_it : m_joinable) {
            const FT_IteratorBase *found_it = sub_it->find(it);
            if(found_it) {
                return found_it;
            }
        }
        return nullptr;
    }

    template<typename key_t>
    key_t FT_ANDNOTIterator<key_t>::getKey() const {
        return getBaseIterator().getKey();
    }

    template<typename key_t>
    bool FT_ANDNOTIterator<key_t>::isEnd() const {
        return getBaseIterator().isEnd();
    }

    template<typename key_t>
    void FT_ANDNOTIterator<key_t>::visit(IteratorVisitor& visitor) const 
    {
        visitor.enterAndNotIterator(this->getID(), m_direction);
        visitor.setLabel(this->getLabel());
        for(const std::unique_ptr<FT_Iterator<key_t>> &sub_it : m_joinable) {
            sub_it->visit(visitor);
        }
        visitor.leaveIterator();
    }

    template<typename key_t>
    void FT_ANDNOTIterator<key_t>::operator++() {
        assert(m_direction > 0);
        next(1);
    }

    template<typename key_t>
    void FT_ANDNOTIterator<key_t>::operator--() {
        assert(m_direction < 0);
        next(-1);
    }

    template<typename key_t>
    bool FT_ANDNOTIterator<key_t>::join(key_t join_key, int direction) 
    {
        if (m_direction > 0) {
            auto &it = getBaseIterator();
            if(!it.join(join_key), 1) {
                return false;
            }
            if(!inResult(it.getKey(), 1) && !next(1)) {
                return false;
            }
            return true;
        } else {
            assert(m_direction < 0);
            auto &it = getBaseIterator();
            if(!it.join(join_key, -1)) {
                return false;
            }
            if(!inResult(it.getKey(), -1) && !next(-1)) {
                return false;
            }
            return true;
        }
    }

    template<typename key_t>
    void FT_ANDNOTIterator<key_t>::joinBound(key_t /*join_key*/) {
        THROWF(db0::InternalException) << "FT_ANDNOTIterator::joinBound not supported" << THROWF_END;
    }

    template<typename key_t>
    std::pair<key_t, bool> FT_ANDNOTIterator<key_t>::peek(key_t join_key) const 
    {
        auto cloned = clone();
        if(cloned->join(join_key), -1) {
            return std::make_pair(cloned->getKey(), true); 
        } else {
            return std::make_pair(join_key, false);
        }
    }

    template<typename key_t>
    std::unique_ptr<FT_Iterator<key_t>> FT_ANDNOTIterator<key_t>::clone(
        CloneMap<FT_Iterator<key_t>> *clone_map_ptr) const
    {
        std::vector<std::unique_ptr<FT_Iterator<key_t>>> sub_iterators;
        sub_iterators.reserve(m_joinable.size());
        for(const auto &sub_it : m_joinable) {
            sub_iterators.emplace_back(sub_it->clone(clone_map_ptr));
        }
        std::unique_ptr<FT_Iterator<key_t>> cloned(
            std::make_unique<FT_ANDNOTIterator>(std::move(sub_iterators), m_direction)
        );

        cloned->setID(this->getID());
        if (clone_map_ptr) {
            clone_map_ptr->insert(*cloned, *this);
        }
        return cloned;
    }
    
    template<typename key_t>
    std::unique_ptr<FT_Iterator<key_t> > FT_ANDNOTIterator<key_t>::beginTyped(int direction) const
    {
        std::vector<std::unique_ptr<FT_Iterator<key_t>>> sub_iterators;
        sub_iterators.reserve(m_joinable.size());
        for(const auto &sub_it : m_joinable) {
            sub_iterators.emplace_back(sub_it->beginTyped(direction));
        }
        auto result = std::make_unique<FT_ANDNOTIterator>(std::move(sub_iterators), direction);
        result->setID(this->getID());
        return result;
    }

    template<typename key_t> 
    bool FT_ANDNOTIterator<key_t>::limitBy(key_t key)
    {
        if(!m_joinable.front()->limitBy(key)) {
            return false;
        }
        m_subtrahends_heap.erase(
            std::remove_if(m_subtrahends_heap.begin(), m_subtrahends_heap.end(),
            [&key](HeapItem &item) {
                return !item.it->limitBy(key);
            }),
            m_subtrahends_heap.end()
        );
        // Rebuild heap
        for(HeapItem &item : m_subtrahends_heap) {
            item.key = item.it->getKey();
        }
        updateWithHeap();
        return true;
    }

    template<typename key_t>
    void FT_ANDNOTIterator<key_t>
    ::scanQueryTree(std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_function, int depth) const 
    {
        scan_function(this, depth);
        for (const auto &sub_it : m_joinable) {
            sub_it->scanQueryTree(scan_function, depth);
        }
    }

    template <typename key_t>
    const db0::FT_Iterator<key_t> &FT_ANDNOTIterator<key_t>::getFirst() const {
        return *m_joinable.front();
    }

    template <typename key_t> void FT_ANDNOTIterator<key_t>
        ::forNot(std::function<bool(const db0::FT_Iterator<key_t> &)> f) const
    {
        auto it = m_joinable.begin(), end = m_joinable.end();
        ++it;
        while (it != end) {
            if (!f(**it)) {
                break;
            }
            ++it;
        }
    }

    template <typename key_t> std::unique_ptr<db0::FT_Iterator<key_t> > FT_ANDNOTIterator<key_t>
        ::beginNot(int direction) const
    {
        db0::FT_OR_ORXIteratorFactory<key_t> factory(true);
        auto it = m_joinable.begin(), end = m_joinable.end();
        ++it;
        while (it != end) {
            factory.add((*it)->beginTyped(direction));
            ++it;
        }
        return factory.release(direction);
    }

    template <typename key_t> std::size_t db0::FT_ANDNOTIterator<key_t>::getDepth() const 
    {
        std::size_t max_inner = 0;
        for (auto &joinable: m_joinable) {
            max_inner = std::max(max_inner, joinable->getDepth());
        }
        return max_inner + 1u;
    }

    template <typename key_t> void db0::FT_ANDNOTIterator<key_t>::stop() {
        getBaseIterator().stop();
    }
    
    template <typename key_t> bool db0::FT_ANDNOTIterator<key_t>
        ::findBy(const std::function<bool(const db0::FT_Iterator<key_t> &)> &f) const 
    {
        if (!FT_Iterator<key_t>::findBy(f)) {
            return false;
        }
        for (const auto &joinable: m_joinable) {
            if (!joinable->findBy(f)) {
                return false;
            }
        }
        return true;
    }

    template <typename key_t> std::pair<bool, bool> 
    db0::FT_ANDNOTIterator<key_t>::mutateInner(const MutateFunction &f) 
    {
        auto result = db0::FT_Iterator<key_t>::mutateInner(f);
        if (result.first) {
            return result;
        }
        // can only mutate AND- part of the iterator
        if (!m_joinable.empty()) {
            auto &it = getBaseIterator();
            result = it.mutateInner(f);
            // was mutated and has result
            if (result.first && result.second) {                
                if (!inResult(it.getKey(), m_direction) && !next(m_direction)) {
                    result.second = false;
                }
            }
        }
        return result;
    }

    template <typename key_t> void db0::FT_ANDNOTIterator<key_t>::detach() {
        /* FIXME: implement
        for (auto &it: m_joinable) {
            it->detach();
        }
        */
    }
    
    template <typename key_t> const std::type_info &db0::FT_ANDNOTIterator<key_t>::typeId() const
    {
        return typeid(self_t);
    }    

    template class FT_ANDNOTIterator<std::uint64_t>;

}
