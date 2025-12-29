// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <algorithm>
#include <cstring>
#include <dbzero/core/utils/heap_utils.hpp>
#include "FT_ANDNOTIterator.hpp"
#include "FT_ORXIterator.hpp"
#include "FT_Serialization.hpp"
#include "FT_ORXIterator.hpp"

namespace db0 

{

    template<typename key_t>
    FT_ANDNOTIterator<key_t>::FT_ANDNOTIterator(std::vector<std::unique_ptr<FT_Iterator<key_t>>> &&inner_iterators,
        int direction, bool lazy_init)
        : FT_ANDNOTIterator<key_t>(this->nextUID(), std::move(inner_iterators), direction, lazy_init)
    {
    }

    template<typename key_t>
    FT_ANDNOTIterator<key_t>::FT_ANDNOTIterator(std::uint64_t uid, std::vector<std::unique_ptr<FT_Iterator<key_t>>> &&inner_iterators,
        int direction, bool lazy_init)
        : FT_Iterator<key_t>(uid)
        , m_direction(direction)
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
    const FT_IteratorBase* FT_ANDNOTIterator<key_t>::find(std::uint64_t uid) const 
    {
        if (this->m_uid == uid) {
            return this;
        }
        for (const std::unique_ptr<FT_Iterator<key_t>> &sub_it : m_joinable) {
            const FT_IteratorBase *found_it = sub_it->find(uid);
            if (found_it) {
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
            if (!it.join(join_key), 1) {
                return false;
            }
            if(!inResult(it.getKey(), 1) && !next(1)) {
                return false;
            }
            return true;
        } else {
            assert(m_direction < 0);
            auto &it = getBaseIterator();
            if (!it.join(join_key, -1)) {
                return false;
            }
            if (!inResult(it.getKey(), -1) && !next(-1)) {
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
        throw std::runtime_error("FT_ANDNOTIterator::peek not implemented");
    }
        
    template<typename key_t>
    std::unique_ptr<FT_Iterator<key_t> > FT_ANDNOTIterator<key_t>::beginTyped(int direction) const
    {
        std::vector<std::unique_ptr<FT_Iterator<key_t>>> sub_iterators;
        sub_iterators.reserve(m_joinable.size());
        for (const auto &sub_it : m_joinable) {
            sub_iterators.emplace_back(sub_it->beginTyped(direction));
        }
        return std::unique_ptr<FT_ANDNOTIterator>(
            new FT_ANDNOTIterator(this->m_uid, std::move(sub_iterators), direction)
        );
    }

    template<typename key_t> 
    bool FT_ANDNOTIterator<key_t>::limitBy(key_t key)
    {
        if (!m_joinable.front()->limitBy(key)) {
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
        for (HeapItem &item : m_subtrahends_heap) {
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
    
    template <typename key_t> const std::type_info &db0::FT_ANDNOTIterator<key_t>::typeId() const {
        return typeid(self_t);
    }    

    template <typename key_t>
    FTIteratorType db0::FT_ANDNOTIterator<key_t>::getSerialTypeId() const {
        return FTIteratorType::JoinAndNot;
    }

    template <typename key_t>
    void db0::FT_ANDNOTIterator<key_t>::serializeFTIterator(std::vector<std::byte> &v) const
    {
        db0::serial::write(v, db0::serial::typeId<key_t>());
        db0::serial::write<std::int8_t>(v, m_direction);
        db0::serial::write(v, m_joinable.size());
        for (const auto &it: m_joinable) {
            it->serialize(v);
        }
    }

    template <typename key_t>
    std::unique_ptr<db0::FT_ANDNOTIterator<key_t>> db0::FT_ANDNOTIterator<key_t>::deserialize(Snapshot &workspace,
        std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
    {
        using TypeIdType = decltype(db0::serial::typeId<void>());
        auto key_type_id = db0::serial::read<TypeIdType>(iter, end);
        if (key_type_id != db0::serial::typeId<key_t>()) {
            THROWF(db0::InternalException) << "Key type mismatch: " << key_type_id << " != " << db0::serial::typeId<key_t>()
                << THROWF_END;
        }
        auto direction = db0::serial::read<std::int8_t>(iter, end);
        auto joinable_size = db0::serial::read<std::size_t>(iter, end);
        std::vector<std::unique_ptr<FT_Iterator<key_t>>> joinable;        
        bool result = true;
        for (std::size_t i = 0; i < joinable_size; ++i) {
            auto inner_it = db0::deserializeFT_Iterator<key_t>(workspace, iter, end);
            if (inner_it) {
                joinable.emplace_back(std::move(inner_it));
            } else {
                // no result if first iterator (inclusion part) is not deserialized
                result &= (i != 0);
            }
        }

        if (!result) {
            return nullptr;
        }
        
        return std::make_unique<FT_ANDNOTIterator<key_t>>(std::move(joinable), direction);
    }
    
    template <typename key_t>
    double db0::FT_ANDNOTIterator<key_t>::compareToImpl(const FT_IteratorBase &it) const
    {
        if (this->typeId() == it.typeId()) {
            return compareTo(reinterpret_cast<const FT_ANDNOTIterator<key_t> &>(it));
        }
        return 1.0;
    }
    
    template <typename key_t>
    double db0::FT_ANDNOTIterator<key_t>::compareTo(const FT_ANDNOTIterator &other) const
    {
        double result = m_joinable.front()->compareTo(*other.m_joinable.front());
		std::list<FT_Iterator<key_t>*> refs;
		for (auto it = ++other.m_joinable.begin(),itend = other.m_joinable.end();it!=itend;++it) {
			refs.push_back((*it).get());
		}

		double p_diff = 1.0 / (double)(other.m_joinable.size() - 1);
		double n_result = 0.0;
		for (auto it = ++m_joinable.begin(),itend = m_joinable.end();it != itend;++it) {
			double m_diff = std::numeric_limits<double>::max();
			auto it_min = refs.end();
			for (auto it2 = refs.begin(),itend = refs.end();it2 != itend;++it2) {
				double diff = (*it)->compareTo(**it2);
				if (diff < m_diff) {
					m_diff = diff;
					it_min = it2;
				}
			}
			refs.erase(it_min);
			n_result += m_diff * p_diff;
		}
        return 1.0 - (1.0 - n_result) * (1.0 - result);
    }
    
    template <typename key_t>
    void db0::FT_ANDNOTIterator<key_t>::getSignature(std::vector<std::byte> &v) const
    {
        assert(m_joinable.size() > 0);
        std::vector<std::byte> buf;
        m_joinable.front()->getSignature(buf);
        // reuse the ORX-iterator signature
        db0::FT_JoinORXIterator<key_t>::getSignature(++m_joinable.begin(), m_joinable.end(), buf);
        // calculate hash from bytes as a signature
        db0::serial::sha256(buf, v);
    }
    
    template <typename key_t>
    bool db0::FT_ANDNOTIterator<key_t>::isNextKeyDuplicated() const {
        return getBaseIterator().isNextKeyDuplicated();
    }

    template class FT_ANDNOTIterator<std::uint64_t>;
    template class FT_ANDNOTIterator<UniqueAddress>;
    
}
