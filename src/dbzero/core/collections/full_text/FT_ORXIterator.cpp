// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <cstring>
#include "FT_ORXIterator.hpp"
#include "FT_Serialization.hpp"
#include "CP_Vector.hpp"

namespace db0

{

	template <typename key_t, typename key_storage_t>	
	FT_JoinORXIterator<key_t, key_storage_t>::FT_JoinORXIterator(std::list<std::unique_ptr<FT_IteratorT> > &&inner_iterators,
	    int direction, bool is_orx, bool lazy_init)
		: FT_JoinORXIterator(this->nextUID(), std::move(inner_iterators), direction, is_orx, lazy_init)
	{
	}

	template <typename key_t, typename key_storage_t>
    FT_JoinORXIterator<key_t, key_storage_t>::FT_JoinORXIterator(std::uint64_t uid,
		std::list<std::unique_ptr<FT_IteratorT> > &&inner_iterators, int direction, bool is_orx, bool lazy_init)
		: super_t(uid)
		, m_direction(direction)
		, m_forward_heap(m_direction > 0?4:0)
		, m_back_heap(m_direction > 0?0:4)
		, m_end(false)
		, m_is_orx(is_orx)		
		, m_key_bound(m_direction)
	{
		for (auto &s: inner_iterators) {
			m_joinable.emplace_back(std::move(s));
		}
		// skip initialization when lazy init requested
		if (!lazy_init) {
            init(direction);
        }
	}

	template <typename key_t, typename key_storage_t>
	FT_JoinORXIterator<key_t, key_storage_t>::~FT_JoinORXIterator() = default;

	template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::isEnd() const {
		return this->m_end;
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::operator++()
    {
		assert(m_direction > 0);
		assert(!m_end);
		if (m_is_orx) {
			auto _key = m_forward_heap.front().m_key;
			_next();
			// pop all equal values from heap (exclusive OR)
			while (!m_forward_heap.empty() && _key == m_forward_heap.front().m_key) {
				_next();
			}
		} else {
			_next();
		}
		if (m_forward_heap.empty()) {
			setEnd();
		} else {
			this->m_join_key = m_forward_heap.front().m_key;
		}
	}
	
	template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::isNextKeyDuplicated() const
	{
		// no duplication when exclusive join
		if (m_is_orx) {
			return false;
		}
		
		// note that duplication may exist either between iterators or within the yielding iterator
		if (m_direction > 0) {
			return (m_forward_heap.isFrontElementDuplicated() || 
				m_forward_heap.front().it->isNextKeyDuplicated()
			);
		} else {
			return (m_back_heap.isFrontElementDuplicated() || 
				m_back_heap.front().it->isNextKeyDuplicated()
			);
		}
		
		return false;
	}
	
	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::_next(void *buf)
    {		
		assert(m_direction < 0);
		assert(!m_end);
		if (buf) {
			*(key_storage_t*)buf = m_join_key;
		}
		if (m_is_orx) {
			auto _key = m_back_heap.front().m_key;
			prev();
			// pop all equal values from heap (excluding OR)
			while (!m_back_heap.empty() && (_key == m_back_heap.front().m_key)) {
				prev();
			}
		} else {
			prev();
		}
		if (m_back_heap.empty()) {
			setEnd();
		} else {
			this->m_join_key = m_back_heap.front().m_key;
		}
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::operator--() {
		this->_next(nullptr);
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::next(void *buf) {
		this->_next(buf);
	}
	
	template <typename key_t, typename key_storage_t>
	key_t FT_JoinORXIterator<key_t, key_storage_t>::getKey() const {
		assert(!m_end);
		return m_join_key;
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::getKey(key_storage_t &key) const {
		assert(!m_end);
		key = m_join_key;
	}
	
	template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::join(key_t key, int direction) 
    {
		if (m_direction > 0) {
            assert(!m_forward_heap.empty());
            // join all sub - iterators, then fix heap
            while (!m_forward_heap.empty() && (m_forward_heap.front().m_key < key)) {
                if (m_forward_heap.front().join(key, 1)) {
                    m_forward_heap.downfix();
                } else {
                    // erase from heap
                    m_forward_heap.pop_front();
                }
            }
            if (m_forward_heap.empty()) {
                setEnd();
                return false; // join failed
            } else {
                this->m_join_key = m_forward_heap.front().m_key;
                return true;
            }
        } else {
            // late initialization
            if (m_back_heap.empty()) {
                initHeap(false);
            }
            while (!m_back_heap.empty() && (m_back_heap.front().m_key > key)) {
                if (m_back_heap.front().join(key, -1)) {
                    m_back_heap.downfix();
                } else {                    
                    // erase from heap
                    m_back_heap.pop_front();
                }
            }
            if (m_back_heap.empty()) {
                setEnd();
                return false; // join failed
            } else {
                this->m_join_key = m_back_heap.front().m_key;
                return true;
            }
        }
	}

    template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::stopCurrentSimple()
    {
        if (m_direction > 0) {
            // late initialization
            if (m_forward_heap.empty()) {
                initHeap(m_direction);
            }
            assert(!m_forward_heap.empty());            
            // erase from heap
            m_forward_heap.pop_front();
            if (m_forward_heap.empty()) {
                setEnd();
                return false;
            } else {
                this->m_join_key = m_forward_heap.front().m_key;
                return true;
            }
        } else {
            // late initialization
            if (m_back_heap.empty()) {
                initHeap(m_direction);
            }
            assert(!m_back_heap.empty());
            // erase from heap
            m_back_heap.pop_front();
            if (m_back_heap.empty()) {
                setEnd();
                return false;
            } else {
                this->m_join_key = m_back_heap.front().m_key;
                return true;
            }
        }
    }

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::joinBound(key_t key)
    {
		for (auto it = m_joinable.begin(),itend = m_joinable.end();it != itend;++it) {
			(**it).joinBound(key);
			key_storage_t _key;
			(**it).getKey(_key);
			// initialize / update join key
			if (_key < m_join_key) {
				m_join_key = _key;
			}
			// bound key reached
			if (_key == key) {
				break;
			}
		}
	}

	template <typename key_t, typename key_storage_t>
	std::pair<key_t, bool> FT_JoinORXIterator<key_t, key_storage_t>::peek(key_t key) const 
    {
		std::pair<key_t,bool> ping_res;
		ping_res.second = false;
		for(auto it = m_joinable.begin(),itend = m_joinable.end(); it!=itend; ++it) {
			std::pair<key_t, bool> res = (**it).peek(key);
			if (res.second) {
				if (!ping_res.second || (res.first > ping_res.first)) {
					ping_res = res;
					// join key reached
					if (ping_res.first==key) {
						break;
					}
				}
			}
		}
		return ping_res;
	}
	
	template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::limitBy(key_t key) 
    {
		// apply bounds to underlying iterators first
		for (auto it = m_joinable.begin(),itend = m_joinable.end();it!=itend;++it) {
			(**it).limitBy(key);
		}
		// must reinitialize heaps (since some of underlying iterators might have been invalidated)
		m_forward_heap.clear();
		m_back_heap.clear();
		return initHeap(m_direction);
	}
	
	template <typename key_t, typename key_storage_t>
	std::unique_ptr<FT_Iterator<key_t, key_storage_t> > FT_JoinORXIterator<key_t, key_storage_t>::beginTyped(int direction) const
    {
		std::list<std::unique_ptr<FT_IteratorT> > temp;
		for (auto it = m_joinable.begin(), itend = m_joinable.end(); it != itend; ++it) {
			temp.push_back((*it)->beginTyped(direction));
		}
		return std::unique_ptr<FT_IteratorT>(
			new FT_JoinORXIterator<key_t, key_storage_t>(this->m_uid, std::move(temp), direction, m_is_orx, false)
		);    		
	}
	
	template <typename key_t, typename key_storage_t>
	std::ostream &FT_JoinORXIterator<key_t, key_storage_t>::dump(std::ostream &os) const 
	{
		os << (this->m_is_orx?"ORX":"OR") << "@" << this << "[";
		dumpJoinable(os);
		return os << "]";
	}

	template <typename key_t, typename key_storage_t>
	const FT_IteratorBase *FT_JoinORXIterator<key_t, key_storage_t>::find(std::uint64_t uid) const 
    {
		// self-check first
		if (this->m_uid == uid) {
			return this;
		}
		for (auto it_sub = m_joinable.begin(),itend = m_joinable.end();it_sub!=itend;++it_sub) {
			const FT_IteratorBase *it_filter = (*it_sub)->find(uid);
			if (it_filter) {
				return it_filter;
			}
		}
		return nullptr;
	}

	template <typename key_t, typename key_storage_t>
	int FT_JoinORXIterator<key_t, key_storage_t>::getJoinCount() const 
    {
		if (m_forward_heap.empty()) {
			return getJoinCount(m_back_heap.begin(), m_back_heap.end());
		} else {
			return getJoinCount(m_forward_heap.begin(), m_forward_heap.end());
		}
	}

	template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::hasDuplicateKeys() const 
	{
		if (m_forward_heap.empty()) {
			return m_back_heap.hasDuplicatesForTopElement();
		} else {
			return m_forward_heap.hasDuplicatesForTopElement();
		}
	}
	
	template <typename key_t, typename key_storage_t>
	std::uint64_t FT_JoinORXIterator<key_t, key_storage_t>::getInnerUID() const
	{
		assert(!isEnd());
		if (!m_forward_heap.empty()) {
			return m_forward_heap.front()->getUID();
		}
		else {
			assert(!m_back_heap.empty());
			return m_back_heap.front()->getUID();
		}
	}
	
	template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::isORX() const {
		return m_is_orx;
	}

	template <typename key_t, typename key_storage_t>
	template <typename iterator_t> 
	int FT_JoinORXIterator<key_t, key_storage_t>::getJoinCount(iterator_t begin, iterator_t end) const 
    {
		int result = 0;
		key_storage_t key;
		getKey(key);
		while (begin!=end) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
			if (begin->m_key == key) {
#pragma GCC diagnostic pop
				++result;
			}
			++begin;
		}
		return result;
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::setEnd() {
		this->m_end = true;
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::init(int direction)
    {
		// init heap
		if (initHeap(direction)) {
			this->m_join_key = (direction > 0)?(m_forward_heap.front().m_key):(m_back_heap.front().m_key);
		} else {
			setEnd();
		}
	}
	
	template <typename key_t, typename key_storage_t>
	bool FT_JoinORXIterator<key_t, key_storage_t>::initHeap(int direction)
    {
		bool result = false;
		for (auto it = m_joinable.begin(),itend = m_joinable.end();it != itend;++it) {
			// do not include "end" iterators
			if (!(**it).isEnd()) {
				if (direction > 0) {
					m_forward_heap.insert_grow(**it);
				} else {
					m_back_heap.insert_grow(**it);
				}
				result = true;
			}
		}
		return result;
	}
    
	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::_next()
	{
		assert(!m_end);
		++(m_forward_heap.front());
		if (m_forward_heap.front().is_end) {
			// remove finished (end) iterator from heap
			m_forward_heap.pop_front();
		} else {
			// fix after front element modified
			m_forward_heap.downfix();
		}
	}
	
	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::prev() 
	{
		assert(!m_end);
		--(m_back_heap.front());
		if (m_back_heap.front().is_end) {
			// remove finished (end) iterator from heap
			m_back_heap.pop_front();
		} else {
			m_back_heap.downfix();
		}
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::joinLead(const FT_IteratorT &it_lead)
    {
		if (!m_forward_heap.empty()) {
			while (!m_end && (m_forward_heap.front()!=it_lead)) {
				_next();
			}
		} else {
			assert (!m_back_heap.empty());
			while (!m_end && (m_back_heap.front()!=it_lead)) {
				prev();
			}
		}
	}

	template <typename key_t, typename key_storage_t>
	void FT_JoinORXIterator<key_t, key_storage_t>::dumpJoinable(std::ostream &os) const
    {
		bool is_first = true;
		for (auto it = m_joinable.begin(),itend = m_joinable.end();it!=itend;++it) {
			if (!is_first) {
				os << ",";
			}
			(*it)->dump(os);
			is_first = false;
		}
	}

	template <typename key_t, typename key_storage_t>
	FT_OR_ORXIteratorFactory<key_t, key_storage_t>::FT_OR_ORXIteratorFactory(bool orx_join)
		: m_orx_join(orx_join)
	{
	}

	template <typename key_t, typename key_storage_t>
	FT_OR_ORXIteratorFactory<key_t, key_storage_t>::~FT_OR_ORXIteratorFactory() = default;

	template <typename key_t, typename key_storage_t>
	void FT_OR_ORXIteratorFactory<key_t, key_storage_t>::add(std::unique_ptr<FT_IteratorT> &&it_joinable)
    {
		if (it_joinable.get()) {
			m_joinable.push_back(std::move(it_joinable));
		}
	}

	template <typename key_t, typename key_storage_t>
	void FT_OR_ORXIteratorFactory<key_t, key_storage_t>::clear() {
		this->m_joinable.clear();
	}

	template <typename key_t, typename key_storage_t>
	bool FT_OR_ORXIteratorFactory<key_t, key_storage_t>::empty() const {
		return m_joinable.empty();
	}

	template <typename key_t, typename key_storage_t>
	std::unique_ptr<FT_Iterator<key_t, key_storage_t> > FT_OR_ORXIteratorFactory<key_t, key_storage_t>::release(
		int direction, bool lazy_init)
    {
        if (m_joinable.size()==1u) {
            // single iterator - no use joining
            return std::move(m_joinable.front());
        }
        FT_JoinORXIterator<key_t, key_storage_t> *temp;
        return releaseSpecial(direction, temp, lazy_init);
	}

	template <typename key_t, typename key_storage_t>
	std::unique_ptr<FT_Iterator<key_t, key_storage_t> > FT_OR_ORXIteratorFactory<key_t, key_storage_t>::releaseSpecial(int direction,
		FT_JoinORXIterator<key_t, key_storage_t> *&result, bool lazy_init)
	{
		result = nullptr;
		if (m_joinable.empty()) {
			// no iterators to join
			return {};
		}
		return std::unique_ptr<FT_IteratorT>(result = new FT_JoinORXIterator<key_t, key_storage_t>(
			std::move(this->m_joinable), direction, this->m_orx_join, lazy_init)
		);
	}

	template <typename key_t, typename key_storage_t>
	std::size_t FT_OR_ORXIteratorFactory<key_t, key_storage_t>::size() const {
		return m_joinable.size();
	}

	template <typename key_t, typename key_storage_t>
	FT_ORIteratorFactory<key_t, key_storage_t>::FT_ORIteratorFactory ()
		: FT_OR_ORXIteratorFactory<key_t, key_storage_t>(false)
	{
	}

	template <typename key_t, typename key_storage_t>
	FT_ORXIteratorFactory<key_t, key_storage_t>::FT_ORXIteratorFactory()
		: FT_OR_ORXIteratorFactory<key_t, key_storage_t>(true)
	{
	}

    template <typename key_t, typename key_storage_t>
    void db0::FT_JoinORXIterator<key_t, key_storage_t>::scanQueryTree(
        std::function<void(const FT_IteratorT *it_ptr, int depth)> scan_func, int depth) const
    {
        scan_func(this, depth);
        for (auto &it: m_joinable) {
            (*it).scanQueryTree(scan_func, depth + 1);
        }
    }

    template <typename key_t, typename key_storage_t>
    void db0::FT_JoinORXIterator<key_t, key_storage_t>::forAll(
		std::function<bool(const FT_IteratorT &)> f) const
    {
        for (const auto &joinable: m_joinable) {
            if (!f(*joinable)) {
                break;
            }
        }
    }

    template <typename key_t, typename key_storage_t>
    std::size_t db0::FT_JoinORXIterator<key_t, key_storage_t>::getDepth() const
    {
        std::size_t max_inner = 0;
        for (auto &joinable: m_joinable) {
            max_inner = std::max(max_inner, joinable->getDepth());
        }
        return max_inner + 1u;
    }

    template <typename key_t, typename key_storage_t>
	void db0::FT_JoinORXIterator<key_t, key_storage_t>::stop() {
        this->m_end = true;
    }

    template <typename key_t, typename key_storage_t>
	bool db0::FT_JoinORXIterator<key_t, key_storage_t>::findBy(
		const std::function<bool(const FT_IteratorT &)> &f) const
    {
        if (!FT_IteratorT::findBy(f)) {
            return false;
        }
        for (const auto &joinable: m_joinable) {
            if (!joinable->findBy(f)) {
                return false;
            }
        }
        return true;
    }

    template <typename key_t, typename key_storage_t> std::pair<bool, bool> 
    db0::FT_JoinORXIterator<key_t, key_storage_t>::mutateInner(const MutateFunction &f) 
    {
        auto result = FT_IteratorT::mutateInner(f);
        if (result.first) {
            return result;
        }
        if (m_direction > 0) {
            if (!m_forward_heap.empty()) {
                m_forward_heap.front()->mutateInner(f);
            }
        } else {
            if (!m_back_heap.empty()) {
                m_back_heap.front()->mutateInner(f);
            }
        }
        if (result.first) {
            if (result.second) {
                if (m_direction > 0) {
                    m_forward_heap.downfix();
                } else {
                    m_back_heap.downfix();
                }
            } else {
                if (m_direction > 0) {
                    m_forward_heap.pop_front();
                    result.second = !m_forward_heap.empty();
                } else {
                    m_back_heap.pop_front();
                    result.second = !m_back_heap.empty();
                }
            }
        }
        return result;
    }

    template <typename key_t, typename key_storage_t>
	void db0::FT_JoinORXIterator<key_t, key_storage_t>::detach() 
	{
		/* FIXME: implement
        for (auto &it: m_joinable) {
            it->detach();
        }
		*/
    }
	
	template <typename key_t, typename key_storage_t>
	const std::type_info &db0::FT_JoinORXIterator<key_t, key_storage_t>::typeId() const {
		return typeid(self_t);
	}

	template <typename key_t, typename key_storage_t>
	db0::FTIteratorType db0::FT_JoinORXIterator<key_t, key_storage_t>::getSerialTypeId() const {
		return FTIteratorType::JoinOr;
	}

	template <typename key_t, typename key_storage_t>
	void db0::FT_JoinORXIterator<key_t, key_storage_t>::serializeFTIterator(std::vector<std::byte> &v) const
	{
		db0::serial::write(v, db0::serial::typeId<key_t>());
		db0::serial::write<std::int8_t>(v, m_direction);
		db0::serial::write(v, m_is_orx);
		db0::serial::write<std::uint32_t>(v, m_joinable.size());
		for (const auto &it: m_joinable) {
			it->serialize(v);
		}	
	}

	template <typename key_t, typename key_storage_t>
	std::unique_ptr<FT_JoinORXIterator<key_t, key_storage_t> > 
	db0::FT_JoinORXIterator<key_t, key_storage_t>::deserialize(Snapshot &workspace,
		std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
	{
		auto key_type_id = db0::serial::read<TypeIdType>(iter, end);
		if (key_type_id != db0::serial::typeId<key_t>()) {
			THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id << THROWF_END;
		}
		int direction = db0::serial::read<std::int8_t>(iter, end);
		bool is_orx = db0::serial::read<bool>(iter, end);
		std::uint32_t joinable_size = db0::serial::read<std::uint32_t>(iter, end);
		std::list<std::unique_ptr<FT_IteratorT> > joinable;
		for (std::uint32_t i = 0; i < joinable_size; ++i) {
			// inner iterator may no longer be available for deserialization (e.g. token removed)
			auto inner_it = db0::deserializeFT_Iterator<key_t, key_storage_t>(workspace, iter, end);
			if (inner_it) {
				joinable.push_back(std::move(inner_it));
			}
		}
		return std::make_unique<self_t>(std::move(joinable), direction, is_orx);
	}
	
	template <typename key_t, typename key_storage_t>
	double db0::FT_JoinORXIterator<key_t, key_storage_t>::compareToImpl(const FT_IteratorBase &it) const
	{
		assert(m_joinable.size() > 0);
		if (it.typeId() == this->typeId()) {
			return compareTo(reinterpret_cast<const FT_JoinORXIterator<key_t, key_storage_t>&>(it));
		}
		
		// piecewise comparison
		double p_diff = 1.0 / (double)m_joinable.size();
		double m_diff = 1.0;
		for (auto &it_sub: m_joinable) {
			m_diff = std::min(m_diff, it_sub->compareTo(it));
		}
		return m_diff + p_diff * (m_joinable.size() - 1);
	}
	
	template <typename key_t, typename key_storage_t>
	double db0::FT_JoinORXIterator<key_t, key_storage_t>::compareTo(
		const FT_JoinORXIterator<key_t, key_storage_t> &other) const
	{
		if (this->m_joinable.size() > other.m_joinable.size()) {
			return other.compareTo(*this);
		}
		assert(this->m_joinable.size() <= other.m_joinable.size());
		std::list<FT_IteratorT*> refs;
		for (auto it = other.m_joinable.begin(),itend = other.m_joinable.end();it!=itend;++it) {
			refs.push_back((*it).get());
		}

		double p_diff = 1.0 / (double)other.m_joinable.size();
		double result = 0.0;
		for (auto &it: this->m_joinable) {
			double m_diff = std::numeric_limits<double>::max();
			auto it_min = refs.end();
			for (auto it2 = refs.begin(),itend = refs.end();it2 != itend;++it2) {
				double diff = it->compareTo(**it2);
				if (diff < m_diff) {
					m_diff = diff;
					it_min = it2;
				}
			}
			refs.erase(it_min);
			result += m_diff * p_diff;
		}
		result += p_diff * refs.size();
		return result;
	}
	
	template <typename key_t, typename key_storage_t>
	void db0::FT_JoinORXIterator<key_t, key_storage_t>::getSignature(std::vector<std::byte> &v) const {
		this->getSignature(m_joinable.begin(), m_joinable.end(), v);
	}
	
    template class FT_JoinORXIterator<UniqueAddress>;
    template class FT_OR_ORXIteratorFactory<UniqueAddress>;
    template class FT_ORIteratorFactory<UniqueAddress>;
    template class FT_ORXIteratorFactory<UniqueAddress>;

    template class FT_JoinORXIterator<const UniqueAddress*, CP_Vector<UniqueAddress>>;
    template class FT_OR_ORXIteratorFactory<const UniqueAddress*, CP_Vector<UniqueAddress>>;
    template class FT_ORIteratorFactory<const UniqueAddress*, CP_Vector<UniqueAddress>>;
    template class FT_ORXIteratorFactory<const UniqueAddress*, CP_Vector<UniqueAddress>>;

    template class FT_JoinORXIterator<std::uint64_t>;
    template class FT_OR_ORXIteratorFactory<std::uint64_t>;
    template class FT_ORIteratorFactory<std::uint64_t>;
    template class FT_ORXIteratorFactory<std::uint64_t>;

	template class FT_JoinORXIterator<const std::uint64_t*, CP_Vector<std::uint64_t>>;
    template class FT_OR_ORXIteratorFactory<const std::uint64_t*, CP_Vector<std::uint64_t>>;
    template class FT_ORIteratorFactory<const std::uint64_t*, CP_Vector<std::uint64_t>>;
    template class FT_ORXIteratorFactory<const std::uint64_t*, CP_Vector<std::uint64_t>>;

}
