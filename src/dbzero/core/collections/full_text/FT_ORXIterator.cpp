#include <cstring>
#include "FT_ORXIterator.hpp"
#include "FT_Serialization.hpp"

namespace db0

{

	template <typename key_t>
	FT_JoinORXIterator<key_t>::FT_JoinORXIterator(std::list<std::unique_ptr<FT_Iterator<key_t> > > &&inner_iterators,
	    int direction, bool is_orx, bool lazy_init)		
		: m_direction(direction)
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

	template <typename key_t>
	FT_JoinORXIterator<key_t>::~FT_JoinORXIterator() = default;
    
	template <typename key_t>
	bool FT_JoinORXIterator<key_t>::isEnd() const {
		return this->m_end;
	}

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::operator++()
    {
		assert(m_direction > 0);
		assert(!m_end);
		if (m_is_orx) {
			key_t _key = m_forward_heap.front().key;
			_next();
			// pop all equal values from heap ( excluding OR )
			while (!m_forward_heap.empty() && _key==m_forward_heap.front().key) {
				_next();
			}
		} else {
			_next();
		}
		if (m_forward_heap.empty()) {
			setEnd();
		} else {
			this->m_join_key = m_forward_heap.front().key;
		}
	}

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::_next(void *buf)
    {
		assert(m_direction < 0);
		assert(!m_end);
		if (buf) {
			std::memcpy(buf, &m_join_key, sizeof(key_t));
		}
		if (m_is_orx) {
			key_t _key = m_back_heap.front().key;
			prev();
			// pop all equal values from heap ( excluding OR )
			while (!m_back_heap.empty() && (_key==m_back_heap.front().key)) {
				prev();
			}
		} else {
			prev();
		}
		if (m_back_heap.empty()) {
			setEnd();
		} else {
			this->m_join_key = m_back_heap.front().key;
		}
	}

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::operator--() {
		this->_next(nullptr);
	}

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::next(void *buf) {
		this->_next(buf);
	}
	
	template <typename key_t>
	key_t FT_JoinORXIterator<key_t>::getKey() const {
		assert(!m_end);
		return m_join_key;
	}
	
	template <typename key_t>
	bool FT_JoinORXIterator<key_t>::join(key_t key, int direction) 
    {
		if (m_direction > 0) {
            assert(!m_forward_heap.empty());
            // join all sub - iterators, then fix heap
            while (!m_forward_heap.empty() && (m_forward_heap.front().key < key)) {
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
                this->m_join_key = m_forward_heap.front().key;
                return true;
            }
        } else {
            // late initialization
            if (m_back_heap.empty()) {
                initHeap(false);
            }
            while (!m_back_heap.empty() && (m_back_heap.front().key > key)) {
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
                this->m_join_key = m_back_heap.front().key;
                return true;
            }
        }
	}

    template <typename key_t> bool FT_JoinORXIterator<key_t>::stopCurrentSimple()
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
                this->m_join_key = m_forward_heap.front().key;
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
                this->m_join_key = m_back_heap.front().key;
                return true;
            }
        }
    }

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::joinBound(key_t key)
    {
		for (auto it = m_joinable.begin(),itend = m_joinable.end();it!=itend;++it) {
			(**it).joinBound(key);
			key_t _key = (**it).getKey();
			// initialize / update join key
			if (_key < m_join_key) {
				m_join_key = _key;
			}
			// bound key reached
			if (_key==key) {
				break;
			}
		}
	}

	template <typename key_t>
	std::pair<key_t, bool> FT_JoinORXIterator<key_t>::peek(key_t key) const 
    {
		std::pair<key_t,bool> ping_res;
		ping_res.second = false;
		for(auto it = m_joinable.begin(),itend = m_joinable.end(); it!=itend; ++it) {
			std::pair<key_t,bool> res = (**it).peek(key);
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

	template <typename key_t>
	bool FT_JoinORXIterator<key_t>::limitBy(key_t key) 
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

	template <typename key_t>
	std::unique_ptr<FT_Iterator<key_t> > FT_JoinORXIterator<key_t>::clone(
		CloneMap<FT_Iterator<key_t> > *clone_map_ptr) const
	{
		std::list<std::unique_ptr<FT_Iterator<key_t> > > temp;
		for (auto it = m_joinable.begin(), itend = m_joinable.end(); it != itend; ++it) {
			temp.push_back((*it)->clone(clone_map_ptr));
		}
		FT_JoinORXIterator<key_t> *orx_result = 0;
		std::unique_ptr<db0::FT_Iterator<key_t> > result(orx_result = new FT_JoinORXIterator<key_t>(
		    std::move(temp), m_direction, this->m_is_orx, false)
		);
		assert(orx_result);		
		if (clone_map_ptr) {
			clone_map_ptr->insert(*result, *this);
		}
		return result;
	}

	template <typename key_t>
	std::unique_ptr<FT_Iterator<key_t> > FT_JoinORXIterator<key_t>::beginTyped(int direction) const
    {
		std::list<std::unique_ptr<FT_Iterator<key_t> > > temp;
		for (auto it = m_joinable.begin(), itend = m_joinable.end(); it != itend; ++it) {
			temp.push_back((*it)->beginTyped(direction));
		}
		std::unique_ptr<FT_Iterator<key_t> > result(
			new FT_JoinORXIterator<key_t>(std::move(temp), direction, m_is_orx, false));    
		return result;
	}

	template <typename key_t>
	std::ostream &FT_JoinORXIterator<key_t>::dump(std::ostream &os) const 
	{
		os << (this->m_is_orx?"ORX":"OR") << "@" << this << "[";
		dumpJoinable(os);
		return os << "]";
	}

	template <typename key_t>
	bool FT_JoinORXIterator<key_t>::equal(const FT_IteratorBase &it) const
    {
		if (it.typeId() != this->typeId()) {
			return false;
		}

		const FT_JoinORXIterator<key_t> &orx_it = reinterpret_cast<const FT_JoinORXIterator<key_t>&>(it);
		if (m_is_orx==orx_it.m_is_orx && m_joinable.size()==orx_it.m_joinable.size()) {
			// compare joinables ( order irrelevant )
			std::list<const FT_Iterator<key_t>*> refs;
			for (auto it = orx_it.m_joinable.begin(),itend = orx_it.m_joinable.end();it!=itend;++it) {
				refs.push_back((*it).get());
			}
			while (!refs.empty()) {
				const FT_Iterator<key_t> &ref_it = *refs.front();
				auto it = m_joinable.begin(),itend = m_joinable.end();
				while (it!=itend) {
					if ((*it)->equal(ref_it)) {
						break;
					}
					++it;
				}
				if (it==itend) {
					return false;
				}
				refs.pop_front();
			}
			return true;
		}		
		return false;
	}

	template <typename key_t>
	const FT_IteratorBase *FT_JoinORXIterator<key_t>::find(const FT_IteratorBase &it) const 
    {
		// self-check first
		if (this->equal(it)) {
			return this;
		}
		for (auto it_sub = m_joinable.begin(),itend = m_joinable.end();it_sub!=itend;++it_sub) {
			const FT_IteratorBase *it_filter = (*it_sub)->find(it);
			if (it_filter) {
				return it_filter;
			}
		}
		return 0;
	}

	template <typename key_t>
	int FT_JoinORXIterator<key_t>::getJoinCount() const 
    {
		if (m_forward_heap.empty()) {
			return getJoinCount(m_back_heap.begin(), m_back_heap.end());
		} else {
			return getJoinCount(m_forward_heap.begin(), m_forward_heap.end());
		}
	}

	template <typename key_t>
	bool FT_JoinORXIterator<key_t>::hasDuplicateKeys() const 
	{
		if (m_forward_heap.empty()) {
			return m_back_heap.hasDuplicatesForTopElement();
		} else {
			return m_forward_heap.hasDuplicatesForTopElement();
		}
	}

	template <typename key_t>
	const FT_Iterator<key_t> &FT_JoinORXIterator<key_t>::getSimple() const {
	    return const_cast<FT_JoinORXIterator<key_t>&>(*this).getSimple();
	}

	template <typename key_t> FT_Iterator<key_t> &FT_JoinORXIterator<key_t>::getSimple()
    {
		assert(!isEnd());
		if (!m_forward_heap.empty()) {
			return *m_forward_heap.front();
		}
		else {
			assert(!m_back_heap.empty());
			return *m_back_heap.front();
		}
	}
    
	template <typename key_t>
	bool FT_JoinORXIterator<key_t>::isORX() const {
		return m_is_orx;
	}

	template <typename key_t>
	template <typename iterator_t> int FT_JoinORXIterator<key_t>::getJoinCount(iterator_t begin, iterator_t end) const 
    {
		int result = 0;
		key_t key = getKey();
		while (begin!=end) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
			if (begin->key==key) {
#pragma GCC diagnostic pop
				++result;
			}
			++begin;
		}
		return result;
	}

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::setEnd() {
		this->m_end = true;
	}

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::init(int direction) 
    {
		// init heap
		if (initHeap(direction)) {
			this->m_join_key = (direction > 0)?(m_forward_heap.front().key):(m_back_heap.front().key);
		} else {
			setEnd();
		}
	}

	template <typename key_t>
	bool FT_JoinORXIterator<key_t>::initHeap(int direction) 
    {
		bool result = false;
		for (auto it = m_joinable.begin(),itend = m_joinable.end();it!=itend;++it) {
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
    
	template <typename key_t>
	void FT_JoinORXIterator<key_t>::_next() 
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
	
	template <typename key_t>
	void FT_JoinORXIterator<key_t>::prev() 
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

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::joinLead(const FT_Iterator<key_t> &it_lead) 
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

	template <typename key_t>
	void FT_JoinORXIterator<key_t>::dumpJoinable(std::ostream &os) const
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

	template <typename key_t>
	FT_OR_ORXIteratorFactory<key_t>::FT_OR_ORXIteratorFactory(bool orx_join)
		: m_orx_join(orx_join)
	{
	}

	template <typename key_t>
	FT_OR_ORXIteratorFactory<key_t>::~FT_OR_ORXIteratorFactory() = default;

	template <typename key_t>
	void FT_OR_ORXIteratorFactory<key_t>::add(std::unique_ptr<FT_Iterator<key_t> > &&it_joinable)
    {
		if (it_joinable.get()) {
			m_joinable.push_back(std::move(it_joinable));
		}
	}

	template <typename key_t>
	void FT_OR_ORXIteratorFactory<key_t>::clear() {
		this->m_joinable.clear();
	}

	template <typename key_t>
	bool FT_OR_ORXIteratorFactory<key_t>::empty() const {
		return m_joinable.empty();
	}

	template <typename key_t>
	std::unique_ptr<FT_Iterator<key_t> > FT_OR_ORXIteratorFactory<key_t>::release(int direction, bool lazy_init) 
    {
        if (m_joinable.size()==1u) {
            // single iterator - no use joining
            return std::move(m_joinable.front());
        }
        FT_JoinORXIterator<key_t> *temp;
        return releaseSpecial(direction, temp, lazy_init);
	}

	template <typename key_t>
	std::unique_ptr<FT_Iterator<key_t> > FT_OR_ORXIteratorFactory<key_t>::releaseSpecial(int direction,
		FT_JoinORXIterator<key_t> *&result, bool lazy_init)
	{
		result = nullptr;
		if (m_joinable.empty()) {
			// no iterators to join
			return {};
		}
		return std::unique_ptr<FT_Iterator<key_t>>(result = new FT_JoinORXIterator<key_t>(
			std::move(this->m_joinable), direction, this->m_orx_join, lazy_init)
		);
	}
	
	template <typename key_t>
	std::size_t FT_OR_ORXIteratorFactory<key_t>::size() const {
		return m_joinable.size();
	}

	template <typename key_t>
	FT_ORIteratorFactory<key_t>::FT_ORIteratorFactory ()
		: FT_OR_ORXIteratorFactory<key_t>(false)
	{
	}

	template <typename key_t>
	FT_ORXIteratorFactory<key_t>::FT_ORXIteratorFactory()
		: FT_OR_ORXIteratorFactory<key_t>(true)
	{
	}

    template <typename key_t>
    void db0::FT_JoinORXIterator<key_t>::scanQueryTree(
        std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_func, int depth) const
    {
        scan_func(this, depth);
        for (auto &it: m_joinable) {
            (*it).scanQueryTree(scan_func, depth + 1);
        }
    }

    template <typename key_t> void db0::FT_JoinORXIterator<key_t>
        ::forAll(std::function<bool(const db0::FT_Iterator<key_t> &)> f) const
    {
        for (const auto &joinable: m_joinable) {
            if (!f(*joinable)) {
                break;
            }
        }
    }

    template <typename key_t> std::size_t db0::FT_JoinORXIterator<key_t>::getDepth() const 
    {
        std::size_t max_inner = 0;
        for (auto &joinable: m_joinable) {
            max_inner = std::max(max_inner, joinable->getDepth());
        }
        return max_inner + 1u;
    }

    template <typename key_t> void db0::FT_JoinORXIterator<key_t>::stop() {
        this->m_end = true;
    }

    template <typename key_t> bool db0::FT_JoinORXIterator<key_t>
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
    db0::FT_JoinORXIterator<key_t>::mutateInner(const MutateFunction &f) 
    {
        auto result = db0::FT_Iterator<key_t>::mutateInner(f);
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

    template <typename key_t> void db0::FT_JoinORXIterator<key_t>::detach() {
		/* FIXME: implement
        for (auto &it: m_joinable) {
            it->detach();
        }
		*/
    }
	
	template <typename key_t>
	const std::type_info &db0::FT_JoinORXIterator<key_t>::typeId() const {
		return typeid(self_t);
	}

	template <typename key_t> 
	db0::FTIteratorType db0::FT_JoinORXIterator<key_t>::getSerialTypeId() const {
		return FTIteratorType::JoinOr;
	}

	template <typename key_t>
	void db0::FT_JoinORXIterator<key_t>::serializeFTIterator(std::vector<std::byte> &v) const
	{
		db0::serial::write(v, db0::serial::typeId<key_t>());
		db0::serial::write<std::int8_t>(v, m_direction);
		db0::serial::write(v, m_is_orx);
		db0::serial::write<std::uint32_t>(v, m_joinable.size());
		for (const auto &it: m_joinable) {
			it->serialize(v);
		}	
	}

	template <typename key_t>
	std::unique_ptr<FT_JoinORXIterator<key_t> > db0::FT_JoinORXIterator<key_t>::deserialize(Snapshot &workspace,
		std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
	{
		auto key_type_id = db0::serial::read<TypeIdType>(iter, end);
		if (key_type_id != db0::serial::typeId<key_t>()) {
			THROWF(db0::InternalException) << "Unsupported key type ID: " << key_type_id << THROWF_END;
		}
		int direction = db0::serial::read<std::int8_t>(iter, end);
		bool is_orx = db0::serial::read<bool>(iter, end);
		std::uint32_t joinable_size = db0::serial::read<std::uint32_t>(iter, end);
		std::list<std::unique_ptr<FT_Iterator<key_t> > > joinable;
		for (std::uint32_t i = 0; i < joinable_size; ++i) {
			// inner iterator may no longer be available for deserialization (e.g. token removed)
			auto inner_it = db0::deserializeFT_Iterator<key_t>(workspace, iter, end);
			if (inner_it) {
				joinable.push_back(std::move(inner_it));
			}			
		}
		return std::make_unique<FT_JoinORXIterator<key_t>>(std::move(joinable), direction, is_orx);
	}
	
	template <typename key_t>
	double db0::FT_JoinORXIterator<key_t>::compareTo(const FT_IteratorBase &it) const
	{
		assert(m_joinable.size() > 0);
		if (it.typeId() != this->typeId()) {
			return compareTo(reinterpret_cast<const FT_JoinORXIterator<key_t>&>(it));
		}
		
		// piecewise comparison
		double p_diff = 1.0 / (double)m_joinable.size();
		double m_diff = 1.0;
		for (auto &it_sub: m_joinable) {
			m_diff = std::min(m_diff, it_sub->compareTo(it));
		}
		return m_diff + p_diff * (m_joinable.size() - 1);
	}
	
	template <typename key_t>
	double db0::FT_JoinORXIterator<key_t>::compareTo(const FT_JoinORXIterator<key_t> &other) const
	{
		if (this->m_joinable.size() > other.m_joinable.size()) {
			return other.compareTo(*this);
		}
		assert(this->m_joinable.size() <= other.m_joinable.size());
		std::list<FT_Iterator<key_t>*> refs;
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
	
    template class FT_JoinORXIterator<std::uint64_t>;
    template class FT_OR_ORXIteratorFactory<std::uint64_t>;
    template class FT_ORIteratorFactory<std::uint64_t>;    
    template class FT_ORXIteratorFactory<std::uint64_t>;

}
