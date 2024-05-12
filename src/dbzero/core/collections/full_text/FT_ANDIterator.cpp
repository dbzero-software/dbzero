#include <cassert>
#include "FT_ANDIterator.hpp"
#include "FT_Serialization.hpp"

namespace db0

{

    template <typename key_t, bool UniqueKeys>
    FT_JoinANDIterator<key_t, UniqueKeys>::FT_JoinANDIterator(
        std::list<std::unique_ptr<FT_Iterator<key_t> > > &&inner_iterators, int direction, bool lazy_init)        
        : m_direction(direction)
        , m_end(false)    
    {
        m_joinable.splice(m_joinable.end(), inner_iterators);
        // skip initialization if the lazy init was requested
        if (!lazy_init) {
            // test end iterator condition
            for (auto &it : m_joinable) {
                if ((*it).isEnd()) {
                    setEnd();
                    return;
                }
            }            
            joinAll();
        }
    }
    
	template <typename key_t, bool UniqueKeys>
	FT_JoinANDIterator<key_t, UniqueKeys>::FT_JoinANDIterator(std::unique_ptr<FT_Iterator<key_t> > &&it0, 
        std::unique_ptr<FT_Iterator<key_t> > &&it1, int direction, bool lazy_init)
        : m_direction(direction)
        , m_end(false)        
	{
		m_joinable.emplace_back(std::move(it0));
		m_joinable.emplace_back(std::move(it1));
        // skip initialization if the lazy init was requested
		if (!lazy_init) {
            // test end iterator(s) condition
            if ((it0->isEnd()) || (it1->isEnd())) {
                setEnd();
            } else {                
                joinAll();
            }
        }
	}

	template <typename key_t, bool UniqueKeys>
	FT_JoinANDIterator<key_t, UniqueKeys>::~FT_JoinANDIterator() = default;

	/**
	 * IFT_Iterator interface members
	 */
	template <typename key_t, bool UniqueKeys>
	bool FT_JoinANDIterator<key_t, UniqueKeys>::isEnd() const {
		return m_end;
	}
    
	template <typename key_t, bool UniqueKeys>
	void FT_JoinANDIterator<key_t, UniqueKeys>::operator++() 
    {
        assert(m_direction > 0);
        assert(!isEnd());        
        if constexpr (UniqueKeys) {
            _nextUnique();
        } else {
            _next();
        }
	}

	template <typename key_t, bool UniqueKeys>
	void FT_JoinANDIterator<key_t, UniqueKeys>::operator--() {
        this->_next(nullptr);
	}

	template <typename key_t, bool UniqueKeys>
	void FT_JoinANDIterator<key_t, UniqueKeys>::_next(void *buf) 
    {
        assert(m_direction < 0);
        assert(!isEnd());
        if (buf) {
            reinterpret_cast<key_t*>(buf)[0] = m_join_key;            
        }        
        if constexpr (UniqueKeys) {
            _nextUnique();
        } else {
            _next();
        }     
    }
    
	template <typename key_t, bool UniqueKeys>
	void FT_JoinANDIterator<key_t, UniqueKeys>::next(void *buf)
    {
        this->_next(buf);
    }

	template <typename key_t, bool UniqueKeys>
	key_t FT_JoinANDIterator<key_t, UniqueKeys>::getKey() const {
		return m_join_key;
	}

    template <typename key_t, bool UniqueKeys>
    const db0::FT_Iterator<key_t>& FT_JoinANDIterator<key_t, UniqueKeys>::getSimple() const {
        assert(!m_joinable.empty());
        return *m_joinable.front();
    }

	template <typename key_t, bool UniqueKeys>
	bool FT_JoinANDIterator<key_t, UniqueKeys>::join(key_t join_key, int direction)
    {		
		if ((*m_joinable.front()).join(join_key, direction)) {
			joinAll();
			return !isEnd();
		} else {
			setEnd();
			return false;
		}
	}
    
	template <typename key_t, bool UniqueKeys>
	void FT_JoinANDIterator<key_t, UniqueKeys>::joinBound(key_t key) 
    {
		for (auto it = m_joinable.begin(), itend = m_joinable.end(); it!=itend; ++it) {
			assert((*it).get());
			// try join leading iterator
			assert(!(**it).isEnd());
			(**it).joinBound(key);
			m_join_key = (**it).getKey();
			if (m_join_key!=key) {
				break;
			}
		}		
	}

	template <typename key_t, bool UniqueKeys>
	std::pair<key_t, bool> FT_JoinANDIterator<key_t, UniqueKeys>::peek(key_t join_key) const 
    {
		key_t lead_key = join_key;
		for (auto it = m_joinable.begin(),itend = m_joinable.end(); it!=itend; ++it) {
			std::pair<key_t, bool> peek_result = (*it)->peek(lead_key);
			if (!peek_result.second) {
				return std::pair<key_t, bool>(key_t(), false);
			}
			// join condition
			assert(peek_result.first <= lead_key);
			if (peek_result.first < lead_key) {
				lead_key = peek_result.first;
				// this is the new leader ( push front using splice operation )
				m_joinable.splice(m_joinable.begin(), m_joinable,it);
			}
		}
		return std::make_pair(lead_key, true);
	}

	template <typename key_t, bool UniqueKeys>
	std::unique_ptr<FT_Iterator<key_t> > FT_JoinANDIterator<key_t, UniqueKeys>::clone(
		CloneMap<FT_Iterator<key_t> > *clone_map_ptr) const
	{        
		std::list<std::unique_ptr<FT_Iterator<key_t> > > temp;
		{
			for (auto it = m_joinable.begin(),itend = m_joinable.end();it!=itend;++it) {
				temp.emplace_back((*it)->clone(clone_map_ptr));
			}
		}
		std::unique_ptr<FT_Iterator<key_t> > result(new FT_JoinANDIterator<key_t, UniqueKeys>(
			std::move(temp), m_direction, m_end, m_join_key, tag_cloned())
		);		
		if (clone_map_ptr) {
			clone_map_ptr->insert(*result, *this);
		}
		return result;
	}

	template <typename key_t, bool UniqueKeys>
	std::unique_ptr<FT_Iterator<key_t> > FT_JoinANDIterator<key_t, UniqueKeys>::beginTyped(int direction) const
    {
		// collect joinable (must sync)
		std::list<std::unique_ptr<FT_Iterator<key_t> > > temp;
        for (auto it = m_joinable.begin(),itend = m_joinable.end();it!=itend;++it) {
            temp.emplace_back((*it)->beginTyped(direction));
        }
		auto result = std::make_unique<FT_JoinANDIterator<key_t> >(std::move(temp), direction, false);        
        return result;
	}

	template <typename key_t, bool UniqueKeys>
	bool FT_JoinANDIterator<key_t, UniqueKeys>::limitBy(key_t key) 
    {
		for (auto it = m_joinable.begin(),itend = m_joinable.end(); it!=itend; ++it) {
			if (!(**it).limitBy(key)) {
				setEnd();
				return false;
			}
		}
		return true;
	}

	template <typename key_t, bool UniqueKeys>
	std::ostream &FT_JoinANDIterator<key_t, UniqueKeys>::dump(std::ostream &os) const 
    {
		os << "AND@" << this << "[";
		bool is_first = true;
		for (auto it = m_joinable.begin(),itend = m_joinable.end();it!=itend;++it) {
			if (!is_first) {
				os << ",";
			}
			(*it)->dump(os);
			is_first = false;
		}
		return os << "]";
	}

	template <typename key_t, bool UniqueKeys>
	bool FT_JoinANDIterator<key_t, UniqueKeys>::equal(const FT_IteratorBase &it) const 
    {
		if (it.typeId() == this->typeId()) {
			const FT_JoinANDIterator<key_t, UniqueKeys> &and_it = static_cast<const FT_JoinANDIterator<key_t, UniqueKeys>&>(it);
			if (m_joinable.size() == and_it.m_joinable.size()) {
				// compare joinables ( order irrelevant )
				std::list<const FT_Iterator<key_t>*> refs;
				for (auto it = and_it.m_joinable.begin(),itend = and_it.m_joinable.end();it!=itend;++it) {
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
					if (it == itend) {
						return false;
					}
					refs.pop_front();
				}
				return true;
			}
		}
		return false;
	}

	template <typename key_t, bool UniqueKeys>
	const FT_IteratorBase *FT_JoinANDIterator<key_t, UniqueKeys>::find(const FT_IteratorBase &it) const 
    {
		// self-check first
		if (this->equal(it)) {
			return this;
		}
		// find in joinables
		for (auto it_sub = m_joinable.begin(), itend = m_joinable.end();it_sub!=itend;++it_sub) {
			const FT_IteratorBase *it_filter = (*it_sub)->find(it);
			if (it_filter) {
				return it_filter;
			}
		}
		// no equal iterator found
		return 0;
	}

	template <typename key_t, bool UniqueKeys>
	void FT_JoinANDIterator<key_t, UniqueKeys>::setEnd() {
		m_end = true;
	}

    template <typename key_t, bool UniqueKeys> void FT_JoinANDIterator<key_t, UniqueKeys>::_next()
    {
        if (m_direction > 0) {
            FT_Iterator<key_t> *lead_it = m_joinable.front().get();
            ++(*lead_it);
            if (lead_it->isEnd() || lead_it->getKey() != m_join_key) {
                m_joinable.splice(m_joinable.end(), m_joinable, m_joinable.begin());
                lead_it = m_joinable.front().get();
                if (lead_it->isEnd()) {
                    setEnd();
                    return;
                }
                if (lead_it->getKey() != m_join_key) {
                    joinAll();
                }
            }
        } else {
            FT_Iterator<key_t> *lead_it = m_joinable.front().get();
            --(*lead_it);
            if (lead_it->isEnd() || lead_it->getKey() != m_join_key) {
                m_joinable.splice(m_joinable.end(), m_joinable, m_joinable.begin());
                lead_it = m_joinable.front().get();
                if (lead_it->isEnd()) {
                    setEnd();
                    return;
                }
                if (lead_it->getKey() != m_join_key) {
                    joinAll();
                }
            }
        }
    }
    
    template <typename key_t, bool UniqueKeys> void FT_JoinANDIterator<key_t, UniqueKeys>::_nextUnique()
    {
        if (m_direction > 0) {
            FT_Iterator<key_t> &lead_it = *m_joinable.front();
            do {
                ++lead_it;
                if(lead_it.isEnd()) {
                    setEnd();
                    return;
                }
            } while(lead_it.getKey() == m_join_key);
            joinAll();
        } else {
            FT_Iterator<key_t> &lead_it = *m_joinable.front();
            do {
                --lead_it;
                if(lead_it.isEnd()) {
                    setEnd();
                    return;
                }
            } while(lead_it.getKey() == m_join_key);
            joinAll();
        }
    }

	template <typename key_t, bool UniqueKeys> void FT_JoinANDIterator<key_t, UniqueKeys>::joinAll() 
    {
        if (m_direction > 0) {
            auto it = m_joinable.begin();
            assert (it!=m_joinable.end());
            assert ((*it).get());
            if ((**it).isEnd()) {
                setEnd();
                return; // end of input
            }
            m_join_key = (**it).getKey();
            ++it;
            while (it!=m_joinable.end()) {
                assert ((*it).get());
                if ((**it).isEnd()) {
                    setEnd();
                    return; // end of input
                }
                // try join leading iterator
                if ((**it).join(m_join_key, m_direction)) {
                    // compare not equal
                    key_t key = (**it).getKey();
                    if (key != m_join_key) {
                        m_join_key = key;
                        // this is the new leader ( push front using splice operation )
                        m_joinable.splice(m_joinable.begin(), m_joinable,it);
                        ++it;
                        continue;
                    }
                } else {
                    setEnd();
                    return; // end of input
                }
                ++it;
            }
        } else {
            auto it = m_joinable.begin();
            assert (it!=m_joinable.end());
            assert ((*it).get());
            if ((**it).isEnd()) {
                setEnd();
                return; // end of input
            }
            m_join_key = (**it).getKey();
            ++it;
            while (it!=m_joinable.end()) {
                assert((*it).get());
                if ((**it).isEnd()) {
                    setEnd();
                    return; // end of input
                }
                // try join leading iterator
                if ((**it).join(m_join_key, m_direction)) {
                    // compare not equal
                    key_t key = (**it).getKey();
                    if (key != m_join_key) {
                        m_join_key = key;
                        // this is the new leader ( push front using splice operation )
                        m_joinable.splice(m_joinable.begin(),m_joinable,it);
                        ++it;
                        continue;
                    }
                } else {
                    setEnd();
                    return; // end of input
                }
                ++it;
            }
        }
	}

	template <typename key_t, bool UniqueKeys>
	FT_JoinANDIterator<key_t, UniqueKeys>::FT_JoinANDIterator(std::list<std::unique_ptr<FT_Iterator<key_t> > > &&inner_iterators, int direction,
        bool is_end, key_t join_key, tag_cloned)        
        : m_direction(direction)
        , m_end(is_end)
        , m_join_key(join_key)        
	{
		for (auto &s: inner_iterators) {
			m_joinable.emplace_back(std::move(s));
		}
	}   

	template <typename key_t, bool UniqueKeys>
	void db0::FT_JoinANDIterator<key_t, UniqueKeys>
        ::scanQueryTree(std::function<void(const FT_Iterator<key_t> *it_ptr, int depth)> scan_function,
                int depth) const
    {
		scan_function(this, depth);
		for (auto &it: m_joinable) {
			(*it).scanQueryTree(scan_function, depth + 1);
		}
	}

    template <typename key_t, bool UniqueKeys> void db0::FT_JoinANDIterator<key_t, UniqueKeys>
        ::forAll(std::function<bool(const db0::FT_Iterator<key_t> &)> f) const
    {
        for (const auto &joinable: m_joinable) {
            if (!f(*joinable)) {
                break;
            }
        }
    }

    template <typename key_t, bool UniqueKeys>
    std::size_t db0::FT_JoinANDIterator<key_t, UniqueKeys>::getDepth() const {
        std::size_t max_inner = 0;
        for (auto &joinable: m_joinable) {
            max_inner = std::max(max_inner, joinable->getDepth());
        }
        return max_inner + 1u;
    }

    template <typename key_t, bool UniqueKeys>
    void db0::FT_JoinANDIterator<key_t, UniqueKeys>::stop() {
        this->setEnd();
    }

    template <typename key_t, bool UniqueKeys>
    bool db0::FT_JoinANDIterator<key_t, UniqueKeys>
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

    template <typename key_t, bool UniqueKeys>
    std::pair<bool, bool> db0::FT_JoinANDIterator<key_t, UniqueKeys>::mutateInner(const MutateFunction &f) {
        auto result = db0::FT_Iterator<key_t>::mutateInner(f);
        if (result.first) {
            return result;
        }
        bool was_mutated = false;
        bool was_end = false;
        for (auto &joinable: m_joinable) {
            auto inner_result = joinable->mutateInner(f);
            was_mutated |= inner_result.first;
            // invalidate the whole iterator when inner invalidated
            if (!inner_result.second) {
                was_end = true;
                break;
            }
        }
        if (was_end) {
            this->setEnd();
        } else {            
            joinAll();
        }
        return { was_mutated, !was_end };
    }

    template <typename key_t, bool UniqueKeys>
    void db0::FT_JoinANDIterator<key_t, UniqueKeys>::detach() {
        /* FIXME: implement
        for (auto &it: m_joinable) {
            it->detach();
        }
        */
    }

    template <typename key_t, bool UniqueKeys>
    const std::type_info &db0::FT_JoinANDIterator<key_t, UniqueKeys>::typeId() const
    {
        return typeid(self_t);
    }

    template <typename key_t, bool UniqueKeys>
    FTIteratorType db0::FT_JoinANDIterator<key_t, UniqueKeys>::getSerialTypeId() const
    {
        return FTIteratorType::JoinAnd;
    }
    
    template <typename key_t, bool UniqueKeys>
    void db0::FT_JoinANDIterator<key_t, UniqueKeys>::serializeFTIterator(std::vector<std::byte> &v) const
    {
        db0::serial::write(v, db0::serial::typeId<key_t>());
        db0::serial::write<bool>(v, UniqueKeys);
        db0::serial::write<std::int8_t>(v, m_direction);
        db0::serial::write<std::uint32_t>(v, m_joinable.size());
        for (const auto &it: m_joinable) {
            it->serialize(v);
        }
    }
    
    template <typename key_t, bool UniqueKeys>
    std::unique_ptr<FT_Iterator<key_t> >db0::FT_JoinANDIterator<key_t, UniqueKeys>::deserialize(Snapshot &workspace,
        std::vector<std::byte>::const_iterator &iter, std::vector<std::byte>::const_iterator end)
    {
        auto key_type_id = db0::serial::read<TypeIdType>(iter, end);
        if (key_type_id != db0::serial::typeId<key_t>()) {
            THROWF(db0::InternalException) << "Key type mismatch: " << key_type_id << " != " << db0::serial::typeId<key_t>()
                << THROWF_END;
        }
        bool unique_keys = db0::serial::read<bool>(iter, end);
        if (unique_keys != UniqueKeys) {
            THROWF(db0::InternalException) << "Unique keys mismatch: " << unique_keys << " != " << UniqueKeys
                << THROWF_END;
        }
        int direction = db0::serial::read<std::int8_t>(iter, end);
        std::uint32_t size = db0::serial::read<std::uint32_t>(iter, end);
        std::list<std::unique_ptr<FT_Iterator<key_t> > > inner_iterators;
        bool result = true;
        for (std::uint32_t i = 0; i < size; ++i) {
            auto inner_it = db0::deserializeFT_Iterator<key_t>(workspace, iter, end);
            if (inner_it) {
                inner_iterators.emplace_back(std::move(inner_it));
            } else {
                // no result if any of the inner iterators does not exist
                result = false;
            }            
        }
        
        if (!result) {
            return nullptr;
        }

        return std::make_unique<FT_JoinANDIterator<key_t, UniqueKeys> >(
            std::move(inner_iterators), direction
        );
    }
    
    template <typename key_t, bool UniqueKeys>
    double db0::FT_JoinANDIterator<key_t, UniqueKeys>::compareToImpl(const FT_IteratorBase &it) const
    {
        if (it.typeId() == this->typeId()) {
            return this->compareTo(reinterpret_cast<const FT_JoinANDIterator<key_t, UniqueKeys>&>(it));
        }
        
        if (m_joinable.size() == 1u) {
            return m_joinable.front()->compareTo(it);
        }
        // different iterators
        return 1.0;
    }
    
    template <typename key_t, bool UniqueKeys>
    double db0::FT_JoinANDIterator<key_t, UniqueKeys>::compareTo(const FT_JoinANDIterator<key_t, UniqueKeys> &other) const
    {
        if (m_joinable.size() != other.m_joinable.size()) {
            return 1.0;
        }
        std::list<const FT_Iterator<key_t>*> refs;
        for (auto ref = other.m_joinable.begin(),itend = other.m_joinable.end();ref!=itend;++ref) {
            refs.push_back((*ref).get());
        }

        // for each iterator from refs_1 pull the closest matching one from refs_2
        double result = 1.0;
        double p_diff = 1.0 / (double)m_joinable.size();
        for (auto &it: m_joinable) {
            double m_diff = std::numeric_limits<double>::max();
            auto it_min = refs.end();
            for (auto it2 = refs.begin(),itend = refs.end(); it2 != itend; ++it2) {
                double d = it->compareTo(**it2);
                if (d < m_diff) {
                    m_diff = d;
                    it_min = it2;
                }
            }
            refs.erase(it_min);
            result *= p_diff - (m_diff * p_diff);
        }
        return 1.0 - result;
    }
    
	template<typename key_t, bool UniqueKeys>
    FT_ANDIteratorFactory<key_t, UniqueKeys>::FT_ANDIteratorFactory() = default;

	template<typename key_t, bool UniqueKeys>
	FT_ANDIteratorFactory<key_t, UniqueKeys>::~FT_ANDIteratorFactory() = default;

	template<typename key_t, bool UniqueKeys>
	void FT_ANDIteratorFactory<key_t, UniqueKeys>::add(std::unique_ptr<FT_Iterator<key_t> > &&it_joinable) {
		if (it_joinable.get()) {
            m_joinable.push_back(std::move(it_joinable));
        }
	}

	template<typename key_t, bool UniqueKeys>
	std::unique_ptr<FT_Iterator<key_t> > FT_ANDIteratorFactory<key_t, UniqueKeys>::release(int direction, bool lazy_init) 
    {
		if (m_joinable.empty()) {
			// no iterators to join
			return nullptr;
		}
		if (m_joinable.size()==1u) {
			// single iterator - no use joining
			return std::move(m_joinable.front());
		}
		return std::unique_ptr<FT_Iterator<key_t> >(new FT_JoinANDIterator<key_t, UniqueKeys>(
			std::move(this->m_joinable), direction, lazy_init)
		);
	}
    
	/**
     * Number of underlying simple joinable iterators
     */
	template <typename key_t, bool UniqueKeys>
	std::size_t FT_ANDIteratorFactory<key_t, UniqueKeys>::size() const {
        return m_joinable.size();
	}

	template <typename key_t, bool UniqueKeys>
	bool FT_ANDIteratorFactory<key_t, UniqueKeys>::empty() const {
		return m_joinable.empty();
	}

	template <typename key_t, bool UniqueKeys>
	void FT_ANDIteratorFactory<key_t, UniqueKeys>::clear() {
		m_joinable.clear();
	}
        
    template class FT_JoinANDIterator<std::uint64_t, false>;
    template class FT_JoinANDIterator<std::uint64_t, true>;
    
    template class FT_ANDIteratorFactory<std::uint64_t, false>;
    template class FT_ANDIteratorFactory<std::uint64_t, true>;

}


