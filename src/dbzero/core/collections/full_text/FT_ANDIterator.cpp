// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <cassert>
#include "FT_ANDIterator.hpp"
#include "FT_Serialization.hpp"
#include <dbzero/core/serialization/hash.hpp>

namespace db0

{

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::FT_JoinANDIterator(
        std::list<std::unique_ptr<FT_Iterator<key_t, key_storage_t> > > &&inner_iterators, int direction, bool lazy_init)        
        : FT_JoinANDIterator(this->nextUID(), std::move(inner_iterators), direction, lazy_init)
    {
    }
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::FT_JoinANDIterator(std::unique_ptr<FT_Iterator<key_t, key_storage_t> > &&it0, 
        std::unique_ptr<FT_Iterator<key_t, key_storage_t> > &&it1, int direction, bool lazy_init)
        : m_direction(direction)
        , m_joinable(std::move(it0), std::move(it1))
        , m_end(false)
	{        
        // skip initialization if the lazy init was requested
		if (!lazy_init) {
            // test end iterator condition
            for (auto &it : m_joinable) {
                if ((*it).isEnd()) {
                    setEnd();
                    return;
                }
            }
            (*m_joinable.front()).getKey(m_join_key);
            joinAll();
        }
	}

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
	FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::FT_JoinANDIterator(std::uint64_t uid, 
        std::list<std::unique_ptr<FT_Iterator<key_t, key_storage_t> > > &&inner_iterators, int direction, bool lazy_init)
        : super_t(uid)
        , m_direction(direction)
        , m_joinable(std::move(inner_iterators))
        , m_end(false)
    {
        // skip initialization if the lazy init was requested
        if (!lazy_init) {
            // test end iterator condition
            for (auto &it : m_joinable) {
                if ((*it).isEnd()) {
                    setEnd();
                    return;
                }
            }
            (*m_joinable.front()).getKey(m_join_key);
            joinAll();
        }
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::~FT_JoinANDIterator() = default;

	/**
	 * IFT_Iterator interface members
	 */
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	bool FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::isEnd() const {
		return m_end;
	}
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::operator++() 
    {
        assert(m_direction > 0);
        assert(!isEnd());
        if constexpr (UniqueKeys) {
            _nextUnique();
        } else {
            _next();
        }
	}

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::operator--() {
        this->_next(nullptr);
	}
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::_next(void *buf) 
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
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::next(void *buf) {
        this->_next(buf);
    }

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	key_t FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::getKey() const {
		return m_join_key;
	}

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    const db0::FT_Iterator<key_t, key_storage_t> &FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::getSimple() const {
        assert(!m_joinable.empty());
        return *m_joinable.front();
    }

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	bool FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::join(key_t join_key, int direction)
    {
		if (m_joinable.front()->join(join_key, direction)) {
            m_joinable.front()->getKey(m_join_key);
			joinAll();
			return !isEnd();
		} else {
			setEnd();
			return false;
		}
	}
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::joinBound(key_t key) 
    {
		for (auto it = m_joinable.begin(), end = m_joinable.end(); it != end; ++it) {
			// try join leading iterator
			assert(!(**it).isEnd());
			(**it).joinBound(key);
			(**it).getKey(m_join_key);
			if (m_join_key != key) {
				break;
			}
        }
	}

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	std::pair<key_t, bool> FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::peek(key_t join_key) const 
    {
		key_t lead_key = join_key;
		for (auto it = m_joinable.begin(),itend = m_joinable.end(); it != itend; ++it) {
			std::pair<key_t, bool> peek_result = (**it).peek(lead_key);
			if (!peek_result.second) {
				return { key_t(), false };
			}
			// join condition
			assert(peek_result.first <= lead_key);
			if (peek_result.first < lead_key) {
				lead_key = peek_result.first;
                // move to front and start over
				it = m_joinable.swapFront(it);
			}
		}
		return std::make_pair(lead_key, true);
	}
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	std::unique_ptr<FT_Iterator<key_t, key_storage_t> > 
    FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::beginTyped(int direction) const
    {
		// collect joinable (must sync)
		std::list<std::unique_ptr<FT_IteratorT> > temp;
        for (auto it = m_joinable.begin(),itend = m_joinable.end();it != itend;++it) {
            temp.emplace_back((**it).beginTyped(direction));
        }
		return std::unique_ptr<self_t>(
            new self_t(this->m_uid, std::move(temp), direction, false)
        );
	}
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	bool FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::limitBy(key_t key) 
    {
		for (auto it = m_joinable.begin(),itend = m_joinable.end(); it != itend; ++it) {
			if (!(**it).limitBy(key)) {
				setEnd();
				return false;
			}
		}
		return true;
	}

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	std::ostream &FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::dump(std::ostream &os) const 
    {
		os << "AND@" << this << "[";
		bool is_first = true;
		for (auto it = m_joinable.begin(),itend = m_joinable.end(); it != itend; ++it) {
			if (!is_first) {
				os << ",";
			}
			(**it).dump(os);
			is_first = false;
		}
		return os << "]";
	}

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	const FT_IteratorBase *FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::find(std::uint64_t uid) const 
    {
		// self-check first
		if (this->m_uid == uid) {
			return this;
		}
		// find in joinables
		for (auto it_sub = m_joinable.begin(), itend = m_joinable.end(); it_sub != itend; ++it_sub) {
			const FT_IteratorBase *it_filter = (**it_sub).find(uid);
			if (it_filter) {
				return it_filter;
			}
		}
		// no iterator of given UID found
		return nullptr;
	}

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::setEnd() {
		m_end = true;
	}
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t> 
    void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::_next()
    {
        // Try advancing one of the iterators which holds a duplicate key
        for (auto &joinable: m_joinable) {
            if ((*joinable).isNextKeyDuplicated()) {
                joinable.nextKey(m_direction);
                return;
            }
        }
            
        // advance the head iterator otherwise
        if (!m_joinable.front().nextKey(m_direction, &m_join_key)) {
            setEnd();
            return;
        }
        joinAll();
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t> 
    void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::_nextUnique()
    {
        if (!m_joinable.front().nextUniqueKey(m_direction, &m_join_key)) {
            setEnd();
            return;
        }
        joinAll();
    }

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
    void FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::joinAll()
    {
        auto it = m_joinable.begin(), end = m_joinable.end();
        assert(it != end);
        assert(!(**it).isEnd());
        assert(m_join_key == (**it).getKey());
        for (;;) {
            ++it;
            if (it == end) {
                break;
            }
            if (!(**it).join(m_join_key, m_direction)) {
                setEnd();
                return;
            }
            // replace m_join_key with the iterator's current key if it is different
            if ((**it).swapKey(m_join_key)) {
                // continue with this iterator as the head
                it = m_joinable.swapFront(it);
            }
        }
    }
    
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>
        ::scanQueryTree(std::function<void(const FT_Iterator<key_t, key_storage_t> *it_ptr, int depth)> scan_function,
                int depth) const
    {
		scan_function(this, depth);
		for (auto &it: m_joinable) {
			(*it).scanQueryTree(scan_function, depth + 1);
		}
	}
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t> void db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>
        ::forAll(std::function<bool(const db0::FT_Iterator<key_t, key_storage_t> &)> f) const
    {
        for (const auto &joinable: m_joinable) {
            if (!f(*joinable)) {
                break;
            }
        }
    }

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    std::size_t db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::getDepth() const 
    {
        std::size_t max_inner = 0;
        for (auto &joinable: m_joinable) {
            max_inner = std::max(max_inner, (*joinable).getDepth());
        }
        return max_inner + 1u;
    }

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    void db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::stop() {
        this->setEnd();
    }

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    bool db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>
        ::findBy(const std::function<bool(const db0::FT_Iterator<key_t, key_storage_t> &)> &f) const 
    {
        if (!FT_IteratorT::findBy(f)) {
            return false;
        }
        for (const auto &joinable: m_joinable) {
            if (!(*joinable).findBy(f)) {
                return false;
            }
        }
        return true;
    }

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    std::pair<bool, bool> db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::mutateInner(const MutateFunction &f) {
        auto result = db0::FT_Iterator<key_t, key_storage_t>::mutateInner(f);
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
            (*m_joinable.front()).getKey(m_join_key);
            joinAll();
        }
        return { was_mutated, !was_end };
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    void db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::detach() 
    {
        /* FIXME: implement
        for (auto &it: m_joinable) {
            it->detach();
        }
        */
    }

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    const std::type_info &db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::typeId() const
    {
        return typeid(self_t);
    }

    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    FTIteratorType db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::getSerialTypeId() const
    {
        return FTIteratorType::JoinAnd;
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    void db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::serializeFTIterator(std::vector<std::byte> &v) const
    {
        db0::serial::write(v, db0::serial::typeId<key_t>());
        db0::serial::write<bool>(v, UniqueKeys);
        db0::serial::write<std::int8_t>(v, m_direction);
        db0::serial::write<std::uint32_t>(v, m_joinable.size());
        for (const auto &it: m_joinable) {
            (*it).serialize(v);
        }
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    std::unique_ptr<FT_Iterator<key_t, key_storage_t> > db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::deserialize(Snapshot &workspace,
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
        std::list<std::unique_ptr<FT_IteratorT> > inner_iterators;
        bool result = true;
        for (std::uint32_t i = 0; i < size; ++i) {
            auto inner_it = db0::deserializeFT_Iterator<key_t, key_storage_t>(workspace, iter, end);
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

        return std::make_unique<self_t>(std::move(inner_iterators), direction);
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    double db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::compareToImpl(const FT_IteratorBase &it) const
    {
        if (it.typeId() == this->typeId()) {
            return this->compareTo(reinterpret_cast<const self_t&>(it));
        }
        
        if (m_joinable.size() == 1u) {
            return (*m_joinable.front()).compareTo(it);
        }
        // different iterators
        return 1.0;
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    double db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::compareTo(const FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t> &other) const
    {
        if (m_joinable.size() != other.m_joinable.size()) {
            return 1.0;
        }
        std::list<const FT_IteratorT*> refs;
        for (auto ref = other.m_joinable.begin(), itend = other.m_joinable.end();ref != itend;++ref) {
            refs.push_back(ref->m_iterator);
        }
        
        // for each iterator from refs_1 pull the closest matching one from refs_2
        double result = 1.0;
        double p_diff = 1.0 / (double)m_joinable.size();
        for (auto &it: m_joinable) {
            double m_diff = std::numeric_limits<double>::max();
            auto it_min = refs.end();
            for (auto it2 = refs.begin(),itend = refs.end(); it2 != itend; ++it2) {
                double d = (*it).compareTo(**it2);
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
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    void db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::getSignature(std::vector<std::byte> &v) const 
    {
        // combine signatures of all joinable iterators
        std::vector<std::byte> buf;
        for (const auto &it: m_joinable) {
            (*it).getSignature(buf);
        }        
        // sort signatures to make the order invariant
        sortSignatures(buf);
        // generate signature as a hash of all compound signatures
        db0::serial::sha256(buf, v);
    }
    
    template <typename key_t, bool UniqueKeys, typename key_storage_t>
    bool db0::FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>::isNextKeyDuplicated() const
    {
        if constexpr (UniqueKeys) {        
            return false;
        } else {
            for (auto &joinable: m_joinable) {
                if ((*joinable).isNextKeyDuplicated()) {
                    return true;
                }
            }
            return false;
        }
    }
    
	template<typename key_t, bool UniqueKeys, typename key_storage_t>
    FT_ANDIteratorFactory<key_t, UniqueKeys, key_storage_t>::FT_ANDIteratorFactory() = default;

	template<typename key_t, bool UniqueKeys, typename key_storage_t>
	FT_ANDIteratorFactory<key_t, UniqueKeys, key_storage_t>::~FT_ANDIteratorFactory() = default;
    
	template<typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_ANDIteratorFactory<key_t, UniqueKeys, key_storage_t>::add(std::unique_ptr<FT_Iterator<key_t, key_storage_t> > &&it_joinable) 
    {
		if (it_joinable.get()) {
            m_joinable.push_back(std::move(it_joinable));
        } else {
            // query yields no results
            m_invalidated = true;
        }
	}
    
	template<typename key_t, bool UniqueKeys, typename key_storage_t>
	std::unique_ptr<FT_Iterator<key_t, key_storage_t> >
    FT_ANDIteratorFactory<key_t, UniqueKeys, key_storage_t>::release(int direction, bool lazy_init) 
    {
		if (m_invalidated || m_joinable.empty()) {
			// no iterators to join
			return nullptr;
		}
		if (m_joinable.size() == 1u) {
			// single iterator - no use joining
			return std::move(m_joinable.front());
		}
		return std::unique_ptr<FT_IteratorT>(new FT_JoinANDIterator<key_t, UniqueKeys, key_storage_t>(
			std::move(this->m_joinable), direction, lazy_init)
		);
	}
    
	/**
     * Number of underlying simple joinable iterators
     */
	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	std::size_t FT_ANDIteratorFactory<key_t, UniqueKeys, key_storage_t>::size() const {
        return m_joinable.size();
	}

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	bool FT_ANDIteratorFactory<key_t, UniqueKeys, key_storage_t>::empty() const {
		return m_joinable.empty();
	}

	template <typename key_t, bool UniqueKeys, typename key_storage_t>
	void FT_ANDIteratorFactory<key_t, UniqueKeys, key_storage_t>::clear() {
		m_joinable.clear();
	}

    template class FT_JoinANDIterator<std::uint64_t, false>;
    template class FT_JoinANDIterator<std::uint64_t, true>;

    template class FT_JoinANDIterator<UniqueAddress, false>;
    template class FT_JoinANDIterator<UniqueAddress, true>;

    template class FT_ANDIteratorFactory<std::uint64_t, false>;
    template class FT_ANDIteratorFactory<std::uint64_t, true>;

    template class FT_ANDIteratorFactory<UniqueAddress, false>;
    template class FT_ANDIteratorFactory<UniqueAddress, true>;
    
    // CartesianProduct specific explicit instantiations
    template class FT_JoinANDIterator<const std::uint64_t*, false, CP_Vector<std::uint64_t> >;
    template class FT_JoinANDIterator<const std::uint64_t*, true, CP_Vector<std::uint64_t> >;

    template class FT_JoinANDIterator<const UniqueAddress*, false, CP_Vector<UniqueAddress> >;
    template class FT_JoinANDIterator<const UniqueAddress*, true, CP_Vector<UniqueAddress> >;

    template class FT_ANDIteratorFactory<const std::uint64_t*, false, CP_Vector<std::uint64_t> >;
    template class FT_ANDIteratorFactory<const std::uint64_t*, true, CP_Vector<std::uint64_t> >;

    template class FT_ANDIteratorFactory<const UniqueAddress*, false, CP_Vector<UniqueAddress> >;
    template class FT_ANDIteratorFactory<const UniqueAddress*, true, CP_Vector<UniqueAddress> >;
        
}
