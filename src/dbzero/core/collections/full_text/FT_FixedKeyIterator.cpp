// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FT_FixedKeyIterator.hpp"
#include <algorithm>

namespace db0

{

    template <typename KeyT> FT_FixedKeyIterator<KeyT>::FT_FixedKeyIterator(const KeyT *begin, const KeyT *end,
        int direction, bool is_sorted)
        : m_sorted_keys(getSorted(begin, end, is_sorted))
        , m_keys(m_sorted_keys.data(), m_sorted_keys.size())
        , m_direction(direction)        
    {
        if (m_direction > 0) {
            m_current = m_keys.begin();
        } else {
            m_current = m_keys.end();
            if (m_current != m_keys.begin()) {
                --m_current;
            }
        }
    }
    
    template <typename KeyT>
    std::vector<KeyT> FT_FixedKeyIterator<KeyT>::getSorted(const KeyT *begin, const KeyT *end, bool is_sorted) 
    {
        assert(begin < end);
        if (is_sorted) {
            return std::vector<KeyT>(begin, end);
        } else {
            std::vector<KeyT> sorted(begin, end);
            std::sort(sorted.begin(), sorted.end());
            return sorted;
        }
    }
    
    template <typename KeyT> 
    const std::type_info &FT_FixedKeyIterator<KeyT>::FT_FixedKeyIterator::typeId() const
    {
        return typeid(self_t);
    }
    
    template <typename KeyT> bool FT_FixedKeyIterator<KeyT>::isEnd() const {
        return m_current == m_keys.end();
    }

    template <typename KeyT> void FT_FixedKeyIterator<KeyT>::next(void *buf) 
    {
        assert(!isEnd());
        if (buf) {
            *static_cast<KeyT *>(buf) = *m_current;
        }
        if (m_direction > 0) {
            ++m_current;
        } else {
            if (m_current == m_keys.begin()) {
                m_current = m_keys.end();
            } else {
                --m_current;
            }
        }
    }

	template <typename KeyT> void FT_FixedKeyIterator<KeyT>::operator++()
    {
        assert(!isEnd());
        assert(m_direction > 0);        
        ++m_current;
    }

	template <typename KeyT> void FT_FixedKeyIterator<KeyT>::operator--()
    {
        assert(!isEnd());
        assert(m_direction < 0);        
        if (m_current == m_keys.begin()) {
            m_current = m_keys.end();
        } else {
            --m_current;
        }
    }
    
    template <typename KeyT> KeyT FT_FixedKeyIterator<KeyT>::getKey() const 
    {
        assert(!isEnd());
        return *m_current;
    }
    
    template <typename KeyT> std::unique_ptr<FT_IteratorBase> FT_FixedKeyIterator<KeyT>::begin() const 
    {
        return std::make_unique<self_t>(&m_sorted_keys.front(), &m_sorted_keys.back() + 1,
            m_direction, true);            
    }
    
    template <typename KeyT> std::unique_ptr<FT_Iterator<KeyT> >
    FT_FixedKeyIterator<KeyT>::beginTyped(int direction) const
    {
        return std::make_unique<self_t>(&m_sorted_keys.front(), &m_sorted_keys.back() + 1,
            direction, true);
    }
    
    template <typename KeyT> bool FT_FixedKeyIterator<KeyT>::join(KeyT join_key, int direction)
    {   
        auto result = m_keys.join((direction > 0 ? m_keys.begin() : m_keys.end()), join_key, direction);
        if (result == m_keys.end()) {
            m_current = m_keys.end();
            return false;
        }
        if (direction > 0) {
            m_current = std::max(m_current, result);
        } else {    
            m_current = std::min(m_current, result);
        }
        return true;
    }
    
    template <typename KeyT> void FT_FixedKeyIterator<KeyT>::joinBound(KeyT join_key) {
        m_current = m_keys.joinBound(m_keys.begin(), join_key);        
    }

    template <typename KeyT> std::pair<KeyT, bool>
    FT_FixedKeyIterator<KeyT>::peek(KeyT join_key) const
    {
        auto result = m_keys.join(m_current, join_key, m_direction);
        if (result == m_keys.end()) {
            return std::make_pair(KeyT(), false);
        } else {
            return { *result, true };
        }
    }
    
    template <typename KeyT> bool FT_FixedKeyIterator<KeyT>::isNextKeyDuplicated() const
    {
        assert(!isEnd());
        auto next = m_current;
        if (m_direction > 0) {
            ++next;
            if (next == m_keys.end()) {
                return false;
            }
            return *next == *m_current;
        } else {
            if (m_current == m_keys.begin()) {
                return false;
            }
            --next;
            return *next == *m_current;
        }
    }

    template <typename KeyT> bool FT_FixedKeyIterator<KeyT>::limitBy(KeyT) {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT> void FT_FixedKeyIterator<KeyT>::stop() {
        m_current = m_keys.end();
    }

    template <typename KeyT> std::ostream &FT_FixedKeyIterator<KeyT>::dump(std::ostream &os) const
    {
        os << "FIXED@" << this << "[" << m_current - m_keys.begin() << "/" << m_keys.size() << "]";
        return os;
    }
    
    template <typename KeyT> FTIteratorType FT_FixedKeyIterator<KeyT>::getSerialTypeId() const {
        return FTIteratorType::FixedKey;
    }

    template <typename KeyT> double FT_FixedKeyIterator<KeyT>::compareToImpl(const FT_IteratorBase &it) const 
    {
		if (this->typeId() == it.typeId()) {
			return compareTo(reinterpret_cast<const self_t &>(it));
		}
		return 1.0;
    }

    template <typename KeyT> double FT_FixedKeyIterator<KeyT>::compareTo(const self_t &other) const
    {
        if (this->size() != other.size()) {
            return 1.0;
        }
        for (std::size_t i = 0; i < this->size(); ++i) {
            if (m_sorted_keys[i] != other.m_sorted_keys[i]) {
                return 1.0;
            }
        }
        return 0.0;
    }

    template <typename KeyT> std::size_t FT_FixedKeyIterator<KeyT>::size() const {
        return m_sorted_keys.size();
    }
    
    template <typename KeyT> void FT_FixedKeyIterator<KeyT>::getSignature(std::vector<std::byte> &v) const {
		// get the serializable's signature
		db0::serial::getSignature(*this, v);
    }
    
    template <typename KeyT> void FT_FixedKeyIterator<KeyT>::serializeFTIterator(std::vector<std::byte> &v) const
    {
		using TypeIdType = decltype(db0::serial::typeId<void>());

        db0::serial::write<std::int8_t>(v, m_direction);
        db0::serial::write<TypeIdType>(v, db0::serial::typeId<KeyT>());
        db0::serial::write<std::uint32_t>(v, m_sorted_keys.size());
        for (const auto &key : m_sorted_keys) {
            db0::serial::write(v, key);
        }        
    }

    template class FT_FixedKeyIterator<std::uint64_t>;
    template class FT_FixedKeyIterator<UniqueAddress>;

}