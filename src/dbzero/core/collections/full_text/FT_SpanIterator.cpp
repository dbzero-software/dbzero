// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FT_SpanIterator.hpp"
#include <cstring>

namespace db0

{

    template <typename KeyT> std::uint64_t valueOf(KeyT key) {
        return key;
    }

    template <> std::uint64_t valueOf(UniqueAddress key) {
        return key.getOffset();
    }

    template <typename KeyT> std::uint64_t spanOf(KeyT key, unsigned int span_shift) {
        return key >> span_shift;
    }

    // UniqueAddress specialization
    template <> std::uint64_t spanOf(UniqueAddress key, unsigned int span_shift) {
        return key.getOffset() >> span_shift;
    }

    template <typename KeyT> KeyT boundOf(KeyT key, unsigned int span_shift, int direction)
    {
        if (direction > 0) {
            return (key >> span_shift) << span_shift; 
        } else {
            return (((key >> span_shift) + 1) << span_shift) - 1;
        }
    }
    
    // UniqueAddress specialization
    template <> UniqueAddress boundOf(UniqueAddress key, unsigned int span_shift, int direction) 
    {
        auto offset = boundOf(key.getOffset(), span_shift, direction);
        if (direction > 0) {
            return { Address::fromOffset(offset), 0 };
        } else {
            return { Address::fromOffset(offset), UniqueAddress::INSTANCE_ID_MAX };
        }
    }

    template <typename KeyT>
    FT_SpanIterator<KeyT>::FT_SpanIterator(std::unique_ptr<FT_Iterator<KeyT> > &&inner_it, unsigned int span_shift,
        int direction)
        : m_inner_it(std::move(inner_it))
        , m_span_shift(span_shift)
        , m_span_size(1u << span_shift)
        , m_direction(direction)        
    {    
    }
    
    template <typename KeyT>
    const std::type_info &FT_SpanIterator<KeyT>::typeId() const {
        return typeid(*this);
    }

    template <typename KeyT> 
    bool FT_SpanIterator<KeyT>::isEnd() const {
        return m_inner_it->isEnd();
    }

    template <typename KeyT> 
    void FT_SpanIterator<KeyT>::next(void *buf) 
    {
        if (m_key) {
            std::memcpy(buf, &m_key.value(), sizeof(KeyT));
            m_key = {};
            m_inner_it->next();
        } else {
            m_inner_it->next(buf);
        }
    }

    template <typename KeyT> void FT_SpanIterator<KeyT>::operator++() {
        m_inner_it->operator++();
    }    

    template <typename KeyT> void FT_SpanIterator<KeyT>::operator--() {
        m_inner_it->operator--();
    }

    template <typename KeyT> KeyT FT_SpanIterator<KeyT>::getKey() const {
        return this->_getKey();
    }
    
    template <typename KeyT> KeyT FT_SpanIterator<KeyT>::_getKey() const
    {
        if (m_key) {
            return *m_key;
        }
        return m_inner_it->getKey();        
    }
    
    template <typename KeyT>
    std::unique_ptr<FT_IteratorBase> FT_SpanIterator<KeyT>::begin() const {
        return std::make_unique<FT_SpanIterator<KeyT> >(m_inner_it->beginTyped(), m_span_shift, m_direction);
    }

    template <typename KeyT>
    std::unique_ptr<FT_Iterator<KeyT> > FT_SpanIterator<KeyT>::beginTyped(int direction) const {
        return std::make_unique<FT_SpanIterator<KeyT> >(m_inner_it->beginTyped(), m_span_shift, m_direction);        
    }
    
    template <typename KeyT>
    bool FT_SpanIterator<KeyT>::join(KeyT join_key, int direction)
    {
        // hold position within the same span
        if (spanOf(join_key, m_span_shift) == spanOf(_getKey(), m_span_shift)) {
            m_key = join_key;
            return true;
        }
        
        if (!m_inner_it->join(boundOf(join_key, m_span_shift, direction), direction)) {
            m_key = {};
            return false;
        }

        m_key = m_inner_it->getKey();
        // if the join key is within the actual key's span then use it
        if (spanOf(join_key, m_span_shift) == spanOf(*m_key, m_span_shift)) {
            m_key = join_key;
        }
        return true;
    }

    template <typename KeyT>
    void FT_SpanIterator<KeyT>::joinBound(KeyT join_key) {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    std::pair<KeyT, bool> FT_SpanIterator<KeyT>::peek(KeyT join_key) const {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT>
    bool FT_SpanIterator<KeyT>::isNextKeyDuplicated() const {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT>
    bool FT_SpanIterator<KeyT>::limitBy(KeyT key) {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename KeyT>
    void FT_SpanIterator<KeyT>::stop() 
    {
        m_key = {};
        m_inner_it->stop();
    }
    
    template <typename KeyT>
    double FT_SpanIterator<KeyT>::compareToImpl(const FT_IteratorBase &it) const 
    {
		if (this->typeId() == it.typeId()) {
			return compareToImpl(reinterpret_cast<const self_t &>(it));
		}
		return 1.0;
    }

    template <typename KeyT>
    double FT_SpanIterator<KeyT>::compareToImpl(const FT_SpanIterator<KeyT> &other) const
    {
        if (m_span_shift != other.m_span_shift) {
            return 1.0;
        }
        return m_inner_it->compareTo(*other.m_inner_it);
    }
    
    template <typename KeyT>
    std::ostream& FT_SpanIterator<KeyT>::dump(std::ostream &os) const 
    {
        os << "SPAN[";
        m_inner_it->dump(os) << "]";
        return os;
    }

    template <typename KeyT>
    void FT_SpanIterator<KeyT>::getSignature(std::vector<std::byte>&) const {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    db0::FTIteratorType FT_SpanIterator<KeyT>::getSerialTypeId() const {
        throw std::runtime_error("Not implemented");
    }

    template <typename KeyT>
    void FT_SpanIterator<KeyT>::serializeFTIterator(std::vector<std::byte>&) const {
        throw std::runtime_error("Not implemented");
    }

    template class FT_SpanIterator<std::uint64_t>;
    template class FT_SpanIterator<UniqueAddress>;

}