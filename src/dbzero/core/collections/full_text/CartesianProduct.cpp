// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "CartesianProduct.hpp"
#include "FT_Iterator.hpp"
#include <stdexcept>

namespace db0

{

    template <typename T> bool isEqual(const std::vector<T> &first, const T *second)
    {
        for (std::size_t i = 0; i < first.size(); ++i) {
            if (first[i] != second[i]) {
                return false;
            }
        }
        return true;
    }

    template <typename key_t>
    CartesianProduct<key_t>::CartesianProduct(const std::vector<std::unique_ptr<FT_Iterator<key_t>>> &components, int direction)
        : m_direction(direction)
        , m_current_key(components.size())
    {
        unsigned int index = 0;
        for (auto &it: components) {
            m_components.push_back(it->beginTyped(direction));
            auto &last = m_components.back();
            if (!last->isEnd()) {
                m_current_key[index] = last->getKey();
            }
            m_overflow |= last->isEnd();
            ++index;
        }
    }

    template <typename key_t>
    CartesianProduct<key_t>::CartesianProduct(
        std::vector<std::unique_ptr<FT_Iterator<key_t>>> &&components, int direction)     
        : m_direction(direction)
        , m_current_key(components.size())
    {
        unsigned int index = 0;
        for (auto &it: components) {
            m_components.push_back(it->beginTyped(direction));
            auto &last = m_components.back();
            if (!last->isEnd()) {
                m_current_key[index] = last->getKey();
            }
            m_overflow |= last->isEnd();
            ++index;
        }
    }

    template <typename key_t>
    void CartesianProduct<key_t>::getKey(KeyStorageT &key) const
    {
        assureAttached();
        assert(!isEnd());
        if (key.size() != m_current_key.size()) {
            key.resize(m_current_key.size());
        }
        key = m_current_key;
    }
    
    template <typename key_t>
    const std::type_info &CartesianProduct<key_t>::typeId() const {
        return typeid(*this);
    }

    template <typename key_t>
    void CartesianProduct<key_t>::next(void * /*buf*/) {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    void CartesianProduct<key_t>::operator++() 
    {
        assureAttached();
        m_overflow = true;
        unsigned int index = 0;
        for (auto &it: m_components) {
            if (!m_overflow) {
                break;
            }
            ++(*it);
            m_overflow = (*it).isEnd();
            if (m_overflow) {
                it = (*it).beginTyped(m_direction);
            } 
            m_current_key[index] = (*it).getKey();            
            ++index;
        }
    }

    template <typename key_t>
    void CartesianProduct<key_t>::operator--() {
        throw std::runtime_error("Not implemented");    
    }
    
    template <typename key_t>
    bool CartesianProduct<key_t>::isEnd() const {
        assureAttached();
        return m_overflow;
    }
    
    template <typename key_t>
    typename CartesianProduct<key_t>::KeyT CartesianProduct<key_t>::getKey() const 
    {
        assureAttached();
        assert(!isEnd());
        // NOTE: key is from the internal buffer, valid only until next modification
        return const_cast<key_t*>(m_current_key.data());
    }
    
    template <typename key_t>
    bool CartesianProduct<key_t>::joinAt(unsigned int at, key_t key, bool reset, int direction)
    {
        auto &item = m_components[at];
        if (!reset && item->join(key, direction)) {
            return item->swapKey(m_current_key[at]);
        } else {
            item = item->beginTyped(direction);
            if (item->join(key, direction)) {
                m_current_key[at] = item->getKey();
            } else {
                item = item->beginTyped(direction);
                m_current_key[at] = item->getKey();
                // must advance higher-order components
                ++at;
                m_overflow = true;
                while (m_overflow && at < m_components.size()) {
                    m_overflow = false;
                    if (direction > 0) {
                        ++(*m_components[at]);
                    } else {
                        --(*m_components[at]);
                    }
                    if (m_components[at]->isEnd()) {
                        m_components[at] = m_components[at]->beginTyped(direction);
                        m_overflow = true;
                    }
                    m_current_key[at] = m_components[at]->getKey();
                    ++at;
                }
            }
            return true;
        }
    }
    
    template <typename key_t>
    bool CartesianProduct<key_t>::join(KeyT join_key, int direction) 
    {
        assureAttached();
        return joinImpl(join_key, direction);
    }

    template <typename key_t>
    bool CartesianProduct<key_t>::joinImpl(KeyT join_key, int direction)
    {
        assert(!m_overflow);
        unsigned int index = m_components.size();
        auto key = join_key + m_components.size();
        bool reset_next = false;
        for (unsigned int i = 0; i < m_components.size(); ++i) {
            --index;
            --key;
            reset_next |= joinAt(index, *key, reset_next, direction);
            if (m_overflow) {
                return false;
            }
        }
        return true;
    }

    template <typename key_t>
    void CartesianProduct<key_t>::joinBound(KeyT /*join_key*/) {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    std::pair<typename CartesianProduct<key_t>::KeyT, bool>
    CartesianProduct<key_t>::peek(KeyT /*join_key*/) const {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    bool CartesianProduct<key_t>::isNextKeyDuplicated() const {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename key_t>
    std::unique_ptr<FT_Iterator<typename CartesianProduct<key_t>::KeyT, typename CartesianProduct<key_t>::KeyStorageT> >
    CartesianProduct<key_t>::beginTyped(int direction) const {
        assureAttached();
        return std::make_unique<CartesianProduct<key_t>>(this->m_components, direction);
    }

    template <typename key_t>
    bool CartesianProduct<key_t>::limitBy(KeyT /*key*/) {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    std::ostream &CartesianProduct<key_t>::dump(std::ostream & /*os*/) const {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    void CartesianProduct<key_t>::stop() {
        m_overflow = true;
        for (auto &component: m_components) {
            component->stop();
        }
    }

    template <typename key_t>
    void CartesianProduct<key_t>::detach()
    {
        if (!m_is_detached) {
            m_detach_key = m_overflow ? std::optional<KeyStorageT> {} : std::make_optional(m_current_key);
            for (auto &component: m_components) {
                component->detach();
            }
            m_is_detached = true;
        }
    }

    template <typename key_t>
    void CartesianProduct<key_t>::assureAttached() const
    {
        if (m_is_detached) {
            const_cast<CartesianProduct<key_t> *>(this)->reattach();
        }
    }

    template <typename key_t>
    void CartesianProduct<key_t>::reattach()
    {
        m_is_detached = false;
        if (!m_detach_key) {
            m_overflow = true;
            return;
        }
        m_overflow = false;
        if (!joinImpl(m_detach_key->data(), m_direction)) {
            m_overflow = true;
        }
    }

    template <typename key_t>
    FTIteratorType CartesianProduct<key_t>::getSerialTypeId() const {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    double CartesianProduct<key_t>::compareToImpl(const FT_IteratorBase & /*it*/) const {
        throw std::runtime_error("Not implemented");
    }

    template <typename key_t>
    void CartesianProduct<key_t>::getSignature(std::vector<std::byte> & /*out*/) const {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename key_t>
    void CartesianProduct<key_t>::serializeFTIterator(std::vector<std::byte> & /*out*/) const {
        throw std::runtime_error("Not implemented");
    }
    
    template <typename key_t>
    bool CartesianProduct<key_t>::swapKey(KeyStorageT &key) const
    {
        assureAttached();
        if (isEqual(key, m_current_key.data())) {
            return false;
        }
        if (key.size() != m_current_key.size()) {
            key.resize(m_current_key.size());
        }
        key = m_current_key.data();
        return true;
    }
    
    // Explicit template instantiations
    template class CartesianProduct<UniqueAddress>;
    template class CartesianProduct<std::uint64_t>;
    
}
