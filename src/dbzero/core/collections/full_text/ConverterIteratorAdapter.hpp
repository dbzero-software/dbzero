// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <iterator>
#include <type_traits>

namespace db0

{
    
    template<typename AdaptedIteratorT, typename GetFN>
    class ConverterIteratorAdapter {
        AdaptedIteratorT m_it;
        GetFN m_fun;

    public:
        using difference_type = typename std::iterator_traits<AdaptedIteratorT>::difference_type;
        using iterator_category = typename std::iterator_traits<AdaptedIteratorT>::iterator_category;
        using reference = std::invoke_result_t<GetFN, typename std::iterator_traits<AdaptedIteratorT>::reference>;
        using value_type = std::decay_t<reference>;
        using pointer = std::remove_reference_t<reference>*;
        
        explicit ConverterIteratorAdapter(const AdaptedIteratorT &it, const GetFN &fun = GetFN())
        : m_it(it), m_fun(fun)
        {}
        
        ConverterIteratorAdapter(const ConverterIteratorAdapter &other)
        : m_it(other.m_it), m_fun(other.m_fun)
        {}
        
        ConverterIteratorAdapter(ConverterIteratorAdapter &&other)
        : m_it(std::move(other.m_it)), m_fun(std::move(other.m_fun))
        {}
        
        reference operator*() const {
            return m_fun(*m_it);
        }
        
        ConverterIteratorAdapter& operator=(const ConverterIteratorAdapter &other) {
            m_it = other.m_it;
            m_fun = other.m_fun;
            return *this;
        }
        
        ConverterIteratorAdapter& operator=(ConverterIteratorAdapter &&other) {
            m_it = std::move(other.m_it);
            m_fun = std::move(other.m_fun);
            return *this;
        }
        
        bool operator==(const ConverterIteratorAdapter &other) const {
            return m_it == other.m_it;
        }
        
        bool operator!=(const ConverterIteratorAdapter &other) const {
            return m_it != other.m_it;
        }
        
        bool operator<(const ConverterIteratorAdapter &other) const {
            return m_it < other.m_it;
        }
        
        bool operator>(const ConverterIteratorAdapter &other) const {
            return m_it > other.m_it;
        }
        
        ConverterIteratorAdapter& operator++() {
            ++m_it;
            return *this;
        }
        
        ConverterIteratorAdapter& operator--() {
            --m_it;
            return *this;
        }
        
        ConverterIteratorAdapter operator++(int) {
            ConverterIteratorAdapter tmp(*this);
            ++(*this);
            return tmp;
        }
        
        ConverterIteratorAdapter operator--(int) {
            ConverterIteratorAdapter tmp(*this);
            --(*this);
            return tmp;
        }
        
        ConverterIteratorAdapter& operator+=(difference_type diff) {
            m_it += diff;
            return *this;
        }
        
        ConverterIteratorAdapter& operator-=(difference_type diff) {
            m_it -= diff;
            return *this;
        }
        
        ConverterIteratorAdapter operator+(difference_type diff) const {
            return ConverterIteratorAdapter(m_it + diff, m_fun);
        }
        
        ConverterIteratorAdapter operator-(difference_type diff) const {
            return ConverterIteratorAdapter(m_it - diff, m_fun);
        }
        
        difference_type operator-(const ConverterIteratorAdapter &other) const {
            return m_it - other.m_it;
        }
    };
    
}
