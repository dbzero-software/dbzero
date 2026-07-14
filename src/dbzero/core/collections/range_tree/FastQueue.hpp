// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <array>
#include <cstddef>

namespace db0

{

    // Fixed-capacity FIO queue
    template <typename ItemT, std::size_t N = 2> class FastQueue
    {
    public:        
        void push(const ItemT& item);
        void pop();

        inline bool empty() const;

        inline const ItemT &head() const 
        {
            assert(!empty());
            return m_data[m_head];
        }

    private:
        std::array<ItemT, N> m_data;
        std::size_t m_head = 0;
        std::size_t m_tail = 0;            
    };

    template <typename ItemT, std::size_t N>
    void FastQueue<ItemT, N>::push(const ItemT& item)
    {
        assert(m_head != (m_tail + 1) % N);
        m_data[m_tail] = item;
        ++m_tail;
        if (m_tail == N) {
            m_tail = 0;
        }
    }

    template <typename ItemT, std::size_t N>
    void FastQueue<ItemT, N>::pop()
    {
        assert(!empty());        
        ++m_head;
        if (m_head == N) {
            m_head = 0;
        }        
    }
    
    template <typename ItemT, std::size_t N>
    bool FastQueue<ItemT, N>::empty() const {
        return m_head == m_tail;
    }
    
}
