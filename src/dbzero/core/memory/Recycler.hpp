// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <deque>
#include "SlabAllocator.hpp"

namespace db0

{
    
    // The recycler class helps maintain the lifecycle of a limited number of
    // shared_ptr based resources (e.g. SlabAllocator instances)
    template <typename T> class Recycler
    {
    public:
        Recycler(unsigned int max_size = 256);

        void append(std::shared_ptr<T>);
        
        /**
         * Get the number of instances currently begin stored
        */
        std::size_t size() const;

        /**
         * Get the maximum number of instances that could be stored
        */
        std::size_t capacity() const;
        
        /**
         * Close / remove all instances that match the predicate
        */
        void close(std::function<bool(const T &)> predicate, bool only_first = false);
        void closeOne(std::function<bool(const T &)> predicate);
        void clear();
        
    private:
        const unsigned int m_max_size;
        std::deque<std::shared_ptr<T> > m_queue;
    };

    template <typename T> Recycler<T>::Recycler(unsigned int max_size)
        : m_max_size(max_size)
    {
    }
    
    template <typename T> void Recycler<T>::append(std::shared_ptr<T> instance) 
    {
        m_queue.push_back(instance);
        while (m_queue.size() > m_max_size) {
            m_queue.pop_front();
        }
    }
    
    template <typename T>
    std::size_t Recycler<T>::size() const {
        return m_queue.size();
    }

    template <typename T>
    std::size_t Recycler<T>::capacity() const {
        return m_max_size;
    }
    
    template <typename T>
    void Recycler<T>::close(std::function<bool(const T &)> predicate, bool only_first)
    {
        for (auto it = m_queue.begin(); it != m_queue.end();) {
            if (predicate(**it)) {
                it = m_queue.erase(it);
                if (only_first) {
                    break;
                }
            } else {
                ++it;
            }
        }
    }
    
    template <typename T>
    void Recycler<T>::closeOne(std::function<bool(const T &)> predicate) {
        close(predicate, true);
    }
    
    template <typename T>
    void Recycler<T>::clear() {
        m_queue.clear();
    }
    
}       