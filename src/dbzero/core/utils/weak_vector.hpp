// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <vector>

namespace db0

{

    // A vector-based collection of weak pointers with auto-cleanup
    template <typename T> class weak_vector
    {
    public:
        weak_vector() = default;
        
        // add a new weak pointer
        void push_back(std::shared_ptr<T> ptr)
        {
            this->cleanup();
            m_data.emplace_back(ptr);
        }
        
        // remove expired weak pointers
        // @return true if the vector is empty after cleanup
        bool cleanup()
        {
            if (m_data.empty()) {
                return true;
            }
            m_data.erase(std::remove_if(m_data.begin(), m_data.end(),
                [](const std::weak_ptr<T> &ptr) { return ptr.expired(); }), m_data.end());
            return m_data.empty();
        }

        // get the size of the vector
        std::size_t size() const {
            return m_data.size();
        }

        void clear() {
            m_data.clear();
        }

        void forEach(std::function<void(const T &)> f) const
        {
            for (const auto &ptr : m_data) {
                if (auto p = ptr.lock()) {                    
                    f(*p);                    
                }
            }
        }

        void forEach(std::function<void(T &)> f)
        {
            for (const auto &ptr : m_data) {
                if (auto p = ptr.lock()) {                    
                    f(*p);                    
                }
            }
        }

        bool empty() const {
            return m_data.empty();
        }

    private:
        std::vector<std::weak_ptr<T> > m_data;        
    };

}