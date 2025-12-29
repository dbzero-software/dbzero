// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

namespace db0

{
    
    // Cartesian-product vector - a simple wrapper over std::vector
    template <typename T>
    struct CP_Vector: public std::vector<T>
    {
        using super_t = std::vector<T>;
        using value_type = T;

        CP_Vector() = default;
        CP_Vector(const CP_Vector &) = default;
        CP_Vector(CP_Vector &&) noexcept = default;        
        CP_Vector &operator=(CP_Vector &&) noexcept = default;

        CP_Vector(std::size_t size): super_t(size) {}

        // Cast to the underlying pointer type
        operator const T*() const {
            return this->data();
        }
        
        // assign ALL values from the provided array (must be of the same size)
        void operator=(const T *values) {
            std::copy(values, values + this->size(), this->begin());
        }

        CP_Vector<T> &operator=(const CP_Vector<T> &other)
        {
            if (this->size() != other.size()) {
                this->resize(other.size());
            }
            this->assign(other.begin(), other.end());            
            return *this;
        }

        bool operator==(const T *values) const {
            return std::equal(this->begin(), this->end(), values);
        }

        bool operator!=(const T *values) const {
            return !(*this == values);
        }

        bool operator==(const CP_Vector<T> &other) const 
        {
            assert(this->size() == other.size());
            return std::equal(this->begin(), this->end(), other.begin());
        }   

        bool operator!=(const CP_Vector<T> &other) const {
            return !(*this == other);
        }

        bool operator<(const CP_Vector<T> &other) const 
        {
            assert(this->size() == other.size());
            // compare from last to first
            return std::lexicographical_compare(this->rbegin(), this->rend(), other.rbegin(), other.rend());
        } 
        
        bool operator>(const CP_Vector<T> &other) const
        {
            assert(this->size() == other.size());
            // compare from last to first
            return std::lexicographical_compare(other.rbegin(), other.rend(), this->rbegin(), this->rend());
        }
    };
    
}