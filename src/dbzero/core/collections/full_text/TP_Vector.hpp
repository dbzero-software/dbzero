// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <array>
#include <vector>

namespace db0

{

    // Tag-product iterator's key
    // NOTE: only the 2nd element (TAG) alone is assumed as the key identifier
    template <typename T>
    struct TP_Key: public std::array<T, 2>
    {
        using super_t = std::array<T, 2>;
        using value_type = T;

        // Cast to the underlying pointer type
        operator const T*() const {
            return this->data();
        }
        
        // assign ALL values from the provided array (must be of the same size)
        void operator=(const T *values) {
            std::copy(values, values + this->size(), this->begin());
        }
        
        bool operator==(const T *values) const {
            return (*this)[1] == values[1];
        }

        bool operator!=(const T *values) const {
            return (*this)[1] != values[1];
        }

        bool operator==(const TP_Key<T> &other) const {
            return (*this)[1] == other[1];
        }   

        bool operator!=(const TP_Key<T> &other) const {
            return (*this)[1] != other[1];
        }
    };
    
    // TP_Vector represents multiple joined TP_Key values    
    template <typename T>
    struct TP_Vector: public std::vector<T>
    {
        using super_t = std::vector<T>;
        using value_type = T;

        TP_Vector() = default;
        TP_Vector(const TP_Vector &) = default;
        TP_Vector(TP_Vector &&) noexcept = default;        
        TP_Vector &operator=(TP_Vector &&) noexcept = default;

        TP_Vector(std::size_t size): super_t(size) {}

        // Cast to the underlying pointer type
        operator const T*() const {
            return this->data();
        }
        
        // assign ALL values from the provided array (must be of the same size)
        void operator=(const T *values) {
            std::copy(values, values + this->size(), this->begin());
        }

        bool operator==(const T *values) const {
            return std::equal(this->begin(), this->end(), values);
        }
        
        TP_Vector<T> &operator=(const TP_Vector<T> &other)
        {
            if (this->size() != other.size()) {
                this->resize(other.size());
            }
            this->assign(other.begin(), other.end());            
            return *this;
        }
    };
    
}