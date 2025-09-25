#pragma once

#include <array>

namespace db0

{

    // Tag-product iterator's key
    // NOTE: only the 1-st element (TAG) alone is assumed as the key identifier
    template <typename T>
    struct TP_Vector: public std::array<T, 2>
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
            return (*this)[0] == values[0];
        }

        bool operator!=(const T *values) const {
            return (*this)[0] != values[0];
        }

        bool operator==(const TP_Vector<T> &other) const {
            return (*this)[0] == other[0];
        }   

        bool operator!=(const TP_Vector<T> &other) const {
            return (*this)[0] != other[0];
        }
    };

}