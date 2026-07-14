// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

DB0_PACKED_BEGIN

    // tuple/array of N-numbers of type T
    template <typename T, unsigned int N> struct DB0_PACKED_ATTR num_pack
    {
        T data[N];

        num_pack() = default;
        num_pack(const num_pack &other) = default;        
        num_pack(num_pack &&other) noexcept = default;

        // construct from N numbers (N >= 2)
        template <typename... Args> num_pack(T first, T second, Args... args) : data{first, second, args...} {}
        
        num_pack &operator=(const num_pack &other) = default;
        
        num_pack &operator=(num_pack &&other) noexcept = default;

        bool operator<(const num_pack &other) const;

        bool operator==(const num_pack &other) const;

        bool operator!=(const num_pack &other) const;

        inline T operator[](unsigned int i) const {
            return data[i];
        }
    };
    
    template <typename T, unsigned int N> bool num_pack<T, N>::operator<(const num_pack &other) const
    {
        for (unsigned int i = 0; i < N; i++) {
            if (data[i] < other.data[i])
                return true;
            if (data[i] > other.data[i])
                return false;
        }
        return false;
    }

    template <typename T, unsigned int N> bool num_pack<T, N>::operator==(const num_pack &other) const
    {
        for (unsigned int i = 0; i < N; i++) {
            if (data[i] != other.data[i])
                return false;
        }
        return true;
    }

    template <typename T, unsigned int N> bool num_pack<T, N>::operator!=(const num_pack &other) const {
        return !(*this == other);
    }
        
DB0_PACKED_END

}
