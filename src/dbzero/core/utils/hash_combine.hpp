// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <functional>

namespace db0

{

    template <class T>
    inline void hash_combine(std::size_t& seed, const T& v)
    {
        std::hash<T> hasher;
        seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
    }

    std::size_t make_hash ();

    /// create combined hash for arguments using std::hash
    template <typename T, typename ...Args> std::size_t make_hash(T&& arg, Args&& ...args)
    {
        std::size_t seed = db0::make_hash(std::forward<Args>(args)...);
        hash_combine(seed, arg);
        return seed;
    }

} 
