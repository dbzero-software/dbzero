// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <type_traits>
#include <ostream>
#include <utility>

namespace db0 

{

    template <typename T>
    struct ident{
        typedef T type;
    };

    template<typename... TN>
    struct sizeof_
    {};

    template<>
    struct sizeof_<>
        : public std::integral_constant<std::size_t, 0>
    {};

    template<typename T1, typename... TN>
    struct sizeof_<T1, TN...>
        : public std::integral_constant<std::size_t, 1+sizeof_<TN...>::value>
    {};

} 

namespace std 

{

    template<typename T1, typename T2>
    ostream& operator<<(ostream& os, const std::pair<T1, T2>& p) {
        return os << "(" << p.first << ", " << p.second << ")";
    }
    
}

