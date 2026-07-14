// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <type_traits>

namespace db0 

{

    template<typename T, typename TrueType, typename FalseType>
    struct if_else_
    {};

    template<typename TrueType, typename FalseType>
    struct if_else_<std::true_type, TrueType, FalseType>{
        typedef typename TrueType::type type;
    };

    template<typename TrueType, typename FalseType>
    struct if_else_<std::false_type, TrueType, FalseType>{
        typedef typename FalseType::type type;
    };

    template<typename... TN>
    struct and_
    {};

    template<typename T1, typename... TN>
    struct and_<T1, TN...>{
        typedef typename if_else_<typename T1::type, and_<TN...>, std::false_type>::type type;
        static constexpr bool value = type::value;
    };

    template<>
    struct and_<>
        : public std::true_type
    {};

    template<typename... TN>
    struct or_
    {};

    template<typename T1, typename... TN>
    struct or_<T1, TN...>{
        typedef typename if_else_<typename T1::type, std::true_type, or_<TN...> >::type type;
        static constexpr bool value = type::value;
    };

    template<>
    struct or_<>
        : public std::false_type
    {};

    template<typename T>
    struct not_
    {};

    template<>
    struct not_<std::true_type>
        : public std::false_type
    {};

    template<>
    struct not_<std::false_type>
        : public std::true_type
    {};

} 
