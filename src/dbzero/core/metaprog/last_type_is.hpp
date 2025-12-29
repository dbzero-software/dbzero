// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <type_traits>
#include <dbzero/core/metaprog/misc_utils.hpp>
#include <dbzero/core/metaprog/bool_meta_op.hpp>

namespace db0

{

    template <typename... TN>
    struct last_type{
        typedef void type;
    };

    template <typename T>
    struct last_type<T>{
        typedef T type;
    };

    template <typename T1, typename T2, typename... TN>
    struct last_type<T1, T2, TN...>{
        typedef typename last_type<T2, TN...>::type type;
    };

    template<typename TC, typename... TN>
    using last_type_is_not_t = typename std::enable_if<
        or_<
            std::integral_constant<bool, sizeof_<TN...>::value==std::size_t(0)>,
            not_<
                typename std::is_same<
                    typename std::decay<
                        typename last_type<TN...>::type
                    >::type,
                    TC
                >::type
            >
        >::value
    >::type;

    template<typename TC, typename... TN>
    using last_type_is_t = typename std::enable_if<
        and_<
            std::integral_constant<bool, sizeof_<TN...>::value!=std::size_t(0)>,
            typename std::is_same<
                typename std::decay<
                    typename last_type<TN...>::type
                >::type,
                TC
            >::type
        >::value
    >::type;

    template <typename... TN>
    struct first_type{
        typedef void type;
    };

    template <typename T1, typename... TN>
    struct first_type<T1, TN...>{
        typedef T1 type;
    };

    template<typename TC, typename... TN>
    using first_type_is_not_t = typename std::enable_if<
        or_<
            std::integral_constant<bool, sizeof_<TN...>::value==std::size_t(0)>,
            not_<
                typename std::is_same<
                    typename std::decay<
                        typename first_type<TN...>::type
                    >::type,
                    TC
                >::type
            >
        >::value
    >::type;

    template<typename TC, typename... TN>
    using first_type_is_t = typename std::enable_if<
        and_<
            std::integral_constant<bool, sizeof_<TN...>::value!=std::size_t(0)>,
            typename std::is_same<
                typename std::decay<
                    typename first_type<TN...>::type
                >::type,
                TC
            >::type
        >::value
    >::type;
    
}