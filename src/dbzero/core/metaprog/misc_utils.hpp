#pragma once

#include <type_traits>

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
