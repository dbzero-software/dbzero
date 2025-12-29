// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <type_traits>
#include <list>
#include <vector>
#include <set>
#include <map>
#include <unordered_map>
#include <unordered_set>

namespace db0

{

    template <typename... T> struct is_sequence : public std::false_type {};
    template <typename... T> struct is_sequence<std::vector<T...> > : public std::true_type {};
    template <typename... T> struct is_sequence<std::list<T...> > : public std::true_type {};
    template <typename... T> struct is_sequence<std::set<T...> > : public std::true_type {};
    template <typename... T> struct is_sequence<std::map<T...> > : public std::true_type {};
    template <typename... T> struct is_sequence<std::unordered_map<T...> > : public std::true_type {};
    template <typename... T> struct is_sequence<std::unordered_set<T...> > : public std::true_type {};

    // iterator / const pointer syntax compatible type
    template<typename T, typename = void>
    struct is_iterator
    {
        static constexpr bool value = false;
    };

    // const char * should not be treated as iterator
    template<> struct is_iterator<const char*>
    {
        static constexpr bool value = false;
    };

    template<typename T>
    struct is_iterator<T, typename std::enable_if<!std::is_same<typename std::iterator_traits<T>::value_type, void>::value>::type>
    {
        static constexpr bool value = true;
    };
    
}

