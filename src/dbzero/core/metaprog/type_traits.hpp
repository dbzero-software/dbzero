// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <type_traits>

namespace db0

{

    namespace metaprog {

        //---------------------------------------------
        // Traits to be used with overlays
        template <typename...>
        using void_t = void;

        template <typename T, typename = void>
        struct has_fixed_header : public std::false_type {};
        
        template <typename T>
        struct has_fixed_header<T, void_t<typename T::fixed_header_type>> : public std::true_type {};


        template <typename T, typename = void>
        struct has_constant_size : public std::false_type {};
        
        template <typename T>
        struct has_constant_size<T, void_t<typename T::has_constant_size>> 
            : public std::conditional<T::has_constant_size::value, std::true_type, std::false_type>::type {};
            
    }

}
