// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <dbzero/core/metaprog/bool_meta_op.hpp>
#include <dbzero/core/metaprog/misc_utils.hpp>

namespace db0 

{

    template<typename T, T... I>
    struct int_seq{
        static constexpr std::size_t size = sizeof...(I);
    };

    template <typename T, T N, T... I>
    struct int_seq_helper{
        typedef typename if_else_<
            std::integral_constant<bool, N<=T(0)>,
            ident<int_seq<T,I...> >,
            int_seq_helper<T, N-1, N-1, I...>
        >::type type;
    };

    template <typename T, T N>
    using make_int_seq_t = typename int_seq_helper<T, N>::type;
    
}