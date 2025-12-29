// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

namespace db0 { namespace metaprog

{

    template <int... Is>
    struct seq { };

    template <int N, int... Is>
    struct gen_seq : gen_seq<N - 1, N - 1, Is...> { };

    template <int... Is>
    struct gen_seq<0, Is...> : seq<Is...> { };

    template <typename T, typename F, int... Is>
    void for_each(T&& t, F &f, seq<Is...>)
    {
        auto l = { (f(std::get<Is>(t)), 0)... };
    }

    template <typename T, typename R, int... Is>
    void set_each(T&& t, R &&r, seq<Is...>)
    {
        auto l = { ( std::get<Is>(r) = (*std::get<Is>(t)), 0)... };
    }

    template <typename... Ts, typename F>
    void for_each_in_tuple(std::tuple<Ts...> &t, F &f)
    {
        for_each(t, f, gen_seq<sizeof...(Ts)>());
    }
    
} }
