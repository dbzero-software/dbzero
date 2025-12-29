// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

namespace db0

{

    template <typename random_iterator> struct Operators
    {
        inline static int sub(random_iterator end, random_iterator begin) {
            return end - begin;
        }

        inline static random_iterator add(random_iterator it, int value) {
            return it + value;
        }

        inline static void prev(random_iterator &it) {
            --it;
        }

        inline static void next(random_iterator &it) {
            ++it;
        }
    };

    template <typename random_iterator> struct ReverseOperators
    {
        inline static int sub(random_iterator end, random_iterator begin) {
            return begin - end;
        }

        inline static random_iterator add(random_iterator it, int value) {
            return it - value;
        }

        inline static void prev(random_iterator &it) {
            ++it;
        }

        inline static void next(random_iterator &it) {
            --it;
        }
    };
    
}