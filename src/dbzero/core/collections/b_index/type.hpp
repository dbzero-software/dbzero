// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

namespace db0::bindex

{

    // 4-bit index type
    enum class type: std::uint8_t
    {
        empty  = 0,
        itty = 1 ,
        array_2 = 2 ,
        array_3 = 3 ,
        array_4 = 4 ,
        sorted_vector = 5,
        bindex = 6 ,
        memory = 7,
        unknown = 8
    };
    
}