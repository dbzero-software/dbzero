#pragma once

namespace db0::bindex

{

    // 4-bit index type
    enum type {
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