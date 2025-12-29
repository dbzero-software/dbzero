// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "bindex_interface.hpp"

namespace db0::bindex

{

    std::ostream &operator<<(std::ostream &os, db0::bindex::type t)
    {
        switch (t) {
            case bindex::type::empty :
                os << "empty";
                break;
            case bindex::type::itty :
                os << "itty";
                break;
            case bindex::type::array_2 :
                os << "array_2";
                break;
            case bindex::type::array_3 :
                os << "array_3";
                break;
            case bindex::type::array_4 :
                os << "array_4";
                break;
            case bindex::type::sorted_vector :
                os << "sorted_vector";
                break;
            case bindex::type::bindex :
                os << "bindex";
                break;
            case bindex::type::memory:
                os << "memory";
                break;
            case bindex::type::unknown :
                os << "unknown";
                break;            
        }
        return os;
    }

}
