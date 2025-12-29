// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TypeId.hpp"

namespace db0::bindings

{
    
    TypeId asNative(TypeId type_id)
    {
        switch (type_id) {
            case TypeId::DB0_BYTES_ARRAY:
                return TypeId::BYTES_ARRAY;            
            case TypeId::DB0_LIST:
                return TypeId::LIST;
            case TypeId::DB0_DICT:
                return TypeId::DICT;
            case TypeId::DB0_TUPLE:
                return TypeId::TUPLE;
            case TypeId::DB0_SET:
                return TypeId::SET;

            default:
                return type_id;
                break;
        }
    }
    
} 