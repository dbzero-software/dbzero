// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <limits>
#include <sstream>

namespace db0::bindings

{
    
    /**
     * Superset of common language types (common to all supported languages)
    */
    enum class TypeId: std::uint16_t
    {
        NONE = 0,
        INTEGER = 1,
        // floating-point
        FLOAT = 2,
        STRING = 3,
        LIST = 4,
        DICT = 5,
        SET = 6,
        DATETIME = 7,
        DATETIME_TZ = 8,
        DATE = 9,
        TIME = 10,
        TIME_TZ = 11,
        TUPLE = 12,
        OBJECT_ITERABLE = 13,
        OBJECT_ITERATOR = 14,
        BYTES = 15,
        BYTES_ARRAY = 16,
        BOOLEAN = 17,
        DECIMAL = 18,
        // dbzero wrappers of common language types
        MEMO_OBJECT = 100,
        DB0_LIST = 101,
        DB0_DICT = 102,
        DB0_TUPLE = 103,
        DB0_SET = 104,
        DB0_TAG_SET = 107,
        DB0_INDEX = 108,
        DB0_BYTES_ARRAY = 109,
        DB0_ENUM = 110,
        DB0_ENUM_VALUE = 111,
        DB0_FIELD_DEF = 112,
        // dbzero class object (e.g. PyClass)
        DB0_CLASS = 113,
        DB0_ENUM_VALUE_REPR = 114,
        DB0_TAG = 115,
        DB0_WEAK_PROXY = 116,
        MEMO_EXPIRED_REF = 117,
        // Python type decorated as memo
        MEMO_TYPE = 118,
        MEMO_IMMUTABLE_OBJECT = 119,
        // COUNT determines size of the type operator arrays
        COUNT = 120,
        // unrecognized type
        UNKNOWN = std::numeric_limits<std::uint16_t>::max()
    };
    
    // Converts to a native Python type ID
    TypeId asNative(TypeId);
    
}
