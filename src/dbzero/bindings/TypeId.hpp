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
        DATE = 8,
        TUPLE = 9,
        OBJECT_ITERATOR = 10,
        TYPED_OBJECT_ITERATOR = 11,
        BYTES = 12,
        // DBZero wrappers of common language types
        MEMO_OBJECT = 100,
        DB0_LIST = 101,
        DB0_DICT = 102,
        DB0_TUPLE = 103,
        DB0_SET = 104,
        DB0_BLOCK = 105,
        DB0_PANDAS_DATAFRAME = 106,
        DB0_TAG_SET = 107,
        DB0_INDEX = 108,
        DB0_DATETIME = 109,
        // COUNT determines size of the type operator arrays
        COUNT = 110,
        // unrecognized type
        UNKNOWN = std::numeric_limits<std::uint16_t>::max()
    };
    
}
