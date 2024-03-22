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
        // DBZero wrappers of common language types
        MEMO_OBJECT = 10000,
        DB0_LIST = 10001,
        DB0_DICT = 10002,
        DB0_TUPLE = 10003,
        DB0_SET = 10004,
        DB0_BLOCK = 10005,
        DB0_PANDAS_DATAFRAME = 10006,
        DB0_TAG_SET = 10007,
        DB0_INDEX = 10008,
        // unrecognized type
        UNKNOWN = std::numeric_limits<std::uint16_t>::max()
    };
    
}

namespace std

{
    
    ostream &operator<<(ostream &, db0::bindings::TypeId);

}