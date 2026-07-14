// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include <dbzero/object_model/dict/o_dict.hpp>

struct _object;
using PyObject = _object;

namespace db0::object_model
{
    struct EmbeddedObjectOffsetCollector;

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_py_dict: public o_dict
    {
    public:
        explicit o_py_dict(PyObject *dict);
        o_py_dict(PyObject *dict, EmbeddedObjectOffsetCollector &offsetCollector);

        static std::size_t measure(PyObject *dict);
        static Element elementFromPythonObject(PyObject *object);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            return o_dict::safeSizeOf(buf);
        }

        static o_py_dict &__ref(void *buf);
        static const o_py_dict &__const_ref(const void *buf);

        template<typename... Args> static o_py_dict &__new(void *buf, Args&& ...args)
        {
            return *(new(buf) o_py_dict(std::forward<Args>(args)...));
        }

        static db0::Foundation::Type<o_py_dict> type();

    private:
        static Element elementFromPythonObject(
            PyObject *object, EmbeddedObjectOffsetCollector *offsetCollector
        );
        static Element valueFromPythonDict(PyObject *dict, PyObject *key);
        static Element valueFromPythonDict(
            PyObject *dict, PyObject *key, EmbeddedObjectOffsetCollector *offsetCollector
        );
        static std::uint32_t dictSize(PyObject *dict);
        static std::size_t measurePairs(PyObject *dict);
        static std::size_t measureCollisionBuckets(PyObject *dict, std::size_t capacity);
    };
DB0_PACKED_END

}
