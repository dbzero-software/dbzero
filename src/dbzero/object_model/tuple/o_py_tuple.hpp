// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <utility>

#include <dbzero/object_model/tuple/o_tuple.hpp>

struct _object;
using PyObject = _object;

namespace db0::object_model
{
    struct EmbeddedObjectOffsetCollector;

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_py_tuple: public o_tuple<>
    {
    public:
        explicit o_py_tuple(PyObject *sequence);
        o_py_tuple(PyObject *sequence, EmbeddedObjectOffsetCollector &offsetCollector);

        static std::size_t measure(PyObject *sequence);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            return o_tuple<>::safeSizeOf(buf);
        }

        static o_py_tuple &__ref(void *buf);
        static const o_py_tuple &__const_ref(const void *buf);

        template<typename... Args> static o_py_tuple &__new(void *buf, Args&& ...args)
        {
            return *(new(buf) o_py_tuple(std::forward<Args>(args)...));
        }

        static db0::Foundation::Type<o_py_tuple> type();

    private:
        static Element elementFromPythonObject(PyObject *object);
        static Element elementFromPythonObject(
            PyObject *object, EmbeddedObjectOffsetCollector *offsetCollector
        );
        static std::size_t sequenceSize(PyObject *sequence);
        static PyObject *sequenceItem(PyObject *sequence, std::size_t index);
        static std::size_t measureElements(PyObject *sequence);
    };
DB0_PACKED_END

}
