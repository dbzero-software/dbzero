// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include <dbzero/object_model/set/o_set.hpp>

struct _object;
using PyObject = _object;

namespace db0::object_model
{
    struct EmbeddedObjectOffsetCollector;

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_py_set: public o_set
    {
    public:
        explicit o_py_set(PyObject *iterable);
        o_py_set(PyObject *iterable, EmbeddedObjectOffsetCollector &offsetCollector);

        static std::size_t measure(PyObject *iterable);
        static Element elementFromPythonObject(PyObject *object);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            return o_set::safeSizeOf(buf);
        }

        static o_py_set &__ref(void *buf);
        static const o_py_set &__const_ref(const void *buf);

        template<typename... Args> static o_py_set &__new(void *buf, Args&& ...args)
        {
            return *(new(buf) o_py_set(std::forward<Args>(args)...));
        }

        static db0::Foundation::Type<o_py_set> type();

    private:
        static Element elementFromPythonObject(
            PyObject *object, EmbeddedObjectOffsetCollector *offsetCollector
        );
        static std::uint32_t setSize(PyObject *set);
        static std::size_t measureElements(PyObject *set);
        static std::size_t measureCollisionBuckets(PyObject *set, std::size_t capacity);
    };
DB0_PACKED_END

}
