#pragma once

#include <Python.h>
#include <cstdint>

namespace db0::python 

{

    /**
     * Adds a mixed-in (but dynamically initialized)
     * member of type T into the PyObject struct.
     **/
    template <typename T> struct PyWrapper: public PyObject
    {
        inline T &ext() {
            // calculate instance offset
            return *reinterpret_cast<T*>((char*)this + Py_TYPE(this)->tp_basicsize - sizeof(T));
        }

        const T &ext() const {
            return *reinterpret_cast<const T*>((char*)this + Py_TYPE(this)->tp_basicsize - sizeof(T));
        }

        static constexpr std::size_t sizeOf() {
            return sizeof(PyObject) + sizeof(T);
        }
    };

}