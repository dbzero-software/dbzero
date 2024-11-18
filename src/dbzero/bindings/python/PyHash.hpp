#pragma once
#include <Python.h>
#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>

namespace db0::python

{   

    using TypeId = db0::bindings::TypeId;

    PyObject* get_py_hash_as_py_object(PyObject *key);

    int64_t get_py_hash(PyObject *key);

    template <TypeId type_id> int64_t get_py_hash_impl(PyObject *key);


    int64_t get_py_hash_impl_default(PyObject *key);
}