// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/tags/CompositeTagDef.hpp>

namespace db0::python

{

    using CompositeTagDef = db0::object_model::CompositeTagDef;
    using PyCompositeTag = PyWrapper<CompositeTagDef, false>;

    PyCompositeTag *PyCompositeTag_new(PyTypeObject *type, PyObject *, PyObject *);
    PyCompositeTag *PyCompositeTagDefault_new();

    void PyCompositeTag_del(PyCompositeTag *);
    extern PyTypeObject PyCompositeTagType;

    bool PyCompositeTag_Check(PyObject *);

}
