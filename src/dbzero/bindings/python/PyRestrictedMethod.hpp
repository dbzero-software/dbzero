// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include "PyTypes.hpp"

namespace db0::python

{

    extern PyTypeObject PyRestrictedMethodType;

    bool isRestrictedName(const char *attr_name);
    PyObject *tryRestrictedMemoGetattro(
        PyObject *memo_obj,
        PyObject *attr,
        const char *attr_name,
        PyTypes::ObjectSharedPtr &member
    );

}
