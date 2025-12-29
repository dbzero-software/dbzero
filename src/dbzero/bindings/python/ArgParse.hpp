// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

namespace db0::python
{

const char* parseStringLikeArgument(PyObject *arg, const char *func_name, const char *arg_name);

}
