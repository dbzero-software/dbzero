// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <Python.h>

namespace db0::python
{

const char* parseStringLikeArgument(PyObject *arg, const char *func_name, const char *arg_name);

}
