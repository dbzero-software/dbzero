// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <Python.h>
#include <stdexcept>

namespace db0::python
{
    class MigrateException: public std::runtime_error
    {
    public:
        using std::runtime_error::runtime_error;
    };

    PyObject *getMigrateError();
}
