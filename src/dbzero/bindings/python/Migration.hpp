// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <Python.h>
#include "PyWrapper.hpp"
#include "shared_py_object.hpp"
#include <vector>

namespace db0::python

{

    struct Migration
    {
        // migration method
        shared_py_object<PyObject*> m_migrate;        
        // migration affected member variables
        std::vector<std::string> m_vars;

        Migration(PyObject *, std::vector<std::string> &&);

        // try executing the migrate function with no arguments
        PyObject *exec(PyObject *self) const;
    };
    
    std::vector<Migration> extractMigrations(PyObject *py_migrations);

}
