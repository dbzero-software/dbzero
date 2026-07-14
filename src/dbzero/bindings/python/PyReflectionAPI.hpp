// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <cstdint>
#include <dbzero/object_model/class/Class.hpp>

namespace db0::python

{   

    PyObject *tryGetPrefixes();    
    PyObject *tryGetMemoClasses(const char *prefix_name, std::uint64_t prefix_uuid);
    PyObject *getSingletonUUID(const db0::object_model::Class &);
    
}
