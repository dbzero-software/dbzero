// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include "PyWrapper.hpp"
#include <dbzero/object_model/CommonBase.hpp>

namespace db0::python 

{
   
    // common type for Python counterparts of dbzero object model's objects
    using PyCommonBase = PyWrapper<db0::object_model::CommonBase>;
    
}