// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <Python.h>
#include "PyWrapper.hpp"

namespace db0::object_model

{
    
    class Object;
    class ObjectImmutableImpl;
    class ObjectAnyImpl;
    
}

namespace db0::python

{

#if PY_VERSION_HEX < 0x030B0000  // Python < 3.11
    // NOTE: since managed dicts were introduced in Python 3.11, we need to use PyWrapperWithDict
    using MemoObject = PyWrapperWithDict<db0::object_model::Object>;
    using MemoImmutableObject = PyWrapperWithDict<db0::object_model::ObjectImmutableImpl>;
    using MemoAnyObject = PyWrapperWithDict<db0::object_model::ObjectAnyImpl>;
#else
    using MemoObject = PyWrapper<db0::object_model::Object>;
    using MemoImmutableObject = PyWrapper<db0::object_model::ObjectImmutableImpl>;
    using MemoAnyObject = PyWrapper<db0::object_model::ObjectAnyImpl>;
#endif

}
