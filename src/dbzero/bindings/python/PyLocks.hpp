// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#define PY_API_FUNC auto __api_lock = db0::python::PyToolkit::lockPyApi();

namespace db0::python

{

    struct GIL_Lock
    {
        PyGILState_STATE m_state;
        GIL_Lock();
        ~GIL_Lock();
    };

    struct WithGIL_Unlocked
    {
        PyThreadState *__thread_state;
        WithGIL_Unlocked();
        ~WithGIL_Unlocked();
    };
    
} 
