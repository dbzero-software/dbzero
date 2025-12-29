// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyLocks.hpp"

namespace db0::python

{

    GIL_Lock::GIL_Lock()
        : m_state(PyGILState_Ensure())
    {
    }

    GIL_Lock::~GIL_Lock() {
        PyGILState_Release(m_state);
    }
    
    WithGIL_Unlocked::WithGIL_Unlocked()
        : __thread_state(PyEval_SaveThread())
    {
    }

    WithGIL_Unlocked::~WithGIL_Unlocked() {
        PyEval_RestoreThread(__thread_state);
    }

} 