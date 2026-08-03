// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <Python.h>
#include <mutex>

#define PY_API_FUNC auto __api_lock = db0::python::PyToolkit::lockPyApi();
#define PY_MUTATING_API_FUNC(error_result) \
    db0::python::AtomicMutationApiScope __atomic_mutation_api_scope; \
    if (!__atomic_mutation_api_scope.ok()) { \
        return error_result; \
    } \
    auto __api_lock = db0::python::PyToolkit::lockPyApi();
#define PY_MUTATING_API_LOCK_FUNC(error_result) \
    db0::python::AtomicMutationApiScope __atomic_mutation_api_scope; \
    if (!__atomic_mutation_api_scope.ok()) { \
        return error_result; \
    } \
    auto __api_lock = db0::python::PyToolkit::lockPyApi();

namespace db0::python

{

    inline bool isPythonFinalizing()
    {
#if PY_VERSION_HEX >= 0x030D0000
        return Py_IsFinalizing();
#else
        return _Py_IsFinalizing();
#endif
    }

// Avoid running dbzero/Python cleanup from tp_dealloc while the interpreter is
// finalizing. This surfaced as a shutdown SIGSEGV after an unhandled Python
// exception left nested durable objects alive; deallocators tried to enter the
// dbzero API lock / Python C API after finalization had started.
#define PY_DEALLOC_GUARD() \
    do { \
        if (!Py_IsInitialized() || db0::python::isPythonFinalizing()) { \
            return; \
        } \
    } while (false)

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

    struct AtomicMutationApiScope
    {
        bool m_ok = true;
        std::unique_lock<std::recursive_mutex> m_atomic_lock;
        bool m_atomic_owner = false;

        explicit AtomicMutationApiScope(bool register_atomic_owner = true);
        ~AtomicMutationApiScope();

        bool ok() const {
            return m_ok;
        }
    };
    
} 
