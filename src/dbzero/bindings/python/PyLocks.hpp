// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
