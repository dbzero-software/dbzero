#pragma once

#include <Python.h>

#define PY_API_FUNC PyThreadState *__save = PyEval_SaveThread(); \
auto __api_lock = db0::python::PyToolkit::lockApi(); \
PyEval_RestoreThread(__save);

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
