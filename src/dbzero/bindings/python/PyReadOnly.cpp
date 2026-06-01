// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyReadOnly.hpp"
#include "PyInternalAPI.hpp"

#include <cstdint>

namespace db0::python

{

    namespace
    {
        PyObject *s_read_only_depth_var = nullptr;
        thread_local std::uint64_t s_read_only_generation = 0;

        struct ReadOnlyDepthCache
        {
            std::uint64_t thread_state_id = 0;
            std::uint64_t context_version = 0;
            std::uint64_t generation = 0;
            unsigned int depth = 0;
            bool valid = false;
        };

        thread_local ReadOnlyDepthCache s_depth_cache;

        unsigned int readOnlyDepthFromPythonContext()
        {
            if (!s_read_only_depth_var) {
                return 0;
            }

            auto *thread_state = PyThreadState_Get();
            if (s_depth_cache.valid
                    && s_depth_cache.thread_state_id == thread_state->id
                    && s_depth_cache.context_version == thread_state->context_ver
                    && s_depth_cache.generation == s_read_only_generation) {
                return s_depth_cache.depth;
            }

            PyObject *py_depth = nullptr;
            if (PyContextVar_Get(s_read_only_depth_var, NULL, &py_depth) < 0) {
                PyErr_Clear();
                return 0;
            }

            unsigned int depth = 0;
            if (py_depth) {
                auto long_depth = PyLong_AsUnsignedLong(py_depth);
                Py_DECREF(py_depth);
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    long_depth = 0;
                }
                depth = static_cast<unsigned int>(long_depth);
            }

            s_depth_cache = {
                .thread_state_id = thread_state->id,
                .context_version = thread_state->context_ver,
                .generation = s_read_only_generation,
                .depth = depth,
                .valid = true,
            };
            return depth;
        }

        void invalidateReadOnlyDepthCache()
        {
            ++s_read_only_generation;
            s_depth_cache.valid = false;
        }

        PyObject *makeDepthObject(unsigned int depth)
        {
            return PyLong_FromUnsignedLong(depth);
        }
    }

    PyReadOnlyContext::PyReadOnlyContext()
    {
        if (!s_read_only_depth_var) {
            THROWF(db0::InternalException) << "read_only context support is not initialized";
        }

        auto current_depth = readOnlyDepthFromPythonContext();
        auto next_depth = Py_OWN(makeDepthObject(current_depth + 1));
        if (!next_depth) {
            THROWF(db0::InputException) << "unable to enter read_only context";
        }

        m_token = PyContextVar_Set(s_read_only_depth_var, next_depth.get());
        if (!m_token) {
            THROWF(db0::InputException) << "unable to enter read_only context";
        }
        db0::ReadOnlyContext::enterExternal();
        invalidateReadOnlyDepthCache();
    }

    PyReadOnlyContext::~PyReadOnlyContext()
    {
        try {
            close();
        } catch (...) {
            PyErr_Clear();
        }
    }

    void PyReadOnlyContext::close()
    {
        if (!m_active) {
            return;
        }

        if (m_token) {
            auto result = PyContextVar_Reset(s_read_only_depth_var, m_token);
            if (result < 0) {
                THROWF(db0::InputException) << "unable to close read_only context";
            }
            Py_DECREF(m_token);
            m_token = nullptr;
            db0::ReadOnlyContext::exitExternal();
            invalidateReadOnlyDepthCache();
        }

        m_active = false;
    }

    static PyMethodDef PyReadOnly_methods[] =
    {
        {"close", (PyCFunction)&PyAPI_PyReadOnly_close, METH_NOARGS, "Close/exit the read-only context"},
        {NULL}
    };

    PyReadOnly *PyReadOnly_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyReadOnly*>(type->tp_alloc(type, 0));
    }

    PyReadOnly *PyReadOnlyDefault_new() {
        return PyReadOnly_new(&PyReadOnlyType, NULL, NULL);
    }

    void PyAPI_PyReadOnly_del(PyReadOnly* self)
    {
        PY_API_FUNC
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    PyTypeObject PyReadOnlyType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.ReadOnlyContext",
        .tp_basicsize = PyReadOnly::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_PyReadOnly_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero read-only context",
        .tp_methods = PyReadOnly_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyReadOnly_new,
        .tp_free = PyObject_Free,
    };

    PyReadOnly *PyAPI_tryBeginReadOnly(PyObject *)
    {
        PY_API_FUNC
        auto py_object = Py_OWN(PyReadOnly_new(&PyReadOnlyType, NULL, NULL));
        py_object->makeNew();
        return py_object.steal();
    }

    PyObject *PyAPI_beginReadOnly(PyObject *self, PyObject *const *, Py_ssize_t nargs)
    {
        if (nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "begin_read_only requires no arguments");
            return NULL;
        }
        return runSafe(PyAPI_tryBeginReadOnly, self);
    }

    PyObject *PyAPI_inReadOnly(PyObject *, PyObject *const *, Py_ssize_t nargs)
    {
        if (nargs != 0) {
            PyErr_SetString(PyExc_TypeError, "_in_read_only requires no arguments");
            return NULL;
        }
        if (db0::ReadOnlyContext::isActive()) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_FALSE;
    }

    bool PyReadOnly_Check(PyObject *object) {
        return Py_TYPE(object) == &PyReadOnlyType;
    }

    PyObject *tryPyReadOnly_close(PyReadOnly *self)
    {
        self->modifyExt().close();
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_PyReadOnly_close(PyObject *self, PyObject *)
    {
        PY_API_FUNC
        return runSafe(tryPyReadOnly_close, reinterpret_cast<PyReadOnly*>(self));
    }

    int initReadOnlyContextSupport()
    {
        if (!s_read_only_depth_var) {
            auto default_depth = Py_OWN(makeDepthObject(0));
            if (!default_depth) {
                return -1;
            }
            s_read_only_depth_var = PyContextVar_New("dbzero_read_only_depth", default_depth.get());
            if (!s_read_only_depth_var) {
                return -1;
            }
            db0::ReadOnlyContext::setDepthProvider(readOnlyDepthFromPythonContext);
        }
        return 0;
    }

}
