// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyReadOnly.hpp"
#include "PyInternalAPI.hpp"

#include <atomic>
#include <cstdint>

namespace db0::python

{

    namespace
    {
        PyObject *s_read_only_depth_var = nullptr;
        std::atomic_uint64_t s_active_read_only_generation = 0;
        thread_local std::uint64_t s_read_only_generation = 0;
        constexpr const char *READ_ONLY_STATE_CAPSULE_NAME = "dbzero.ReadOnlyState";

        struct ReadOnlyState
        {
            std::atomic_bool active = true;
        };

        struct ReadOnlyDepthCache
        {
            std::uint64_t thread_state_id = 0;
            std::uint64_t context_version = 0;
            std::uint64_t generation = 0;
            std::uint64_t active_generation = 0;
            unsigned int depth = 0;
            bool valid = false;
        };

        thread_local ReadOnlyDepthCache s_depth_cache;

        void incrementActiveReadOnlyGeneration()
        {
            s_active_read_only_generation.fetch_add(1, std::memory_order_release);
        }

        void destroyReadOnlyStateCapsule(PyObject *capsule)
        {
            if (!PyCapsule_IsValid(capsule, READ_ONLY_STATE_CAPSULE_NAME)) {
                return;
            }
            auto *state = static_cast<ReadOnlyState*>(
                PyCapsule_GetPointer(capsule, READ_ONLY_STATE_CAPSULE_NAME)
            );
            delete state;
        }

        PyObject *makeReadOnlyStateCapsule()
        {
            auto *state = new ReadOnlyState();
            auto *capsule = PyCapsule_New(
                state,
                READ_ONLY_STATE_CAPSULE_NAME,
                destroyReadOnlyStateCapsule
            );
            if (!capsule) {
                delete state;
                return nullptr;
            }
            return capsule;
        }

        ReadOnlyState *getReadOnlyState(PyObject *object)
        {
            if (!PyCapsule_IsValid(object, READ_ONLY_STATE_CAPSULE_NAME)) {
                return nullptr;
            }
            return static_cast<ReadOnlyState*>(
                PyCapsule_GetPointer(object, READ_ONLY_STATE_CAPSULE_NAME)
            );
        }

        bool readOnlyStateIsActive(PyObject *object)
        {
            auto *state = getReadOnlyState(object);
            return state && state->active.load(std::memory_order_acquire);
        }

        unsigned int activeReadOnlyDepthFromPythonContext()
        {
            if (!s_read_only_depth_var) {
                return 0;
            }

            PyObject *py_states = nullptr;
            if (PyContextVar_Get(s_read_only_depth_var, NULL, &py_states) < 0) {
                PyErr_Clear();
                return 0;
            }

            if (!py_states) {
                return 0;
            }

            unsigned int depth = 0;
            if (PyTuple_Check(py_states)) {
                auto tuple_size = PyTuple_GET_SIZE(py_states);
                for (Py_ssize_t i = 0; i < tuple_size; ++i) {
                    if (readOnlyStateIsActive(PyTuple_GET_ITEM(py_states, i))) {
                        ++depth;
                    }
                }
            }

            Py_DECREF(py_states);
            return depth;
        }

        PyObject *makeContextStatesTuple(PyObject *new_state_capsule)
        {
            PyObject *current_states = nullptr;
            if (PyContextVar_Get(s_read_only_depth_var, NULL, &current_states) < 0) {
                PyErr_Clear();
                return nullptr;
            }

            Py_ssize_t active_count = 0;
            if (current_states && PyTuple_Check(current_states)) {
                auto tuple_size = PyTuple_GET_SIZE(current_states);
                for (Py_ssize_t i = 0; i < tuple_size; ++i) {
                    if (readOnlyStateIsActive(PyTuple_GET_ITEM(current_states, i))) {
                        ++active_count;
                    }
                }
            }

            auto *tuple = PyTuple_New(active_count + 1);
            if (!tuple) {
                Py_XDECREF(current_states);
                return nullptr;
            }

            Py_ssize_t tuple_index = 0;
            if (current_states && PyTuple_Check(current_states)) {
                auto tuple_size = PyTuple_GET_SIZE(current_states);
                for (Py_ssize_t i = 0; i < tuple_size; ++i) {
                    auto *state_capsule = PyTuple_GET_ITEM(current_states, i);
                    if (!readOnlyStateIsActive(state_capsule)) {
                        continue;
                    }
                    Py_INCREF(state_capsule);
                    PyTuple_SET_ITEM(tuple, tuple_index++, state_capsule);
                }
            }

            Py_INCREF(new_state_capsule);
            PyTuple_SET_ITEM(tuple, tuple_index, new_state_capsule);
            Py_XDECREF(current_states);
            return tuple;
        }

        unsigned int readOnlyDepthFromPythonContext()
        {
            if (!s_read_only_depth_var) {
                return 0;
            }

            auto *thread_state = PyThreadState_Get();
            auto active_generation = s_active_read_only_generation.load(std::memory_order_acquire);
            if (s_depth_cache.valid
                    && s_depth_cache.thread_state_id == thread_state->id
                    && s_depth_cache.context_version == thread_state->context_ver
                    && s_depth_cache.generation == s_read_only_generation
                    && s_depth_cache.active_generation == active_generation) {
                return s_depth_cache.depth;
            }

            auto depth = activeReadOnlyDepthFromPythonContext();

            s_depth_cache = {
                .thread_state_id = thread_state->id,
                .context_version = thread_state->context_ver,
                .generation = s_read_only_generation,
                .active_generation = active_generation,
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

        PyObject *makeEmptyContextIdsTuple()
        {
            return PyTuple_New(0);
        }
    }

    PyReadOnlyContext::PyReadOnlyContext()
    {
        if (!s_read_only_depth_var) {
            THROWF(db0::InternalException) << "read_only context support is not initialized";
        }

        m_state_capsule = makeReadOnlyStateCapsule();
        if (!m_state_capsule) {
            THROWF(db0::InputException) << "unable to enter read_only context";
        }

        auto next_states = Py_OWN(makeContextStatesTuple(m_state_capsule));
        if (!next_states) {
            Py_DECREF(m_state_capsule);
            m_state_capsule = nullptr;
            THROWF(db0::InputException) << "unable to enter read_only context";
        }

        m_token = PyContextVar_Set(s_read_only_depth_var, next_states.get());
        if (!m_token) {
            Py_DECREF(m_state_capsule);
            m_state_capsule = nullptr;
            THROWF(db0::InputException) << "unable to enter read_only context";
        }
        db0::ReadOnlyContext::enterExternal();
        incrementActiveReadOnlyGeneration();
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
            auto *state = getReadOnlyState(m_state_capsule);
            if (state) {
                state->active.store(false, std::memory_order_release);
            }
            Py_DECREF(m_state_capsule);
            m_state_capsule = nullptr;
            db0::ReadOnlyContext::exitExternal();
            incrementActiveReadOnlyGeneration();
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
            auto default_ids = Py_OWN(makeEmptyContextIdsTuple());
            if (!default_ids) {
                return -1;
            }
            s_read_only_depth_var = PyContextVar_New("dbzero_read_only_context_ids", default_ids.get());
            if (!s_read_only_depth_var) {
                return -1;
            }
            db0::ReadOnlyContext::setDepthProvider(readOnlyDepthFromPythonContext);
        }
        return 0;
    }

}
