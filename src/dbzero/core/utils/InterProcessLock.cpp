// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "InterProcessLock.hpp"
#include <iostream>
#include <filesystem>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0

{

    PyObject *getKwargs(db0::LockFlags lock_flags)
    {
        auto keywords = Py_OWN(PyDict_New());
        PySafeDict_SetItemString(*keywords, "blocking", Py_OWN(PyBool_FromLong(lock_flags.m_blocking)));
        PySafeDict_SetItemString(*keywords, "timeout", Py_OWN(PyLong_FromLong(lock_flags.m_timeout)));
        return keywords.steal();
    }
    
    InterProcessLock::InterProcessLock(const std::string & lockPath, LockFlags lock_flags)
        : m_lock_flags(lock_flags)
        , m_lockPath(lockPath)
    {
        // assure GIL since the API may be called from native code
        auto __gil = db0::python::PyToolkit::ensureLocked();
        auto pName = Py_OWN(PyUnicode_DecodeFSDefault("fasteners"));
        auto pModule = Py_OWN(PyImport_Import(*pName));
        if (!pModule) {
            THROWF(db0::InternalException) << "Failed to load fasteners module";        
        }

        auto pInterLock = Py_OWN(PyObject_GetAttrString(*pModule, "InterProcessLock"));
        if (!pInterLock) {
            THROWF(db0::InternalException) << "Failed to load InterProcessLock class";        
        }
        
        if (m_lock_flags.m_force_unlock) {
            // remove lock file
            std::remove(m_lockPath.c_str());
        }
        auto args = Py_OWN(PySafeTuple_Pack(Py_OWN(PyUnicode_FromString(m_lockPath.c_str()))));
        m_lock = Py_OWN(PyObject_CallObject(*pInterLock, *args));
        if (!m_lock) {
            THROWF(db0::InternalException) << "Failed to create InterProcessLock object";        
        }

        assureLocked();
    }
    
    InterProcessLock::~InterProcessLock()
    {
        // NOTE: python interpreter may be destroyed before this destructor is called
        if (db0::python::PyToolkit::isValid()) {
            auto __gil = db0::python::PyToolkit::ensureLocked();
            auto res = Py_OWN(PyObject_CallMethod(*m_lock, "release", NULL));
            m_lock = nullptr;
        } else {
            // just drop the reference since python interpreter is already destroyed
            m_lock.steal();
        }
    }

    bool InterProcessLock::isLocked() const
    {
        // assure GIL since the API may be called from native code
        auto __gil = db0::python::PyToolkit::ensureLocked();
        auto result = Py_OWN(PyObject_CallMethod(*m_lock, "exists", NULL));
        if (!result) {
            THROWF(db0::InternalException) << "Failed to check if lock exists";
        }
        return PyLong_AsLong(*result) != 0;
    }
    
    void InterProcessLock::assureLocked()
    {
        // assure GIL since the API may be called from native code
        auto __gil = db0::python::PyToolkit::ensureLocked();
        auto keywords = Py_OWN(getKwargs(m_lock_flags));
        auto aquire = Py_OWN(PyObject_GetAttrString(*m_lock, "acquire"));
        if (!aquire) {
            THROWF(db0::InternalException) << "Failed to get acquire method";
        }

        auto args = Py_OWN(Py_BuildValue("()"));
        auto result = Py_OWN(PyObject_Call(*aquire, *args, *keywords));        
        if (!PyLong_AsLong(*result)) {
            THROWF(db0::InternalException) << "Failed to aquire lock of: " << m_lockPath;
        }
    }

}
