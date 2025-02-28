#include "InterProcessLock.hpp"
#include <iostream>
#include <filesystem>
#include <dbzero/core/exception/Exceptions.hpp>

PyObject *getKwargs(db0::LockFlags lock_flags)
{
    PyObject *keywords = PyDict_New();
    PyDict_SetItemString(keywords, "blocking", PyBool_FromLong(lock_flags.m_blocking));
    PyDict_SetItemString(keywords, "timeout", PyLong_FromLong(lock_flags.m_timeout));
    return keywords;
}

namespace db0

{

    InterProcessLock::InterProcessLock(const std::string & lockPath, LockFlags lock_flags)
        : m_lock_flags(lock_flags)
        , m_lockPath(lockPath)
    {
        PyObject *pName = PyUnicode_DecodeFSDefault("fasteners");
        PyObject *pModule = PyImport_Import(pName);
        Py_DECREF(pName);
        if (pModule == NULL) {    
            THROWF(db0::InternalException) << "Failed to load fasteners module";        
        }

        PyObject *pInterLock = PyObject_GetAttrString(pModule, "InterProcessLock");
        if (pInterLock == NULL) {
            THROWF(db0::InternalException) << "Failed to load InterProcessLock class";        
        }

        if (m_lock_flags.m_force_unlock) {
            // remove lock file
            std::remove(m_lockPath.c_str());
        }
        PyObject * str = PyUnicode_FromString(m_lockPath.c_str());
        PyObject *args = PyTuple_Pack(1, str);
        m_lock = PyObject_CallObject(pInterLock, args);
        Py_DECREF(args);
        if (m_lock == NULL) {
            THROWF(db0::InternalException) << "Failed to create InterProcessLock object";        
        }
        assureLocked();
        Py_DECREF(pInterLock);
        Py_DECREF(pModule);
        Py_DECREF(str);
    }

    InterProcessLock::~InterProcessLock()
    {
        PyObject_CallMethod(m_lock, "release", NULL);
        if (m_lock != NULL) {
            Py_DECREF(m_lock);
        }
    }

    bool InterProcessLock::is_locked() const
    {
        PyObject * result = PyObject_CallMethod(m_lock, "exists", NULL);
        if (result == NULL) {
            THROWF(db0::InternalException) << "Failed to check if lock exists";
        }
        return PyLong_AsLong(result) != 0;
    }
    
    void InterProcessLock::assureLocked()
    {
        PyObject *keywords = getKwargs(m_lock_flags);
        PyObject* aquire = PyObject_GetAttrString(m_lock, "acquire");
        if (aquire == NULL) {
            THROWF(db0::InternalException) << "Failed to get acquire method";
        }

        PyObject *args = Py_BuildValue("()");
        PyObject *result = PyObject_Call(aquire, args, keywords);
        auto result_str = PyLong_AsLong(result);
        if (result_str == 0) {
            // FIXME: log
            assert(false);
            THROWF(db0::InternalException) << "Failed to aquire lock of: " << m_lockPath;
        }
        
        Py_DECREF(keywords);
        Py_DECREF(args);
        Py_DECREF(result);
        Py_DECREF(aquire);
    }

}
