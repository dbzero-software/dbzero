#pragma once

#include <Python.h>
#include <cstdint>
#include "ObjectId.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>

namespace db0
{
    class WorkspaceView;
}

namespace db0::python

{

    using ObjectId = db0::object_model::ObjectId;

    /**
     * Extarct full object UUID from python args compatible with db0.open()
    */
    ObjectId extractObjectId(PyObject *args);
    
    /**
     * Open DBZero object by UUID
     * @param py_expected_type - expected Python type of the object
    */    
    PyObject *fetchObject(ObjectId object_id, PyTypeObject *py_expected_type = nullptr);
    
    /**
     * Open DBZero singleton by its corresponding Python type
    */
    PyObject *fetchSingletonObject(PyTypeObject *py_type);
    
    /**
     * Open DBZero singleton from a specific snapshot (workspace view)
    */
    PyObject *fetchSingletonObject(db0::WorkspaceView &, PyTypeObject *py_type);

    PyObject *fetchMemoObject(db0::swine_ptr<Fixture> &, ObjectId);

    PyObject *fetchListObject(db0::swine_ptr<Fixture> &, ObjectId);
    
    /**
     * Open DBZero object from a specific fixture
     * with optional type validation
    */
    PyObject *fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id, PyTypeObject *py_expected_type = nullptr);
    
    void renameField(PyTypeObject *py_type, const char *from_name, const char *to_name);
    
    /**
     * Runs a function, catch exeptions and translate into Python errors
    */
    template <typename... Args, typename T> PyObject *runSafe(T func, Args... args)
    {
        try {
            return func(args...);
        } catch (const db0::AbstractException &e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return NULL;
        } catch (const std::exception &e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return NULL;
        } catch (...) {
            PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
            return NULL;
        }
    }

#ifndef NDEBUG
    /**
     * A test function to make an allocation and write random bytes into the current prefix
    */
    PyObject *writeBytes(PyObject *self, PyObject *args);
#endif
    
    bool isBase(PyTypeObject *py_type, PyTypeObject *base_type);

}

