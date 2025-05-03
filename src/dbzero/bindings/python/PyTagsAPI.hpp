#pragma once

#include <Python.h>
#include <cstdint>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include "shared_py_object.hpp"
#include <type_traits>
#include "PyToolkit.hpp"
    
namespace db0

{

    class Snapshot;

}

namespace db0::object_model

{

    class ObjectIterable;
    
}

namespace db0::python

{   
        
    using ObjectId = db0::object_model::ObjectId;
    using ObjectIterable = db0::object_model::ObjectIterable;
    
    /**
     * Universal find implementation (works on Workspace or WorkspaceView)
     * @param context - the optional context / scope to be attached to the result query
     * @return PyObjectIterable
    */
    PyObject *findIn(db0::Snapshot &, PyObject* const *args, Py_ssize_t nargs, PyObject *context = nullptr);

    PyObject *trySplitBy(PyObject *args, PyObject *kwargs);
        
    PyObject *trySelectModCandidates(const ObjectIterable &, StateNumType from_state,
        std::optional<StateNumType> to_state);
    
    PyObject *trySplitBySnapshots(const ObjectIterable &, const std::vector<db0::Snapshot*> &snapshots);
    
}