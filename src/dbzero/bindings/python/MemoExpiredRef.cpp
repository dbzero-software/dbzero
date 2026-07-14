// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MemoExpiredRef.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{
    
    void MemoExpiredRef::init(std::uint64_t fixture_uuid, UniqueAddress address)
    {
        m_fixture_uuid = fixture_uuid;
        m_address = address;
    }

    UniqueAddress MemoExpiredRef::getUniqueAddress() const {
        return m_address;
    }
    
    Address MemoExpiredRef::getAddress() const {
        return m_address.getAddress();
    }

    const std::uint64_t MemoExpiredRef::getFixtureUUID() const {
        return m_fixture_uuid;
    }    
    
    PyObject *PyAPI_MemoExpiredRef_getattro(MemoExpiredRef *, PyObject *)
    {
        // just report the ReferenceError
        PyErr_SetString(PyToolkit::getTypeManager().getReferenceError(), "Memo instance expired");
        return nullptr;
    }
    
    PyObject *PyAPI_MemoEpxiredRef_setattro(MemoExpiredRef *, PyObject *, PyObject *)
    {
        // just report the ReferenceError
        PyErr_SetString(PyToolkit::getTypeManager().getReferenceError(), "Memo instance expired");
        return nullptr;
    }
    
    PyTypeObject MemoExpiredRefType = 
    {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "MemoExpiredRef",
        .tp_basicsize = sizeof(MemoExpiredRef),
        .tp_itemsize = 0,    
        .tp_getattro = reinterpret_cast<getattrofunc>(PyAPI_MemoExpiredRef_getattro),
        .tp_setattro = reinterpret_cast<setattrofunc>(PyAPI_MemoEpxiredRef_setattro),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_new = PyType_GenericNew,
    };

    bool MemoExpiredRef_Check(PyObject *obj) {
        return PyObject_TypeCheck(obj, &MemoExpiredRefType);
    }
    
    shared_py_object<PyObject*> MemoExpiredRef_new(std::uint64_t fixture_uuid, UniqueAddress address)
    {
        auto py_expired_ref = PyObject_New(MemoExpiredRef, &MemoExpiredRefType);
        if (!py_expired_ref) {
            return nullptr;
        }
        
        py_expired_ref->init(fixture_uuid, address);        
        return reinterpret_cast<PyObject*>(py_expired_ref);
    }
    
}
