#include "MemoExpiredRef.hpp"
#include <dbzero/bindings/python/PyToolkit.hpp>

namespace db0::python

{
    
    void MemoExpiredRef::init(std::uint64_t fixture_uuid, std::uint64_t address)
    {
        m_fixture_uuid = fixture_uuid;
        m_address = address;
    }

    const std::uint64_t MemoExpiredRef::getAddress() const {
        return m_address;
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
        PyVarObject_HEAD_INIT(NULL, 0)
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
    
    shared_py_object<PyObject*> MemoExpiredRef_new(std::uint64_t fixture_uuid, std::uint64_t address)
    {
        auto py_expired_ref = PyObject_New(MemoExpiredRef, &MemoExpiredRefType);
        if (!py_expired_ref) {
            return nullptr;
        }
        
        py_expired_ref->init(fixture_uuid, address);        
        return reinterpret_cast<PyObject*>(py_expired_ref);
    }
    
}
