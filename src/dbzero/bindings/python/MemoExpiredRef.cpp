#include "MemoExpiredRef.hpp"

namespace db0::python

{

    PyTypeObject MemoExpiredRefType = 
    {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "MemoExpiredRef",
        .tp_basicsize = sizeof(MemoExpiredRef),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)MemoExpiredRef_del,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_new = PyType_GenericNew,
    };

    bool MemoExpiredRef_Check(PyObject *obj) {
        return PyObject_TypeCheck(obj, &MemoExpiredRefType);
    }

    void MemoExpiredRef_del(MemoExpiredRef *py_expired_ref)
    {
        py_expired_ref->~MemoExpiredRef();
        Py_TYPE(py_expired_ref)->tp_free((PyObject*)py_expired_ref);
    }
    
    shared_py_object<PyObject*> MemoExpiredRef_new(std::uint64_t fixture_uuid, std::uint64_t address)
    {
        auto py_expired_ref = PyObject_New(MemoExpiredRef, &MemoExpiredRefType);
        if (!py_expired_ref) {
            return nullptr;
        }

        py_expired_ref->m_fixture_uuid = fixture_uuid;
        py_expired_ref->m_address = address;
        return reinterpret_cast<PyObject*>(py_expired_ref);
    }
    
}
