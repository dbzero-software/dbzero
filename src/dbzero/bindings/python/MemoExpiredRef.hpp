#pragma once

#include <cstddef>
#include <Python.h>
#include "PyWrapper.hpp"
#include "shared_py_object.hpp"

namespace db0::python

{
    
    struct MemoExpiredRef
    {
        PyObject_HEAD        
        std::uint64_t m_fixture_uuid;
        std::uint64_t m_address;
    };
        
    extern PyTypeObject MemoExpiredRefType;
    
    bool MemoExpiredRef_Check(PyObject *obj);
    void MemoExpiredRef_del(MemoExpiredRef *self);
    shared_py_object<PyObject*> MemoExpiredRef_new(std::uint64_t fixture_uuid,
        std::uint64_t address);
    
}