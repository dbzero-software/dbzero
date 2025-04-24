#pragma once

#include <cstddef>
#include <Python.h>
#include "PyWrapper.hpp"
#include "shared_py_object.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0::python

{
    
    class MemoExpiredRef
    {            
        using Address = db0::Address;
        PyObject_HEAD
        std::uint64_t m_fixture_uuid;
        Address m_address;

    public:
        void init(std::uint64_t fixture_uuid, Address address);

        Address getAddress() const;
        const std::uint64_t getFixtureUUID() const;
    };
    
    extern PyTypeObject MemoExpiredRefType;
    
    bool MemoExpiredRef_Check(PyObject *obj);    
    
    shared_py_object<PyObject*> MemoExpiredRef_new(std::uint64_t fixture_uuid, Address);
    
}