#pragma once

#include <cstddef>
#include <Python.h>
#include "PyWrapper.hpp"
#include "shared_py_object.hpp"

namespace db0::python

{
    
    class MemoExpiredRef
    {            
        PyObject_HEAD
        std::uint64_t m_fixture_uuid;
        std::uint64_t m_address;

    public:
        void init(std::uint64_t fixture_uuid, std::uint64_t address);

        const std::uint64_t getAddress() const;
        const std::uint64_t getFixtureUUID() const;
    };
    
    extern PyTypeObject MemoExpiredRefType;
    
    bool MemoExpiredRef_Check(PyObject *obj);    
    
    shared_py_object<PyObject*> MemoExpiredRef_new(std::uint64_t fixture_uuid,
        std::uint64_t address);
    
}