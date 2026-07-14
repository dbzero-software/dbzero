// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <Python.h>
#include "PyWrapper.hpp"
#include "shared_py_object.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0::python

{
    
    using Address = db0::Address;
    using UniqueAddress = db0::UniqueAddress;
    
    class MemoExpiredRef
    {
        PyObject_HEAD
        std::uint64_t m_fixture_uuid;
        UniqueAddress m_address;

    public:
        void init(std::uint64_t fixture_uuid, UniqueAddress address);

        Address getAddress() const;
        UniqueAddress getUniqueAddress() const;

        const std::uint64_t getFixtureUUID() const;
    };
    
    extern PyTypeObject MemoExpiredRefType;
    
    bool MemoExpiredRef_Check(PyObject *obj);    
    
    shared_py_object<PyObject*> MemoExpiredRef_new(std::uint64_t fixture_uuid, UniqueAddress);
    
}