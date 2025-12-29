// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace tests

{
    
    using namespace db0;
    using namespace db0::python;
    using ObjectSharedPtr = shared_py_object<PyObject *>;

    ObjectSharedPtr createTuple()
    {
        auto key = Py_OWN(PyUnicode_FromString("first"));
        auto value = Py_OWN(PyUnicode_FromString("second"));
        return Py_OWN(PySafeTuple_Pack(key, value));
    }
    
    TEST( PySafeAPITest, testPySafeTuple_Pack )
    {
        // initialize Python interpreter
        Py_Initialize();
        for (int i = 0; i < 1000; ++i) {
            auto tuple = createTuple();
            ASSERT_TRUE(tuple.get());
            ASSERT_TRUE(!!tuple);
            ASSERT_EQ(PyTuple_Size(*tuple), 2);
            ASSERT_EQ(PyUnicode_AsUTF8(PyTuple_GetItem(*tuple, 0)), std::string("first"));
            ASSERT_EQ(PyUnicode_AsUTF8(PyTuple_GetItem(*tuple, 1)), std::string("second"));
        }
    }

}