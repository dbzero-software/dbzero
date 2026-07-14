// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyDecimal.hpp"
#include <dbzero/bindings/python/types/ByteUtils.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0::python

{

    static PyObject *decimal_module = nullptr;
    static PyObject *decimal_class = nullptr;

    PyObject *getDecimalClass()
    {
        if (decimal_class == nullptr) {
            if (decimal_module == nullptr) {
                decimal_module = PyImport_ImportModule("decimal");
                if (decimal_module == NULL) {                    
                    Py_DECREF(decimal_module);
                    return nullptr; // Return an error
                }
            }
            decimal_class = PyObject_GetAttrString(decimal_module, "Decimal");
            if (decimal_class == NULL) {                
                Py_DECREF(decimal_module);
                return nullptr; // Handle the error appropriately
            }
            Py_DECREF(decimal_module);
        }
        return decimal_class;
    }
    
    PyObject *uint64ToPyDecimal(std::uint64_t decimal)
    {
        PyObject *decimal_type = getDecimalClass();
        std::int64_t numerator = get_bytes(decimal, 0, 57);

        std::int64_t exponent = get_bytes(decimal, 57, 6);
        int is_negative = get_bytes(decimal, 63, 1);
        exponent = -exponent;
        if (decimal == 0) {
            return PyObject_CallFunctionObjArgs(decimal_type, *Py_OWN(PyLong_FromLong(0)), NULL);
        }
        if (is_negative) {
            numerator = -numerator;
        }
        auto numerator_py = Py_OWN(PyLong_FromLongLong(numerator));
        if (!numerator_py) {
            return nullptr;
        }
        auto decimal_value = Py_OWN(PyObject_CallFunctionObjArgs(decimal_type, *numerator_py, NULL));
        if (!decimal_value) {
            return nullptr;
        }
        auto decimal_result = Py_OWN(PyObject_CallMethod(*decimal_value, "scaleb", "l", exponent));
        if (!decimal_result) {
            return nullptr;
        }
        return decimal_result.steal();
    }
    
    std::uint64_t decimal_tuple_to_uint64(PyObject *as_tuple, int max_lenght)
    {
        if (!as_tuple || !PyTuple_Check(as_tuple)) {
            THROWF(db0::InputException) << "Invalid type of object. Tuple expected" << THROWF_END;
        }
        std::uint64_t decimal = 0;
        auto iterator = Py_OWN(PyObject_GetIter(as_tuple));
        if (!iterator) {
            THROWF(db0::InputException) << "Invalid type of object. Tuple expected" << THROWF_END;
        }

        Py_FOR(item, iterator) {
            if (max_lenght == 0) {                
                break;
            }
            decimal *= 10;
            auto value = PyLong_AsLong(*item);
            decimal += value;
            --max_lenght;
        }
        return decimal;
    }
    
    std::uint64_t pyDecimalToUint64(PyObject *py_decimal)
    {   
        auto as_tuple = Py_OWN(PyObject_CallMethod(py_decimal, "as_tuple", nullptr));
        if (!as_tuple) {
            THROWF(db0::InputException) << "as_tuple failed" << THROWF_END;
        }
        
        auto exponent = abs(PyLong_AsLongLong(PyTuple_GetItem(*as_tuple, 2)));
        if (exponent > 63) {
            THROWF(db0::InputException) << "Decimal out of range." << THROWF_END;            
        }
        
        auto is_negative = PyObject_IsTrue(PyTuple_GetItem(*as_tuple, 0));
        auto number_tuple_object = PyTuple_GetItem(*as_tuple, 1);
        auto number_lenght = PyTuple_Size(number_tuple_object);

        if (number_lenght > 17) {
            exponent -= (number_lenght- 17);
            if (exponent < 0) {
                THROWF(db0::InputException) << "Decimal out of range." << THROWF_END;
            }
        }
        
        std::uint64_t integer_value = decimal_tuple_to_uint64(number_tuple_object, 17);
        
        std::uint64_t decimal = 0;
        set_bytes(decimal, 0, 57, integer_value);
        set_bytes(decimal, 57, 6, exponent);
        set_bytes(decimal, 63, 1, is_negative);
        return decimal;
    }
    
}