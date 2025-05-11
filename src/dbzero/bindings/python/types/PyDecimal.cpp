#include "PyDecimal.hpp"
#include <dbzero/bindings/python/types/ByteUtils.hpp>

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
        auto numerator = get_bytes(decimal, 0, 57);
        std::int64_t exponent = get_bytes(decimal, 57, 6);
        int is_negative = get_bytes(decimal, 63, 1);
        exponent = -exponent;
        if (decimal == 0) {
            return PyObject_CallFunctionObjArgs(decimal_type, PyLong_FromLong(0), NULL);
        }
        if (is_negative) {
            numerator = -numerator;
        }
        PyObject *numerator_py = PyLong_FromLong(numerator);
        PyObject* decimal_value = PyObject_CallFunctionObjArgs(decimal_type, numerator_py, NULL);
        PyObject* decimal_result = PyObject_CallMethod(decimal_value, "scaleb", "l", exponent);
        Py_DECREF(decimal_value);
        Py_DECREF(numerator_py);
        // check if error set
        if (PyErr_Occurred()) {
            THROWF(db0::InputException) << "Error occurred while converting numerator to long." << THROWF_END;
        }
        return decimal_result;
    }

    std::int64_t decimalToIntegral(PyObject *decimal)
    {
        auto decimal_integral = PyObject_CallMethod(decimal, "to_integral_value", nullptr);
        PyObject* intValue = PyObject_CallFunctionObjArgs((PyObject*)&PyLong_Type, decimal_integral, NULL);
        if (PyErr_Occurred()) {
            THROWF(db0::InputException) << "Error occurred while converting numerator to long." << THROWF_END;
        }

        auto result = PyLong_AsLong(intValue);
        Py_DECREF(decimal_integral);
        Py_DECREF(intValue);
        return result;

    }

    std::uint64_t decimal_tuple_to_uint64(PyObject *as_tuple, int max_lenght)
    {
        if (!as_tuple || !PyTuple_Check(as_tuple)) {
            THROWF(db0::InputException) << "Invalid type of object. Tuple expected" << THROWF_END;
        }
        std::uint64_t decimal = 0;
        auto iterator = PyObject_GetIter(as_tuple);
        if (!iterator) {
            THROWF(db0::InputException) << "Invalid type of object. Tuple expected" << THROWF_END;
        }

        while (auto item = PyIter_Next(iterator)) {
            if (max_lenght == 0) {
                Py_DECREF(item);
                break;
            }
            decimal *= 10;
            auto value = PyLong_AsLong(item);
            Py_DECREF(item);
            decimal += value;
            max_lenght--;
        }
        Py_DECREF(iterator);
        return decimal;
    }

    std::uint64_t pyDecimalToUint64(PyObject *py_datetime)
    {
        // check if decimal is 0
        if (PyObject_IsTrue(PyObject_CallMethod(py_datetime, "is_zero", nullptr))){
            return 0;
        }

        PyObject *as_tuple = PyObject_CallMethod(py_datetime, "as_tuple", nullptr);
        auto exponent_obj = PyTuple_GetItem(as_tuple, 2);
        auto exponent = PyLong_AsLongLong(exponent_obj);
        exponent = abs(exponent);
        if (exponent > 63) {
            Py_DECREF(as_tuple);
            THROWF(db0::InputException) << "Decimal out of range." << THROWF_END;            
        }

        auto sign_obj = PyTuple_GetItem(as_tuple, 0);
        auto is_negative = PyObject_IsTrue(sign_obj);
        Py_DECREF(sign_obj);

        auto number_tuple_object = PyTuple_GetItem(as_tuple, 1);
        auto number_lenght = PyTuple_Size(number_tuple_object);

        if (number_lenght > 17) {
            exponent -= (number_lenght- 17);
            if (exponent < 0) {
                THROWF(db0::InputException) << "Decimal out of range." << THROWF_END;
            }
        }

        std::uint64_t integer_value = 0;
        try {
            integer_value = decimal_tuple_to_uint64(number_tuple_object, 17); 
        } catch (...) {
            Py_DECREF(number_tuple_object);
            Py_DECREF(as_tuple);
            throw;
        }

        Py_DECREF(number_tuple_object);
        Py_DECREF(as_tuple);
        
        std::uint64_t decimal = 0;
        set_bytes(decimal, 0, 57, integer_value);
        set_bytes(decimal, 57, 6, exponent);
        set_bytes(decimal, 63, 1, is_negative);
        return decimal;
    }
    
}