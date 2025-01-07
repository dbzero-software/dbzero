#include "PyDecimal.hpp"
#include <dbzero/bindings/python/types/ByteUtils.hpp>

static PyObject *decimal_module = nullptr;
static PyObject *decimal_class = nullptr;

PyObject* db0::python::getDecimalClass() {
    if(decimal_class == nullptr){
        if(decimal_module == nullptr){
            decimal_module = PyImport_ImportModule("decimal");
            if (decimal_module == NULL) {
                PyErr_Print(); // Print the error if the attribute cannot be retrieved
                Py_DECREF(decimal_module);
                return nullptr; // Return an error
            }
        }
        decimal_class = PyObject_GetAttrString(decimal_module, "Decimal");
        if (decimal_class == NULL) {
            PyErr_Print(); // Print the error if the attribute cannot be retrieved
            Py_DECREF(decimal_module);
            return nullptr; // Handle the error appropriately
        }
        Py_DECREF(decimal_module);
    }
    return decimal_class;
}

PyObject *db0::python::uint64ToPyDecimal(std::uint64_t datetime)
{
    auto numerator = get_bytes(datetime, 0, 58);
    std::int64_t exponent = get_bytes(datetime, 58, 5);
    exponent = -exponent;
    PyObject *numerator_py = PyLong_FromLong(numerator);
    PyObject *decimal_type = getDecimalClass();
    PyObject* decimal_value = PyObject_CallFunctionObjArgs(decimal_type, numerator_py, NULL);
    PyObject* decimal_result = PyObject_CallMethod(decimal_value, "scaleb", "l", exponent);
    Py_DECREF(decimal_value);
    Py_DECREF(numerator_py);
        // check if error set
    if (PyErr_Occurred()) {
        PyErr_Print();
        throw std::runtime_error("Error occurred while converting numerator to long.");
    }
    return decimal_result;
}

std::int64_t decimalToIntegral(PyObject *decimal){
    auto decimal_integral = PyObject_CallMethod(decimal, "to_integral_value", nullptr);
    PyObject* intValue = PyObject_CallFunctionObjArgs((PyObject*)&PyLong_Type, decimal_integral, NULL);
    auto result = PyLong_AsLong(intValue);
    Py_DECREF(decimal_integral);
    Py_DECREF(intValue);
    return result;

}

std::uint64_t db0::python::pyDecimalToUint64(PyObject *py_datetime)
{

    PyObject *as_tuple = PyObject_CallMethod(py_datetime, "as_tuple", nullptr);
    auto exponent_obj = PyTuple_GetItem(as_tuple, 2);
    auto exponent = PyLong_AsLongLong(exponent_obj);
    PyObject *log_decimal = PyObject_CallMethod(py_datetime, "log10", nullptr);
    auto log_value = decimalToIntegral(log_decimal);
    Py_DECREF(log_decimal);
    Py_DECREF(as_tuple);
    Py_DECREF(exponent_obj);
    if (!as_tuple) {
        PyErr_Print();
        throw std::runtime_error("Error occurred while calling as_tuple.");
    }
    exponent = -exponent;
    if(log_value + exponent > 15){
        throw std::runtime_error("Decimal out of range.");
    }
    PyObject *integer_obj = PyObject_CallMethod(py_datetime, "shift", "l", exponent);
    auto integer_value = decimalToIntegral(integer_obj);
    // check if error set
    if (PyErr_Occurred()) {
        PyErr_Print();
        throw std::runtime_error("Error occurred while converting numerator to long.");
    }
    std::uint64_t decimal = 0;
    set_bytes(decimal, 0, 58, integer_value);
    set_bytes(decimal, 58, 5, exponent);
    return decimal;
}