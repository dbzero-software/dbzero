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

PyObject *db0::python::uint64ToPyDecimal(std::uint64_t decimal)
{

    PyObject *decimal_type = getDecimalClass();
    auto numerator = get_bytes(decimal, 0, 57);
    std::int64_t exponent = get_bytes(decimal, 57, 6);
    int is_negative = get_bytes(decimal, 63, 1);
    exponent = -exponent;
    if(decimal == 0){
        return PyObject_CallFunctionObjArgs(decimal_type, PyLong_FromLong(0), NULL);
    }
    if(is_negative){
        numerator = -numerator;
    }
    PyObject *numerator_py = PyLong_FromLong(numerator);
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
    if (PyErr_Occurred()) {
        PyErr_Print();
        throw std::runtime_error("Error occurred while converting numerator to long.");
    }
    auto result = PyLong_AsLong(intValue);
    Py_DECREF(decimal_integral);
    Py_DECREF(intValue);
    return result;

}

std::uint64_t decimal_tuple_to_uint64(PyObject *as_tuple, int max_lenght){
    if (!as_tuple || !PyTuple_Check(as_tuple)) {
        PyErr_Print();
        std::cerr << "Error occurred while calling as_tuple." << std::endl;
        throw std::runtime_error("Error occurred while calling as_tuple.");
    }
    std::uint64_t decimal = 0;
    auto iterator = PyObject_GetIter(as_tuple);
    while(auto item = PyIter_Next(iterator)){
        if(max_lenght == 0){
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

std::uint64_t db0::python::pyDecimalToUint64(PyObject *py_datetime)
{
    // check if decimal is 0
    if(PyObject_IsTrue(PyObject_CallMethod(py_datetime, "is_zero", nullptr))){
        return 0;
    }
    PyObject *as_tuple = PyObject_CallMethod(py_datetime, "as_tuple", nullptr);
    auto exponent_obj = PyTuple_GetItem(as_tuple, 2);
    auto exponent = PyLong_AsLongLong(exponent_obj);
    exponent = abs(exponent);
    if(exponent > 63){
        std::cerr << "Decimal out of range." << std::endl;
        throw std::runtime_error("Decimal out of range.");
    }
    //Py_DECREF(exponent_obj);

    auto sign_obj = PyTuple_GetItem(as_tuple, 0);
    auto is_negative = PyObject_IsTrue(sign_obj);
    Py_DECREF(sign_obj);

    auto number_tuple_object = PyTuple_GetItem(as_tuple, 1);
    auto number_lenght = PyTuple_Size(number_tuple_object);
    // if(number_lenght > 17){
    //     std::cerr << "Decimal out of range." << std::endl;
    //     throw std::runtime_error("Decimal out of range.");
    // 
    if(number_lenght > 17){
        exponent -= (number_lenght- 17);
        if (exponent < 0) {
            std::cerr << "Decimal out of range." << std::endl;
            throw std::runtime_error("Decimal out of range.");
        }
    }
    auto integer_value = decimal_tuple_to_uint64(number_tuple_object, 17);
    Py_DECREF(as_tuple);
    //Py_DECREF(number_tuple_object);
    std::uint64_t decimal = 0;
    set_bytes(decimal, 0, 57, integer_value);
    set_bytes(decimal, 57, 6, exponent);
    set_bytes(decimal, 63, 1, is_negative);
    return decimal;
}