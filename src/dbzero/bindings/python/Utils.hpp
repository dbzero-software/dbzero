#pragma once
#include <Python.h>
#include <iostream>
#include <mutex>
#include "PyToolkit.hpp"

namespace db0::python

{
    
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

    template <typename PyCollection>
    bool has_all_elements_same(PyCollection *collection, PyObject *iterator)
    {        
        auto py_collection_iter = Py_OWN(PyObject_GetIter(collection));
        if (!py_collection_iter) {
            THROWF(db0::InputException) <<  "argument must be an iterable";
        }

        ObjectSharedPtr lh, rh;
        while ((rh = Py_OWN(PyIter_Next(iterator)))) {
            lh = Py_OWN(PyIter_Next(*py_collection_iter));
            if (!lh) {
                return false;
            }

            if (PyObject_RichCompareBool(*lh, *rh, Py_NE) == 1) {
                return false;
            }
        }
        lh = Py_OWN(PyIter_Next(*py_collection_iter));        
        return lh.get() == nullptr;
    }
    
    template <typename PyCollection>
    bool has_all_elements_in_collection(PyCollection *collection, PyObject *object)
    {                
        auto iterator = Py_OWN(PyObject_GetIter(object));
        if (!iterator) {
            THROWF(db0::InputException) <<  "argument must be an iterable";
        }

        ObjectSharedPtr elem;
        while ((elem = Py_OWN(PyIter_Next(*iterator)))) {
            if (!sequenceContainsItem(collection, *elem)) {                
                return false;
            }            
        }        
        return true;
    }
    
    PyObject * PyBool_fromBool(bool value);

}