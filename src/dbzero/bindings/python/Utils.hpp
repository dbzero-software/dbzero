#pragma once
#include <Python.h>
#include <iostream>

namespace db0::python

{
    
    template<typename PyCollection>
    bool has_all_elements_same(PyCollection *collection, PyObject *iterator){
        PyObject *lh, *rh;
        PyObject *py_collection_iter = PyObject_GetIter(collection);
        while ((rh = PyIter_Next(iterator))) {
            lh = PyIter_Next(py_collection_iter);
            if(lh == nullptr){
                return false;
            }
            if(PyObject_RichCompareBool(lh, rh, Py_NE) == 1){
                Py_DECREF(lh);
                Py_DECREF(rh);
                return false;
            }
            Py_DECREF(lh);
            Py_DECREF(rh);
        }
        lh = PyIter_Next(py_collection_iter);
        Py_DECREF(py_collection_iter);
        return lh == nullptr;
    }

    template<typename PyCollection>
    bool has_all_elements_in_collection(PyCollection *collection, PyObject *object){
        PyObject* elem;
        PyObject* iterator = PyObject_GetIter(object);
        int index = 0;
        while ((elem = PyIter_Next(iterator))) {
            if(!collection->ext().has_item(elem)){
                std::cerr << "COMPARE 2 FALSE " << index << std::endl;
                return false;
            }
            ++index;
            Py_DECREF(elem);
        }
        Py_DECREF(iterator);
        return true;
    }

    PyObject * PyBool_fromBool(bool value);

}