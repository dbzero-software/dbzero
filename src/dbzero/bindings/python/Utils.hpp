// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
#include <Python.h>
#include <iostream>
#include <mutex>
#include "PyToolkit.hpp"
#include <dbzero/bindings/python/PySafeAPI.hpp>

namespace db0::python

{
    
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

    template <typename PyCollection>
    std::optional<bool> has_all_elements_same(PyCollection *collection, PyObject *iter)
    {        
        auto py_collection_iter = Py_OWN(PyObject_GetIter(collection));
        if (!py_collection_iter) {
            THROWF(db0::InputException) <<  "argument must be an iterable";
        }
        
        auto iterator = Py_BORROW(iter);
        ObjectSharedPtr lh;
        Py_FOR(rh, iterator) {
            lh = Py_OWN(PyIter_Next(*py_collection_iter));
            if (!lh) {
                return false;
            }
            auto cmp_result = PyObject_RichCompareBool(*lh, *rh, Py_NE);
            if (cmp_result == -1) {
                // return nullopt on error
                return std::nullopt;
            }
            if (cmp_result == 1) {
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
        Py_FOR(elem, iterator) {        
            if (!sequenceContainsItem(collection, *elem)) {                
                return false;
            }            
        }        
        return true;
    }
    
}