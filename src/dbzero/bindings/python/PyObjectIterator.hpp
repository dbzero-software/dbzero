#pragma once

#include "PyWrapper.hpp"
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>

namespace db0::python

{

    struct Iterator
    {
        // m_iterator always present
        std::unique_ptr<db0::object_model::ObjectIterator> m_iterator;
        // typed object iterator only initialized for TypedObjectIterator wrapper
        db0::object_model::TypedObjectIterator *m_typed_iterator_ptr = nullptr;

        inline db0::object_model::ObjectIterator *operator->() {
            return m_iterator.get();
        }

        inline const db0::object_model::ObjectIterator *operator->() const {
            return m_iterator.get();
        }

        inline db0::object_model::ObjectIterator &operator*() {
            return *m_iterator;
        }

        inline const db0::object_model::ObjectIterator &operator*() const {
            return *m_iterator;
        }

        bool isTyped() const;
        
        static void makeNew(void *at_ptr, std::unique_ptr<db0::object_model::ObjectIterator> &&);

        static void makeNew(void *at_ptr, std::unique_ptr<db0::object_model::TypedObjectIterator> &&);
    };
    
    using PyObjectIterator = PyWrapper<Iterator, false>;
    
    PyObjectIterator *PyObjectIterator_new(PyTypeObject *type, PyObject *, PyObject *);
    shared_py_object<PyObjectIterator*> PyObjectIteratorDefault_new();
    void PyObjectIterator_del(PyObjectIterator* self);
    
    extern PyTypeObject PyObjectIteratorType;
        
    bool PyObjectIterator_Check(PyObject *);
    
    /**
     * db0.find implementation
     * returns either ObjectIterator or TypedObjectIterator wrapper
    */
    PyObject *find(PyObject *, PyObject* const *args, Py_ssize_t nargs);
    
}