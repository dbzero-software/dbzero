#include "ByteArray.hpp"
#include "Iterator.hpp"
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python
{

    PyObject *ByteArrayObject_GetItem(ByteArrayObject *bytearray_obj, Py_ssize_t i)
    {
        if (static_cast<std::size_t>(i) >= bytearray_obj->ext().size()) {
            PyErr_SetString(PyExc_IndexError, "bytearray index out of range");
            return NULL;
        }
        bytearray_obj->ext().getFixture()->refreshIfUpdated();
        return PyLong_FromLong((long)bytearray_obj->ext().getItem(i));
    }

    static PySequenceMethods ByteArrayObject_sq = {
        .sq_length = (lenfunc)ByteArrayObject_len,
        .sq_item = (ssizeargfunc)ByteArrayObject_GetItem,
    };

    
    static PyMethodDef ByteArrayObject_methods[] = {
        {NULL}
    };

    PyTypeObject ByteArrayObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.ByteArray",
        .tp_basicsize = ByteArrayObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ByteArrayObject_del,
        .tp_as_sequence = &ByteArrayObject_sq,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero bytearray",
        .tp_methods = ByteArrayObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)ByteArrayObject_new,
        .tp_free = PyObject_Free,        
    };

    ByteArrayObject *ByteArrayObject_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<ByteArrayObject*>(type->tp_alloc(type, 0));
    }

    ByteArrayObject *ByteArrayDefaultObject_new()
    {   
        return ByteArrayObject_new(&ByteArrayObjectType, NULL, NULL);
    }
    
    void ByteArrayObject_del(ByteArrayObject* bytearray_obj)
    {
        // destroy associated DB0 ByteArray instance
        bytearray_obj->ext().~ByteArray();
        Py_TYPE(bytearray_obj)->tp_free((PyObject*)bytearray_obj);
    }

    Py_ssize_t ByteArrayObject_len(ByteArrayObject *bytearray_obj)
    {
        bytearray_obj->ext().getFixture()->refreshIfUpdated();
        return bytearray_obj->ext().size();
    }
    
    ByteArrayObject *makeByteArray(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if(nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "make_bytearray() takes exacly 1 arguments");
            return NULL;
        }
        // make actual DBZero instance, use default fixture
        auto bytearray_object = ByteArrayObject_new(&ByteArrayObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        
        if(PyBytes_Check(args[0])) {
            auto size = PyBytes_GET_SIZE(args[0]);
            auto *str = PyBytes_AsString(args[0]);
            std::byte * bytes = reinterpret_cast<std::byte *>(str);
            db0::object_model::ByteArray::makeNew(&bytearray_object->ext(), *fixture, bytes, size);
        } else {
            PyErr_SetString(PyExc_TypeError, "bytearray() targument needs to be bytearray");
            return NULL;
        }
        // register newly created bytearray with py-object cache
        (*fixture)->getLangCache().add(bytearray_object->ext().getAddress(), bytearray_object, true);
        return bytearray_object;
    }
    
    bool ByteArrayObject_Check(PyObject *object)
    {
        return Py_TYPE(object) == &ByteArrayObjectType;        
    }

}