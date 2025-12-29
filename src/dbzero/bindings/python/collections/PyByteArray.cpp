// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyByteArray.hpp"
#include <functional>
#include "PyIterator.hpp"
#include "CollectionMethods.hpp"
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>

namespace db0::python

{

    static PySequenceMethods ByteArrayObject_sq = getPySequenceMehods<ByteArrayObject>();

    void makeByteArrayFromPyBytes(db0::swine_ptr<Fixture> &fixture, ByteArrayObject * bytearray_object,
        PyObject *py_bytes)
    {
        auto size = PyBytes_GET_SIZE(py_bytes);
        // this is a pointer to an internal buffer, needs not to be deallocated
        auto safe_str = PyBytes_AsString(py_bytes);
        std::byte *bytes = reinterpret_cast<std::byte *>(safe_str);
        bytearray_object->makeNew(fixture, bytes, size);
    }
    
    PyObject* asPyObject(ByteArrayObject *bytearray_obj)
    {
        char * bytes_str = new char[bytearray_obj->ext().size() + 1];       
        for (unsigned int i = 0; i < bytearray_obj->ext().size(); i++) {
            bytes_str[i] = static_cast<char>(bytearray_obj->ext().getByte(i));
        }
        bytes_str[bytearray_obj->ext().size()] = '\0';
        auto result = Py_OWN(PyBytes_FromStringAndSize(bytes_str, bytearray_obj->ext().size()));
        delete [] bytes_str;
        return result.steal();
    }

    PyObject *callMethod(const char * name, ByteArrayObject *object_inst, PyObject* args, PyObject* kwargs)
    {
        auto py_obj = Py_OWN(asPyObject(object_inst));
        auto function = Py_OWN(PyObject_GetAttrString(*py_obj, name));
        if (!function) {
            PyErr_Format(PyExc_AttributeError, "ByteArray object has no attribute '%s'", name);
            return NULL;
        }
        return PyObject_Call(*function, args, kwargs);
    }
    
    PyObject *ByteArray_CallMethod(const char * name, ByteArrayObject *object_inst, PyObject* args, PyObject* kwargs)
    {
        auto py_obj = callMethod(name, object_inst, args, kwargs);
        if (!py_obj) {
            return NULL;
        }
        auto bytearray_object = ByteArrayObject_new(&ByteArrayObjectType, NULL, NULL);
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        makeByteArrayFromPyBytes(*lock, bytearray_object, py_obj);
        lock->getLangCache().add(bytearray_object->ext().getAddress(), bytearray_object);
        return bytearray_object;
    }

    static std::unordered_map<std::string, PyCFunction> methods;

    PyMethodDef getMethod(const char *name) {
        PyMethodDef method = {name, (PyCFunction)methods[name], METH_VARARGS | METH_KEYWORDS, ""};
        return method;
    }

    #define ADD_CALL_METHOD(NAME) \
        PyObject *ByteArray_##NAME(ByteArrayObject *object_inst, PyObject* args, PyObject* kwargs) { \
            PY_API_FUNC \
            return runSafe(callMethod, #NAME, object_inst, args, kwargs); \
        }

    #define ADD_BYTEARRAY_CALL_METHOD(NAME) \
        PyObject *ByteArray_##NAME(ByteArrayObject *object_inst, PyObject* args, PyObject* kwargs) { \
            PY_API_FUNC \
            return runSafe(ByteArray_CallMethod, #NAME, object_inst, args, kwargs); \
        }

    ADD_BYTEARRAY_CALL_METHOD(capitalize)
    ADD_BYTEARRAY_CALL_METHOD(removeprefix)
    ADD_BYTEARRAY_CALL_METHOD(removesuffix)
    ADD_CALL_METHOD(decode)
    ADD_CALL_METHOD(endswith)
    ADD_CALL_METHOD(find)
    ADD_CALL_METHOD(index)
    ADD_BYTEARRAY_CALL_METHOD(join)
    ADD_CALL_METHOD(partition)
    ADD_BYTEARRAY_CALL_METHOD(replace)
    ADD_CALL_METHOD(rfind)
    ADD_CALL_METHOD(rindex)
    ADD_CALL_METHOD(rpartition)
    ADD_CALL_METHOD(startswith)
    ADD_BYTEARRAY_CALL_METHOD(translate)
    ADD_BYTEARRAY_CALL_METHOD(center)
    ADD_BYTEARRAY_CALL_METHOD(ljust)
    ADD_BYTEARRAY_CALL_METHOD(lstrip)
    ADD_BYTEARRAY_CALL_METHOD(rjust)
    ADD_BYTEARRAY_CALL_METHOD(rstrip)
    ADD_CALL_METHOD(split)
    ADD_BYTEARRAY_CALL_METHOD(strip)
    ADD_BYTEARRAY_CALL_METHOD(expandtabs)
    ADD_CALL_METHOD(isalnum)
    ADD_CALL_METHOD(isalpha)
    ADD_CALL_METHOD(isascii)
    ADD_CALL_METHOD(isdigit)
    ADD_CALL_METHOD(islower)
    ADD_CALL_METHOD(isspace)
    ADD_CALL_METHOD(istitle)
    ADD_CALL_METHOD(isupper)
    ADD_BYTEARRAY_CALL_METHOD(lower)
    ADD_CALL_METHOD(splitlines)
    ADD_BYTEARRAY_CALL_METHOD(swapcase)
    ADD_BYTEARRAY_CALL_METHOD(title)
    ADD_BYTEARRAY_CALL_METHOD(upper)
    ADD_BYTEARRAY_CALL_METHOD(zfill)

    PyObject * tryPyAPI_ByteArray_Count(ByteArrayObject *object_inst, PyObject *const *args, Py_ssize_t nargs){
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "count() takes exactly one argument");
            return NULL;
        }
        auto arg = args[0];
        if (PyLong_Check(arg)) {
            long value = PyLong_AsLong(arg);
            if (value < 0 || value > 255) {
                PyErr_SetString(PyExc_ValueError, "byte must be in range(0, 256)");
                return NULL;
            }
            std::byte byte = (std::byte)value;
            return PyLong_FromLong(object_inst->ext().count(byte));
        } else if (PyBytes_Check(arg)){
            auto size = PyBytes_GET_SIZE(arg);
            // this is a pointer to an internal buffer, needs not to be deallocated
            auto safe_str = PyBytes_AsString(arg);
            return PyLong_FromLong(object_inst->ext().count(reinterpret_cast<std::byte *>(safe_str), size));
        } else if (ByteArrayObject_Check(arg)) {
            auto py_array = reinterpret_cast<ByteArrayObject *>(arg);
            return PyLong_FromLong(object_inst->ext().count(py_array->ext(), py_array->ext().size()));
        } else {
            PyErr_SetString(PyExc_TypeError, "count() takes an integer, bytearray or bytes as argument");
            return NULL;
        }
        return 0;
    }

    PyObject *PyAPI_ByteArray_Count(ByteArrayObject *object_inst, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryPyAPI_ByteArray_Count, object_inst, args, nargs);
    }

    #define CREATE_METHOD_DEF(NAME, DESCRIPTION) \
        {#NAME, (PyCFunction)ByteArray_##NAME, METH_VARARGS | METH_KEYWORDS, DESCRIPTION}

    static PyMethodDef ByteArrayObject_methods[] = {
        {"append", (PyCFunction)PyAPI_ObjectT_append<ByteArrayObject>, METH_FASTCALL, "Append an item to the container."},
        {"extend", (PyCFunction)PyAPI_ObjectT_extend<ByteArrayObject>, METH_FASTCALL, "Add the elements of a any iterable, to the end of the current BytesArray."},
        {"insert", (PyCFunction)PyAPI_ObjectT_Insert<ByteArrayObject>, METH_FASTCALL, "Insert the elements to position in ByteArray"},
        CREATE_METHOD_DEF(capitalize, "Capitalize ByteArray"),
        CREATE_METHOD_DEF(removeprefix, "Remove prefix from ByteArray"),
        CREATE_METHOD_DEF(removesuffix, "Remove suffix from ByteArray"),
        CREATE_METHOD_DEF(decode, "Return a string decoded from the ByteArray.\
                                   The bytes in the ByteArray are decoded into characters using the given encoding.\
                                   Errors are handled according to the errors argument.\
                                   The encoding defaults to 'utf-8'.\
                                   The errors argument defaults to 'strict'.\
                                   Returns a string."),
        CREATE_METHOD_DEF(endswith, "Return True if ByteArray ends with the specified suffix, False otherwise.\
                                     With optional start, test beginning at that position.\
                                     With optional end, stop comparing at that position.\
                                     suffix can also be a tuple of suffixes to look for."),
        CREATE_METHOD_DEF(find, "Return the lowest index in the ByteArray where substring sub is found, such that sub is contained within s[start:end].\
                                 Optional arguments start and end are interpreted as in slice notation.\
                                 Return -1 on failure."),
        CREATE_METHOD_DEF(index, "Return the lowest index in the ByteArray where substring sub is found, such that sub is contained within s[start:end].\
                                  Optional arguments start and end are interpreted as in slice notation.\
                                  Return -1 on failure."),
        CREATE_METHOD_DEF(join, "Return a string which is the concatenation of the strings in the sequence seq.\
                                The separator between elements is the string providing this method.\
                                The sequence seq must be of type list, tuple, or set.\
                                If seq is a list, tuple or set, the elements must be strings.\
                                If the elements are not strings, this method raises a TypeError."),
        CREATE_METHOD_DEF(partition, "Return a 3-tuple containing the part before the first occurrence of separator, the separator itself, and the part after it.\
                                       If the separator is not found, return a 3-tuple containing the string itself, followed by two empty strings."),
        CREATE_METHOD_DEF(replace, "Return a copy of the string with all occurrences of substring old replaced by new.\
                                    If the optional argument count is given, only the first count occurrences are replaced."),
        CREATE_METHOD_DEF(rfind, "Return the highest index in the ByteArray where substring sub is found, such that sub is contained within s[start:end].\
                                   Optional arguments start and end are interpreted as in slice notation. Return -1 on failure."),
        CREATE_METHOD_DEF(rindex, "Return the highest index in the ByteArray where substring sub is found, such that sub is contained within s[start:end].\
                                   Optional arguments start and end are interpreted as in slice notation. Return -1 on failure."),
        CREATE_METHOD_DEF(rpartition, "Return a 3-tuple containing the part before the separator, the separator itself, and the part after it.\
                                       If the separator is not found, return a 3-tuple containing two empty strings followed by the string itself."),
        CREATE_METHOD_DEF(startswith, "Return True if ByteArray starts with str, False otherwise"),
        CREATE_METHOD_DEF(translate, "Return a copy of the string in which each character has been mapped through the given translation table.\
                                      The table must implement lookup/indexing via __getitem__(), for instance a dictionary or list,\
                                      mapping Unicode ordinals to Unicode ordinals, strings, or None.\
                                      Unmapped characters are left untouched. Characters mapped to None are deleted."),
        CREATE_METHOD_DEF(center, "Return a centered string of length width. Padding is done using the specified fill character (default is a space)."),
        CREATE_METHOD_DEF(ljust, "Return a left-justified string of length width. Padding is done using the specified fill character (default is a space)."),
        CREATE_METHOD_DEF(lstrip, "Return a copy of the string with leading whitespace removed."),
        CREATE_METHOD_DEF(rjust, "Return a right-justified string of length width. Padding is done using the specified fill character (default is a space)."),
        CREATE_METHOD_DEF(rstrip, "Return a copy of the string with trailing whitespace removed."),
        CREATE_METHOD_DEF(split, "Return a list of the words in the string, using sep as the delimiter string."),
        CREATE_METHOD_DEF(strip, "Return a copy of the string with leading and trailing whitespace removed."),
        CREATE_METHOD_DEF(expandtabs, "Return a copy where all ASCII tab characters are replaced by one or more ASCII spaces"),
        CREATE_METHOD_DEF(isalnum, "Return True if all characters in the string are alphanumeric and there is at least one character, False otherwise."),
        CREATE_METHOD_DEF(isalpha, "Return True if all characters in the string are alphabetic and there is at least one character, False otherwise."),
        CREATE_METHOD_DEF(isascii, "Return True if all characters in the string are ASCII, False otherwise."),
        CREATE_METHOD_DEF(isdigit, "Return True if all characters in the string are digits and there is at least one character, False otherwise."),
        CREATE_METHOD_DEF(islower, "Return True if all cased characters in the string are lowercase and there is at least one cased character, False otherwise."),
        CREATE_METHOD_DEF(isspace, "Return True if there are only whitespace characters in the string and there is at least one character, False otherwise."),
        CREATE_METHOD_DEF(istitle, "Return True if the string is a titlecased string and there is at least one character, for example uppercase characters may only follow uncased characters and lowercase characters only cased ones. Return False otherwise."),
        CREATE_METHOD_DEF(isupper, "Return True if all cased characters in the string are uppercase and there is at least one cased character, False otherwise."),
        CREATE_METHOD_DEF(lower, "Return a copy of the string converted to lowercase."),
        CREATE_METHOD_DEF(splitlines, "Return a list of the lines in the string, breaking at line boundaries."),
        CREATE_METHOD_DEF(swapcase, "Return a copy of the string with uppercase characters converted to lowercase and vice versa."),
        CREATE_METHOD_DEF(title, "Return a version of the string where each word is titlecased."),
        CREATE_METHOD_DEF(upper, "Return a copy of the string converted to uppercase."),
        CREATE_METHOD_DEF(zfill, "Return a copy of the string left filled with ASCII '0' digits to make a string of length width."),
        {"count", (PyCFunction)PyAPI_ByteArray_Count, METH_FASTCALL, "Count occurences of byte in ByteArray"},
        {NULL}
    };
    
    static PyObject *tryPyAPI_ByteArrayObject_rq(ByteArrayObject *list_obj, PyObject *other, int op) 
    {
        if (ByteArrayObject_Check(other)) {
            ByteArrayObject * other_list = (ByteArrayObject*) other;
            switch (op)
            {
            case Py_EQ:
                return PyBool_fromBool(list_obj->ext() == other_list->ext());
            case Py_NE:
                return PyBool_fromBool(list_obj->ext() != other_list->ext());
            default:
                Py_RETURN_NOTIMPLEMENTED;
            }
        } else {
            Py_RETURN_NOTIMPLEMENTED;
        }
    }

    static PyObject *PyAPI_ByteArrayObject_rq(ByteArrayObject *list_obj, PyObject *other, int op) 
    {
        PY_API_FUNC
        return runSafe(tryPyAPI_ByteArrayObject_rq, list_obj, other, op);
    }

    PyTypeObject ByteArrayObjectType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "ByteArray",
        .tp_basicsize = ByteArrayObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyAPI_ByteArrayObject_del,
        .tp_as_sequence = &ByteArrayObject_sq,
        .tp_flags =  Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero bytearray",
        .tp_richcompare = (richcmpfunc)PyAPI_ByteArrayObject_rq,
        .tp_methods = ByteArrayObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)ByteArrayObject_new,
        .tp_free = PyObject_Free,   
    };
    
    ByteArrayObject *ByteArrayObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        // not API method, lock not needed (otherwise may cause deadlock)
        return reinterpret_cast<ByteArrayObject*>(type->tp_alloc(type, 0));
    }

    shared_py_object<ByteArrayObject *> ByteArrayDefaultObject_new() {
        // not API method, lock not needed (otherwise may cause deadlock)
        return { ByteArrayObject_new(&ByteArrayObjectType, NULL, NULL), false };
    }
    
    void PyAPI_ByteArrayObject_del(ByteArrayObject* bytearray_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 ByteArray instance
        bytearray_obj->destroy();
        Py_TYPE(bytearray_obj)->tp_free((PyObject*)bytearray_obj);
    }
    
    ByteArrayObject *tryPyAPI_makeByteArray(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {

        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "make_bytearray() takes exacly 1 arguments");
            return NULL;
        }
        // make actual dbzero instance, use default fixture
        auto bytearray_object = ByteArrayDefaultObject_new();
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        if (PyBytes_Check(args[0])) {
            makeByteArrayFromPyBytes(*lock, bytearray_object.get(), args[0]);
        } else {
            PyErr_SetString(PyExc_TypeError, "bytearray() argument needs to be bytearray");
            return NULL;
        }
        // register newly created bytearray with py-object cache
        lock->getLangCache().add(bytearray_object.get()->ext().getAddress(), bytearray_object.get());
        return bytearray_object.steal();
    }
    
    ByteArrayObject *PyAPI_makeByteArray(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
        PY_API_FUNC
        return runSafe(tryPyAPI_makeByteArray, self, args, nargs);
    }
    
    bool ByteArrayObject_Check(PyObject *object) {
        return Py_TYPE(object) == &ByteArrayObjectType;        
    }

}
