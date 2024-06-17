#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include "PandasBlock.hpp"

namespace db0::python

{
    ////////// PANDAS BLOCK ////////////

    PyObject *PandasBlockObject_GetItem(PandasBlockObject *block_obj, Py_ssize_t i) {
        return block_obj->ext().getItem(i).steal();
    }

    int PandasBlockObject_SetItem(PandasBlockObject *block_obj, Py_ssize_t i, PyObject *value)
    {
        db0::FixtureLock lock(block_obj->ext().getFixture());
        block_obj->ext().setItem(lock, i, value);
        return 0;
    }

    Py_ssize_t PandasBlockObject_len(PandasBlockObject *block_obj) {
        return block_obj->ext().size();
    }

    PyObject *PandasBlockObject_append(PandasBlockObject *block_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }
        
        db0::FixtureLock lock(block_obj->ext().getFixture());
        block_obj->ext().append(lock, args[0]);
        Py_RETURN_NONE;
    }

    static PySequenceMethods PandasBlockObject_sq = {
        .sq_length = (lenfunc)PandasBlockObject_len,
        .sq_item = (ssizeargfunc)PandasBlockObject_GetItem,
        .sq_ass_item = (ssizeobjargproc)PandasBlockObject_SetItem
    };

    static PyMethodDef PandasBlockObject_methods[] = {
        {"append", (PyCFunction)PandasBlockObject_append, METH_FASTCALL, "Append  element"},
        {"get_storage_class", (PyCFunction)PandasBlockObject_GetStorageClass, METH_FASTCALL, "Append  element"},
        {NULL}
    };

    PyTypeObject PandasBlockObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.block",
        .tp_basicsize = PandasBlockObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PandasBlockObject_del,
        .tp_as_sequence = &PandasBlockObject_sq,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero block object",
        .tp_methods = PandasBlockObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PandasBlockObject_new,
        .tp_free = PyObject_Free,        
    };

    PandasBlockObject *PandasBlockObject_new(PyTypeObject *type, PyObject *args, PyObject *) {
        return reinterpret_cast<PandasBlockObject*>(type->tp_alloc(type, 0));
    }
    
    void PandasBlockObject_del(PandasBlockObject* block_obj)
    {
        // destroy associated DB0 Block instance
        block_obj->ext().~Block();
        Py_TYPE(block_obj)->tp_free((PyObject*)block_obj);
    }

    PandasBlockObject *makeBlock(PyObject *, PyObject *, PyObject *)
    {        
        // make actual DBZero instance, use default fixture
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        auto block_object = PandasBlockObject_new(&PandasBlockObjectType, NULL, NULL);
        db0::object_model::pandas::Block::makeNew(&block_object->ext(), *lock);
        return block_object;
    }

    bool PandasBlockType_Check(PyTypeObject *type) {
        return type->tp_new == reinterpret_cast<newfunc>(PandasBlockObject_new);
    }

    bool PandasBlock_Check(PyObject *obj) {
        return PandasBlockType_Check(Py_TYPE(obj));
    }

    PandasBlockObject *BlockDefaultObject_new() {
        return PandasBlockObject_new(&PandasBlockObjectType, NULL, NULL);
    }

    PyObject * PandasBlockObject_GetStorageClass(PandasBlockObject *block_obj) {
        return block_obj->ext().getStorageClass().steal();
    }

}