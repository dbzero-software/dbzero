#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/GlobalMutex.hpp>
#include "PandasBlock.hpp"


namespace db0::python

{
    ////////// PANDAS BLOCK ////////////

    PyObject *PandasBlockObject_GetItem(PandasBlockObject *block_obj, Py_ssize_t i) {
        std::lock_guard pbm_lock(python_bindings_mutex);
        return block_obj->ext().getItem(i).steal();
    }

    int PandasBlockObject_SetItem(PandasBlockObject *block_obj, Py_ssize_t i, PyObject *value)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        db0::FixtureLock lock(block_obj->ext().getFixture());
        block_obj->modifyExt().setItem(lock, i, value);
        return 0;
    }

    Py_ssize_t PandasBlockObject_len(PandasBlockObject *block_obj) {
        std::lock_guard pbm_lock(python_bindings_mutex);
        return block_obj->ext().size();
    }

    PyObject *PandasBlockObject_append(PandasBlockObject *block_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        std::lock_guard pbm_lock(python_bindings_mutex);
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "append() takes exactly one argument");
            return NULL;
        }
        
        db0::FixtureLock lock(block_obj->ext().getFixture());
        block_obj->modifyExt().append(lock, args[0]);
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

    shared_py_object<PandasBlockObject *> PandasBlockObject_new(PyTypeObject *type, PyObject *args, PyObject *) {
        std::lock_guard pbm_lock(python_bindings_mutex);
        return shared_py_object<PandasBlockObject *>(reinterpret_cast<PandasBlockObject*>(type->tp_alloc(type, 0)), false);
    }
    
    void PandasBlockObject_del(PandasBlockObject* block_obj)
    {
        // destroy associated DB0 Block instance
        block_obj->destroy();
        Py_TYPE(block_obj)->tp_free((PyObject*)block_obj);
    }
    
    shared_py_object<PandasBlockObject*> makeDB0PandasBlock(db0::swine_ptr<Fixture> &fixture, PyObject *const *args, Py_ssize_t nargs)
    {   

        auto block_object = PandasBlockObject_new(&PandasBlockObjectType, NULL, NULL);
        db0::FixtureLock lock(fixture);
        auto &block = block_object.get()->modifyExt();
        db0::object_model::pandas::Block::makeNew(&block, *lock);
        return block_object;
    }
    
    PyObject *makeBlock(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {        
        // make actual DBZero instance, use default fixture
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();

        return makeDB0PandasBlock(fixture, args, nargs).steal();
    }
    
    bool PandasBlockType_Check(PyTypeObject *type) {
        return type->tp_new == reinterpret_cast<newfunc>(PandasBlockObject_new);
    }

    bool PandasBlock_Check(PyObject *obj) {
        return PandasBlockType_Check(Py_TYPE(obj));
    }

    shared_py_object<PandasBlockObject*> BlockDefaultObject_new() {
        return PandasBlockObject_new(&PandasBlockObjectType, NULL, NULL).steal();
    }

    PyObject * PandasBlockObject_GetStorageClass(PandasBlockObject *block_obj) {
        return block_obj->ext().getStorageClass().steal();
    }

}