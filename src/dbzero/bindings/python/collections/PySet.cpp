#include "PySet.hpp"
#include "PyIterator.hpp"
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/set/SetIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PyHash.hpp>


namespace db0::python

{

    using SetIteratorObject = PySharedWrapper<db0::object_model::SetIterator, false>;

    PyTypeObject SetIteratorObjectType = GetIteratorType<SetIteratorObject>("dbzero_ce.SetObjectIterator",
        "dbzero typed query object iterator");

    SetIteratorObject *trySetObject_iter(SetObject *self)
    {        
        return makeIterator<SetIteratorObject,db0::object_model::SetIterator>(
            SetIteratorObjectType, self->ext().begin(), &self->ext(), self
        );
    }

    SetIteratorObject *PyAPI_SetObject_iter(SetObject *self)
    {
        PY_API_FUNC
        return runSafe(trySetObject_iter, self);
    }
    
    int trySetObject_HasItem(SetObject *set_obj, PyObject *key)
    {
        PY_API_FUNC
        auto hash = get_py_hash(key);        
        return set_obj->ext().has_item(hash, key);
    }

    int PyAPI_SetObject_HasItem(SetObject *set_obj, PyObject *key)
    {
        PY_API_FUNC
        return runSafe<-1>(trySetObject_HasItem, set_obj, key);
    }

    static PySequenceMethods SetObject_sq = {
        .sq_length = (lenfunc)PyAPI_SetObject_len,
        .sq_contains = (objobjproc)PyAPI_SetObject_HasItem
    };
    
    static PyMethodDef SetObject_methods[] = 
    {
        {"add", (PyCFunction)PyAPI_SetObject_add, METH_FASTCALL, "Add an item to the set."},
        {"isdisjoint", (PyCFunction)PyAPI_SetObject_isdisjoint, METH_FASTCALL, "Return True if the set has no elements in common with other."},
        {"issubset", (PyCFunction)PyAPI_SetObject_issubset, METH_FASTCALL, "Test whether every element in the set is in other."},
        {"issuperset", (PyCFunction)PyAPI_SetObject_issuperset, METH_FASTCALL, "Test whether every element of other is in set."},
        {"copy", (PyCFunction)PyAPI_SetObject_copy, METH_NOARGS, "Returns copy of set."},
        {"union", (PyCFunction)PyAPI_SetObject_union, METH_FASTCALL, "Returns union of sets"},
        {"intersection", (PyCFunction)PyAPI_SetObject_intersection_func, METH_FASTCALL, "Returns difference of sets"},
        {"difference", (PyCFunction)PyAPI_SetObject_difference_func, METH_FASTCALL, "Returns difference of sets"},
        {"symmetric_difference", (PyCFunction)PyAPI_SetObject_symmetric_difference_func, METH_FASTCALL, "Returns difference of sets"},
        {"remove", (PyCFunction)PyAPI_SetObject_remove, METH_FASTCALL, "Remove an item to the set. Throws when item not found."},
        {"discard", (PyCFunction)PyAPI_SetObject_discard, METH_FASTCALL, "Discar an item to the set."},
        {"pop", (PyCFunction)PyAPI_SetObject_pop, METH_FASTCALL, "Pop an element from set."},
        {"clear", (PyCFunction)PyAPI_SetObject_clear, METH_FASTCALL, "Clear all items from set."},
        {NULL}
    };

    static PyNumberMethods SetObject_as_num = 
    {
        .nb_subtract = (binaryfunc)PyAPI_SetObject_difference_binary,
        .nb_and = (binaryfunc)PyAPI_SetObject_intersection_binary,
        .nb_xor = (binaryfunc)PyAPI_SetObject_symmetric_difference_binary,
        .nb_or = (binaryfunc)PyAPI_SetObject_union_binary,
        .nb_inplace_subtract = (binaryfunc)PyAPI_SetObject_difference_in_place,
        .nb_inplace_and = (binaryfunc)PyAPI_SetObject_intersection_in_place,
        .nb_inplace_xor = (binaryfunc)PyAPI_SetObject_symmetric_difference_in_place,
        .nb_inplace_or = (binaryfunc)PyAPI_SetObject_update,
    };

    Py_ssize_t trySetObject_len(SetObject *set_obj)
    {
        set_obj->ext().getFixture()->refreshIfUpdated();
        return set_obj->ext().size();
    }
    
    Py_ssize_t PyAPI_SetObject_len(SetObject *set_obj)
    {
        PY_API_FUNC
        return runSafe(trySetObject_len, set_obj);
    }

    Py_ssize_t getLenPyObjectOrSet(PyObject *obj)
    {
        if (SetObject_Check(obj)) {
            return trySetObject_len((SetObject*)obj);
        }
        return PyObject_Length(obj);
    }
    
    PyObject *trySetObject_issubsetInternal(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "isdisjoint() takes exactly one argument");
            return NULL;
        }
        
        if (SetObject_Check(args[0])) {
            SetObject *other = (SetObject*)args[0];
            if (trySetObject_len(self) == 0 || trySetObject_len(other) == 0) Py_RETURN_TRUE;

            auto it1 = self->ext().begin();
            auto it2 = other->ext().begin();
            auto it1End = self->ext().end();
            auto it2End = other->ext().end();

            while (it1 != it1End) {
                if (it2 == it2End) {
                    Py_RETURN_FALSE;
                }
                if (*it1 == *it2) {
                    ++it1;
                }
                ++it2;
            }
        } else {
            PyObject *other = args[0];
            if (trySetObject_len(self) == 0 || PyObject_Length(other) == 0) Py_RETURN_TRUE;
            PyObject *iterator = PyObject_GetIter(self);
            if (!iterator) {
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
                return NULL;
            }
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                if (!PySequence_Contains(other, elem)) {                    
                    Py_DECREF(iterator);
                    Py_DECREF(elem);
                    Py_RETURN_FALSE;
                }                
                Py_DECREF(elem);
            }            
            Py_DECREF(iterator);
        }
        Py_RETURN_TRUE;
    }

    PyObject *PyAPI_SetObject_issubset(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        return runSafe(trySetObject_issubsetInternal, self, args, nargs);
    }

    PyObject *trySetObject_issupersetInternal(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "issuperset() takes exactly one argument");
            return NULL;
        }
        if (SetObject_Check(args[0])) {
            SetObject *other = (SetObject*)args[0];
            PyObject *py_self = (PyObject*)self;
            return trySetObject_issubsetInternal(other, &py_self,1);
        } else {
            PyObject *other = args[0];
            if (trySetObject_len(self) == 0 || PyObject_Length(other) == 0) Py_RETURN_TRUE;
            PyObject *iterator = PyObject_GetIter(other);
            if (!iterator) {
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
                return NULL;
            }
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                auto hash = get_py_hash(elem);  
                if (!self->ext().has_item(hash,elem)) {                    
                    Py_DECREF(iterator);
                    Py_DECREF(elem);
                    Py_RETURN_FALSE;
                }                
                Py_DECREF(elem);
            }            
            Py_DECREF(iterator);
        }
        Py_RETURN_TRUE;
    }

    PyObject * PyAPI_SetObject_issuperset(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        return runSafe(trySetObject_issupersetInternal, self, args, nargs);
    }

    PyObject *trySetObject_rq(SetObject *set_obj, PyObject *other, int op)
    {        
        PyObject** args = &other;    
        switch (op) {
            case Py_EQ:
                if (trySetObject_len(set_obj) != getLenPyObjectOrSet(other)) {
                    Py_RETURN_FALSE;
                }
                return PyBool_fromBool(has_all_elements_in_collection(set_obj, other));
            case Py_NE:
                if (trySetObject_len(set_obj) != getLenPyObjectOrSet(other)) {
                    Py_RETURN_TRUE;
                }
                return PyBool_fromBool(!has_all_elements_in_collection(set_obj, other));
            case Py_LE:  // Test whether every element in the set is in other.
                return trySetObject_issubsetInternal(set_obj, args, 1);
            case Py_LT:{  // Test whether the set is a proper subset of other, that is, set <= other and set != other.
                if (trySetObject_len(set_obj) == getLenPyObjectOrSet(other)) {
                    Py_RETURN_FALSE;
                }
                return trySetObject_issubsetInternal(set_obj, args, 1);
            }
            case Py_GE:  // Test whether every element in the set is in other.
                return trySetObject_issupersetInternal(set_obj, args, 1);
            case Py_GT:{  // Test whether the set is a proper superset of other, that is, set >= other and set != other.
                if (trySetObject_len(set_obj) == getLenPyObjectOrSet(other)) {
                    Py_RETURN_FALSE;
                }
                return trySetObject_issupersetInternal(set_obj, args, 1);
            }
            default:
                Py_RETURN_NOTIMPLEMENTED;
        }
        Py_RETURN_NOTIMPLEMENTED;
    }
    
    PyObject *PyAPI_SetObject_rq(SetObject *set_obj, PyObject *other, int op)
    {
        PY_API_FUNC
        return runSafe(trySetObject_rq, set_obj, other, op);
    }

    PyTypeObject SetObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "Set",
        .tp_basicsize = SetObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)SetObject_del,
        .tp_as_number = &SetObject_as_num,
        .tp_as_sequence = &SetObject_sq,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "dbzero set collection object",
        .tp_richcompare = (richcmpfunc)PyAPI_SetObject_rq,
        .tp_iter = (getiterfunc)PyAPI_SetObject_iter,
        .tp_methods = SetObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)SetObject_new,
        .tp_free = PyObject_Free,        
    };
    
    SetObject *SetObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<SetObject*>(type->tp_alloc(type, 0));
    }
    
    shared_py_object<SetObject*> SetDefaultObject_new() {
        return { SetObject_new(&SetObjectType, NULL, NULL), false };
    }
    
    void SetObject_del(SetObject* set_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 Set instance
        set_obj->destroy();
        Py_TYPE(set_obj)->tp_free((PyObject*)set_obj);
    }

    PyObject *trySetObject_add(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        auto hash = get_py_hash(args[0]);
        db0::FixtureLock lock(set_obj->ext().getFixture());
        set_obj->modifyExt().append(lock, hash, args[0]);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_SetObject_add(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "add() takes exactly one argument");
            return NULL;
        }
        return runSafe(trySetObject_add, set_obj, args, nargs);
    }
    
    shared_py_object<SetObject*> tryMake_DB0Set(
        db0::swine_ptr<Fixture> &fixture, PyObject *const *args, Py_ssize_t nargs)
    {
        // make actual dbzero instance, use default fixture
        auto py_set = SetDefaultObject_new();
        db0::FixtureLock lock(fixture);
        auto &set = py_set.get()->modifyExt();
        db0::object_model::Set::makeNew(&set, *lock);
        if (nargs == 1) {
            PyObject *iterator = PyObject_GetIter(args[0]);
            if (!iterator) {                
                return nullptr;
            }
            PyObject *item;
            while ((item = PyIter_Next(iterator))) {
                auto hash = get_py_hash(item);
                set.append(lock, hash, item);                
                Py_DECREF(item);
            }
        }

        // register newly created set with py-object cache
        fixture->getLangCache().add(set.getAddress(), py_set.get());
        return py_set;
    }
    
    SetObject *tryMake_Set(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return tryMake_DB0Set(fixture, args, nargs).steal();
    }
    
    SetObject *PyAPI_makeSet(PyObject *obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(tryMake_Set, obj, args, nargs);
    }
    
    bool SetObject_Check(PyObject *object) {
        return Py_TYPE(object) == &SetObjectType;        
    }

    PyObject *trySetObject_isdisjoint(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (SetObject_Check(args[0])) {
            SetObject *other = (SetObject*)args[0];
            if (trySetObject_len(self) == 0 || trySetObject_len(other) == 0) Py_RETURN_TRUE;
            auto it1 = self->ext().begin();
            auto it2 = other->ext().begin();
            auto it1End = self->ext().end();
            auto it2End = other->ext().end();

            while (it1 != it1End && it2 != it2End) {
                if(*it1 == *it2) Py_RETURN_FALSE;
                if(*it1 < *it2) { ++it1; }
                else { ++it2; }
            }
        } else {
            PyObject *other = args[0];
            if (trySetObject_len(self) == 0 || PyObject_Length(other) == 0) Py_RETURN_TRUE;
            PyObject *iterator = PyObject_GetIter(other);
            if (!iterator) {
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
                return NULL;
            }
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                auto hash = get_py_hash(elem);  
                if (self->ext().has_item(hash, elem)) {
                    Py_DECREF(iterator);
                    Py_DECREF(elem);
                    Py_RETURN_FALSE;
                }
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        Py_RETURN_TRUE;
    }

    PyObject *PyAPI_SetObject_isdisjoint(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "isdisjoint() takes exactly one argument");
            return NULL;
        }

        return runSafe(trySetObject_isdisjoint, self, args, nargs);
    }
    
    PyObject *trySetObject_copyInternal(SetObject *py_src_set)
    {   
        db0::FixtureLock lock(py_src_set->ext().getFixture());        
        auto py_set = SetDefaultObject_new();
        db0::object_model::Set::makeNew(&py_set.get()->modifyExt(), *lock);
        py_set.get()->modifyExt().insert(py_src_set->ext());
        lock->getLangCache().add(py_set.get()->ext().getAddress(), py_set.get());
        return py_set.steal();
    }

    PyObject *PyAPI_SetObject_copy(SetObject *py_src_set)
    {   
        PY_API_FUNC        
        return runSafe(trySetObject_copyInternal, py_src_set);
    }
    
    PyObject *PyAPI_SetObject_union_binary(SetObject *self, PyObject * obj) {        
        return PyAPI_SetObject_union(self, &obj, 1);
    }

    PyObject *trySetObject_union(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    { 
        db0::FixtureLock lock(self->ext().getFixture());
        SetObject *copy = (SetObject *)trySetObject_copyInternal(self);
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            if (SetObject_Check(args[i])) {
                SetObject *other = (SetObject* )args[i];
                copy->modifyExt().insert(other->ext());
            } else {
                PyObject * elem;
                PyObject *iterator = PyObject_GetIter(args[i]);
                if (!iterator) {
                    Py_DECREF(copy);
                    PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
                    return NULL;
                }

                auto &set_impl = copy->modifyExt();
                while ((elem = PyIter_Next(iterator))) {
                    auto hash = get_py_hash(elem);
                    set_impl.append(lock, hash, elem);                    
                    Py_DECREF(elem);
                }                
                Py_DECREF(iterator);
            }
        }
        return copy;
    }

    PyObject *PyAPI_SetObject_union(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    { 
        PY_API_FUNC
        if (nargs == 0) {
            PyErr_SetString(PyExc_TypeError, "union() takes more than 0 arguments");
            return NULL;
        }

        return runSafe(trySetObject_union, self, args, nargs);
    }
    
    void trySetObject_intersectionInternal(FixtureLock &fixture, SetObject * set_obj, PyObject *it1, PyObject *elem1,
        PyObject *it2, PyObject *elem2)
    {
        if (elem1 == nullptr || elem2 == nullptr) {
            return;
        }
        if (elem1 < elem2) {            
            Py_DECREF(elem1);
            elem1 = PyIter_Next(it1);
        } else if (elem1 > elem2) {            
            Py_DECREF(elem2);
            elem2 = PyIter_Next(it2);
        } else if (elem1 == elem2) {
            auto hash = get_py_hash(elem1);
            set_obj->modifyExt().append(fixture, hash, elem1);            
            Py_DECREF(elem1);
            Py_DECREF(elem2);
            elem1 = PyIter_Next(it1);
            elem2 = PyIter_Next(it2);
        }
        return trySetObject_intersectionInternal(fixture, set_obj, it1 ,elem1, it2, elem2);
    }
    
    PyObject *PyAPI_SetObject_intersection_binary(SetObject *self, PyObject * obj) {
        return PyAPI_SetObject_intersection_func(self, &obj, 1);
    }

    PyObject *trySetObject_intersection_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    { 
        SetObject *set_obj = tryMake_Set(nullptr, nullptr, 0);
        PyObject *elem1, *elem2, *it2;
        PyObject *it1 = PyObject_GetIter((PyObject*)self);
        if (!it1) {
            Py_DECREF(set_obj);
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
            return NULL;
        }

        db0::FixtureLock lock(self->ext().getFixture());
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            it2 = PyObject_GetIter(args[i]);
            if (!it2) {
                Py_DECREF(it1);
                Py_DECREF(set_obj);
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
                return NULL;
            }

            elem1 = PyIter_Next(it1);
            elem2 = PyIter_Next(it2);
            set_obj = tryMake_Set(nullptr, nullptr, 0);
            trySetObject_intersectionInternal(lock, set_obj, it1, elem1, it2, elem2);
            Py_DECREF(it1);
            Py_DECREF(it2);
            it1 = PyObject_GetIter((PyObject*)set_obj);
            if (!it1) {
                Py_DECREF(set_obj);
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
                return NULL;
            }
        }
        return set_obj;
    }

    PyObject *PyAPI_SetObject_intersection_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    { 
        PY_API_FUNC
        if (nargs == 0) {
            PyErr_SetString(PyExc_TypeError, "intersection() takes more than 0 arguments");
            return NULL;
        }

        return runSafe(trySetObject_intersection_func, self, args, nargs);
    }

    bool trySetObject_differenceInternal(FixtureLock &fixture, SetObject * set_result, SetObject *set_input,
        PyObject *ob, bool symmetric)
    {
        PyObject *it = PyObject_GetIter((PyObject*)set_input);
        if (!it) {
            return false;
        }
        PyObject *item;
        auto &set_impl = set_result->modifyExt();
        while ((item = PyIter_Next(it))) {
            if (!PySequence_Contains(ob, item)) {
                auto hash = get_py_hash(item);
                set_impl.append(fixture, hash, item);
            }

            Py_DECREF(item);
        }
        if (symmetric) {
            Py_DECREF(it);
            it = PyObject_GetIter(ob);
            if (!it) {
                THROWF(db0::InputException) <<  "argument must be a sequence or set";
            }
            while ((item = PyIter_Next(it))) {
                if (!PySequence_Contains((PyObject*)set_input, item)) {
                    auto hash = get_py_hash(item);
                    set_impl.append(fixture, hash, item);
                }
                Py_DECREF(item);
            }
        }
        Py_DECREF(it);
        return true;
    }

    PyObject *trySetObject_differenceInternal(SetObject *self, PyObject *const *args, Py_ssize_t nargs, bool symmetric)
    {
        if (nargs == 0) {
            PyErr_SetString(PyExc_TypeError, "difference() takes more than 0 arguments");
            return NULL;
        }
        SetObject *set_obj = tryMake_Set(nullptr, nullptr, 0);
        db0::FixtureLock lock(self->ext().getFixture());
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            Py_DECREF(set_obj);
            set_obj = tryMake_Set(nullptr, nullptr, 0);
            if (!trySetObject_differenceInternal(lock, set_obj, self, args[i], symmetric)) {
                Py_DECREF(set_obj);
                PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
                return NULL;
            }
            self = set_obj;
        }
        return set_obj;
    }
    
    PyObject *trySetObject_difference(SetObject *self, PyObject *const *args, Py_ssize_t nargs, bool symmetric) {
        return trySetObject_differenceInternal(self, args, nargs, symmetric);
    }
    
    PyObject *PyAPI_SetObject_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(trySetObject_difference, self, args, nargs, false);
    }

    PyObject *PyAPI_SetObject_symmetric_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "symmetric_difference() takes exacly 1 argument");
            return NULL;
        }
        return runSafe(trySetObject_difference, self, args, nargs, true);
    }
    
    PyObject *PyAPI_SetObject_difference_binary(SetObject *self, PyObject * obj) 
    {
        PY_API_FUNC
        return runSafe(trySetObject_difference, self, &obj, 1, false);
    }

    PyObject *PyAPI_SetObject_symmetric_difference_binary(SetObject *self, PyObject * obj)
    { 
        PY_API_FUNC
        return runSafe(trySetObject_difference, self, &obj, 1, true);
    }

    PyObject *trySetObject_remove(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs, bool throw_ex)
    {        
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "remove() takes exactly one argument");
            return NULL;
        }
        auto hash = get_py_hash(args[0]);
        db0::FixtureLock lock(set_obj->ext().getFixture());
        if (set_obj->modifyExt().remove(lock, hash, args[0]) == false && throw_ex) {
            PyErr_SetString(PyExc_KeyError, "Element not found");
            return NULL;
        }
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_SetObject_remove(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(trySetObject_remove, set_obj, args, nargs, true);
    }

    PyObject *PyAPI_SetObject_discard(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs) 
    {
        PY_API_FUNC
        return runSafe(trySetObject_remove, set_obj, args, nargs, false);
    }

    PyObject *trySetObject_pop(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        db0::FixtureLock lock(set_obj->ext().getFixture());
        auto obj = set_obj->modifyExt().pop(lock);
        if (obj == nullptr) {
            PyErr_SetString(PyExc_KeyError, "Cannot pop from empty set");
            return NULL;
        }
        return obj.steal();
    }

    PyObject *PyAPI_SetObject_pop(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        return runSafe(trySetObject_pop, set_obj, args, nargs);
    }

    PyObject *trySetObject_clear(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        db0::FixtureLock lock(set_obj->ext().getFixture());
        set_obj->modifyExt().clear(lock);
        Py_RETURN_NONE;
    }

    PyObject *PyAPI_SetObject_clear(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC        
        return runSafe(trySetObject_clear, set_obj, args, nargs);
    }

    PyObject *trySetObject_update(SetObject *self, PyObject * ob)
    {        
        PyObject* it = PyObject_GetIter(ob);
        if (!it) {
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
            return NULL;
        }

        PyObject* item;
        auto &set_impl = self->modifyExt();
        db0::FixtureLock lock(set_impl.getFixture());
        while ((item = PyIter_Next(it))) {
            auto hash = get_py_hash(item);
            set_impl.append(lock, hash, item);            
            Py_DECREF(item);
        }        
        Py_DECREF(it);
        Py_INCREF(self);
        return self;
    }

    PyObject *PyAPI_SetObject_update(SetObject *self, PyObject * ob)
    {
        PY_API_FUNC
        return runSafe(trySetObject_update, self, ob);
    }

    bool sequenceContainsItem(PyObject *set_obj, PyObject *item)
    {
        if (SetObject_Check(set_obj)) {
            auto hash = get_py_hash(item);  
            return ((SetObject*)set_obj)->ext().has_item(hash, item);
        } else {
            return PySequence_Contains(set_obj, item);
        }
    }

    PyObject *trySetObject_intersection_in_place(SetObject *self, PyObject * ob)
    {        
        PyObject* it = PyObject_GetIter(self);
        if (!it) {
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
            return NULL;
        }
        PyObject* item;
        std::list<std::pair<size_t, PyObject*>> hashes_and_items;
        while ((item = PyIter_Next(it))) {
            if (!sequenceContainsItem(ob, item)){
                auto hash = get_py_hash(item);
                hashes_and_items.push_back({hash,item});
            }            
            Py_DECREF(item);
        }
        auto &set_impl = self->modifyExt();
        db0::FixtureLock lock(set_impl.getFixture());
        for (auto hash_and_item: hashes_and_items) {
            set_impl.remove(lock, hash_and_item.first, hash_and_item.second);
        }
        Py_INCREF(self);        
        Py_DECREF(it);        
        return self;
    }

    PyObject *PyAPI_SetObject_intersection_in_place(SetObject *self, PyObject * ob)
    {
        PY_API_FUNC
        return runSafe(trySetObject_intersection_in_place, self, ob);
    }

    PyObject *trySetObject_difference_in_place(SetObject *self, PyObject * ob)
    {        
        PyObject* it = PyObject_GetIter(ob);
        if (!it) {
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
            return NULL;
        }
        PyObject* item;
        std::list<size_t> hashes;
        auto &set_impl = self->modifyExt();
        db0::FixtureLock lock(set_impl.getFixture());
        while ((item = PyIter_Next(it))) {
            auto hash = get_py_hash(item);
            set_impl.remove(lock, hash, item);        
            Py_DECREF(item);
        }
        Py_INCREF(self);        
        Py_DECREF(it);        
        return self;
    }

    PyObject *PyAPI_SetObject_difference_in_place(SetObject *self, PyObject * ob)
    {
        PY_API_FUNC
        return runSafe(trySetObject_difference_in_place, self, ob);
    }

    PyObject *trySetObject_symmetric_difference_in_place(SetObject *self, PyObject * ob)
    {        
        PyObject* it = PyObject_GetIter(ob);
        if (!it) {
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
            return NULL;
        }
        std::list<std::pair<size_t, PyObject*>> hashes_and_items_to_remove;
        std::list<PyObject *> items_to_add;

        PyObject* item;
        auto &set_impl = self->modifyExt();
        db0::FixtureLock lock(set_impl.getFixture());
        while ((item = PyIter_Next(it))) {
            auto hash = get_py_hash(item);  
            if (set_impl.has_item(hash, item)) {
                auto hash = get_py_hash(item);
                hashes_and_items_to_remove.push_back({hash, item});
            } else {
                items_to_add.push_back(item);
            }
        }
        for (auto hash_and_item: hashes_and_items_to_remove) {
            set_impl.remove(lock, hash_and_item.first, hash_and_item.second);
        }
        for (auto item: items_to_add) {
            auto hash = get_py_hash(item);
            set_impl.append(lock, hash, item);            
            Py_DECREF(item);
        }
        Py_INCREF(self);        
        Py_DECREF(it);        
        return self;
    }

    PyObject *PyAPI_SetObject_symmetric_difference_in_place(SetObject *self, PyObject * ob)
    {
        PY_API_FUNC
        return runSafe(trySetObject_symmetric_difference_in_place, self, ob);
    }
    
    PyObject *tryLoadSet(PyObject *set, PyObject *kwargs, std::unordered_set<const void*> *load_stack_ptr)
    {    
        PyObject *iterator = PyObject_GetIter(set);
        if (!iterator) {
            PyErr_SetString(PyExc_TypeError, "argument must be a sequence or set");
            return NULL;
        }
        
        PyObject *elem;
        PyObject *py_result = PySet_New(nullptr);
        while ((elem = PyIter_Next(iterator))) {
            auto result = tryLoad(elem, kwargs, nullptr, load_stack_ptr);
            if (result == nullptr) {
                Py_DECREF(iterator);
                Py_DECREF(elem);
                Py_DECREF(py_result);
                return nullptr;
            }
            PySet_Add(py_result, result);
            Py_DECREF(elem);
        }
        Py_DECREF(iterator);
        return py_result;
    }
    
}