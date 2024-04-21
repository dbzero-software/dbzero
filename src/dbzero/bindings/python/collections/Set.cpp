#include "Set.hpp"
#include "Iterator.hpp"
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/set/SetIterator.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>


namespace db0::python

{

    using SetIteratorObject = PyWrapper<db0::object_model::SetIterator>;

    PyTypeObject SetIteratorObjectType = GetIteratorType<SetIteratorObject>("dbzero_ce.TypedObjectIterator",
                                                                              "DBZero typed query object iterator");

    SetIteratorObject *SetObject_iter(SetObject *self)
    {
        return makeIterator<SetIteratorObject,db0::object_model::SetIterator>(SetIteratorObjectType, 
            self->ext().begin(), &self->ext());        
    }
    
    PyObject *SetObject_GetItem(SetObject *set_obj, Py_ssize_t i)
    {
        set_obj->ext().getFixture()->refreshIfUpdated();
        return set_obj->ext().getItem(i).steal();
    }
    
    int SetObject_SetItem(SetObject *set_obj, Py_ssize_t i, PyObject *value)
    {
        auto fixture = set_obj->ext().getMutableFixture();
        set_obj->ext().setItem(fixture, i, value);
        return 0;
    }

     int SetObject_HasItem(SetObject *set_obj, PyObject *key)
    {
        return set_obj->ext().has_item(key);
    }

    static PySequenceMethods SetObject_sq = {
        .sq_length = (lenfunc)SetObject_len,
        .sq_item = (ssizeargfunc)SetObject_GetItem,
        .sq_ass_item = (ssizeobjargproc)SetObject_SetItem,
        .sq_contains = (objobjproc)SetObject_HasItem
    };
    
    static PyMethodDef SetObject_methods[] = {
        {"add", (PyCFunction)SetObject_add, METH_FASTCALL, "Add an item to the set."},
        {"isdisjoint", (PyCFunction)SetObject_isdisjoint, METH_FASTCALL, "Return True if the set has no elements in common with other."},
        {"issubset", (PyCFunction)SetObject_issubset, METH_FASTCALL, "Test whether every element in the set is in other."},
        {"issuperset", (PyCFunction)SetObject_issuperset, METH_FASTCALL, "Test whether every element of other is in set."},
        {"copy", (PyCFunction)SetObject_copy, METH_NOARGS, "Returns copy of set."},
        {"union", (PyCFunction)SetObject_union, METH_FASTCALL, "Returns union of sets"},
        {"intersection", (PyCFunction)SetObject_intersection_func, METH_FASTCALL, "Returns difference of sets"},
        {"difference", (PyCFunction)SetObject_difference_func, METH_FASTCALL, "Returns difference of sets"},
        {"symmetric_difference", (PyCFunction)SetObject_symmetric_difference_func, METH_FASTCALL, "Returns difference of sets"},
        {"remove", (PyCFunction)SetObject_remove, METH_FASTCALL, "Remove an item to the set. Throws when item not found."},
        {"discard", (PyCFunction)SetObject_discard, METH_FASTCALL, "Discar an item to the set."},
        {"pop", (PyCFunction)SetObject_pop, METH_FASTCALL, "Pop an element from set."},
        {"clear", (PyCFunction)SetObject_clear, METH_FASTCALL, "Clear all items from set."},
        {NULL}
    };

    static PyNumberMethods SetObject_as_num = {
        .nb_subtract = (binaryfunc)SetObject_difference_binary,
        .nb_and = (binaryfunc)SetObject_intersection_binary,
        .nb_xor = (binaryfunc)SetObject_symmetric_difference_binary,
        .nb_or = (binaryfunc)SetObject_union_binary,
        .nb_inplace_subtract = (binaryfunc)SetObject_difference_in_place,
        .nb_inplace_and = (binaryfunc)SetObject_intersection_in_place,
        .nb_inplace_xor = (binaryfunc)SetObject_symmetric_difference_in_place,
        .nb_inplace_or = (binaryfunc)SetObject_update,
    };

    static PyObject *SetObject_rq(SetObject *set_obj, PyObject *other, int op) 
    {        
        PyObject** args = &other;    
        switch (op)
        {
        case Py_EQ:
            if(SetObject_len(set_obj) != PyObject_Length(other)) {
                return Py_False;
            }
            return PyBool_fromBool(has_all_elements_in_collection(set_obj, other));
        case Py_NE:
            if(SetObject_len(set_obj) != PyObject_Length(other)) {
                return Py_True;
            }
            return PyBool_fromBool(!has_all_elements_in_collection(set_obj, other));
        case Py_LE:  // Test whether every element in the set is in other.
            return SetObject_issubset(set_obj, args, 1);
        case Py_LT:{  // Test whether the set is a proper subset of other, that is, set <= other and set != other.
            if(SetObject_len(set_obj) == PyObject_Length(other)){
                return Py_False;
            }
            return SetObject_issubset(set_obj, args, 1);
        }
        case Py_GE:  // Test whether every element in the set is in other.
            return SetObject_issuperset(set_obj, args, 1);
        case Py_GT:{  // Test whether the set is a proper superset of other, that is, set >= other and set != other.
            if(SetObject_len(set_obj) == PyObject_Length(other)){
                return Py_False;
            }
            return SetObject_issuperset(set_obj, args, 1);
        }
        default:
            return Py_NotImplemented;
        }
        return Py_NotImplemented;
    }

    PyTypeObject SetObjectType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Set",
        .tp_basicsize = SetObject::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)SetObject_del,
        .tp_as_number = &SetObject_as_num,
        .tp_as_sequence = &SetObject_sq,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "DBZero set collection object",
        .tp_richcompare = (richcmpfunc)SetObject_rq,
        .tp_iter = (getiterfunc)SetObject_iter,
        .tp_methods = SetObject_methods,        
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)SetObject_new,
        .tp_free = PyObject_Free,        
    };

    SetObject *SetObject_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<SetObject*>(type->tp_alloc(type, 0));
    }

    SetObject *SetDefaultObject_new() {
        return SetObject_new(&SetObjectType, NULL, NULL);
    }
    
    void SetObject_del(SetObject* set_obj)
    {
        // destroy associated DB0 Set instance
        set_obj->ext().~Set();
        Py_TYPE(set_obj)->tp_free((PyObject*)set_obj);
    }

    Py_ssize_t SetObject_len(SetObject *set_obj)
    {
        set_obj->ext().getFixture()->refreshIfUpdated();
        return set_obj->ext().size();
    }
    
    PyObject *SetObject_add(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "add() takes exactly one argument");
            return NULL;
        }
        auto hash = PyObject_Hash(args[0]);
        auto fixture = set_obj->ext().getMutableFixture();
        set_obj->ext().append(fixture, hash, args[0]);
        Py_RETURN_NONE;
    }
    
    SetObject *makeSet(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        // make actual DBZero instance, use default fixture
        auto set_object = SetObject_new(&SetObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::object_model::Set::makeNew(&set_object->ext(), *fixture);
        if (nargs == 1) {
            PyObject *iterator = PyObject_GetIter(args[0]);
            PyObject *item;
            while ((item = PyIter_Next(iterator))) {
                auto hash = PyObject_Hash(item);
                set_object->ext().append(fixture, hash, item);
                Py_DECREF(item);
            }
        }
        // register newly created set with py-object cache
        (*fixture)->getLangCache().add(set_object->ext().getAddress(), set_object, true);
        return set_object;
    }
    
    bool SetObject_Check(PyObject *object) {
        return Py_TYPE(object) == &SetObjectType;        
    }

    PyObject * SetObject_isdisjoint(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "isdisjoint() takes exactly one argument");
            return NULL;
        }

        if (SetObject_Check(args[0])) {
            SetObject *other = (SetObject*)args[0];
            if(SetObject_len(self) == 0 || SetObject_len(other) == 0) return Py_True;
            auto it1 = self->ext().begin();
            auto it2 = other->ext().begin();
            auto it1End = self->ext().end();
            auto it2End = other->ext().end();

            while(it1 != it1End && it2 != it2End)
            {
                if(*it1 == *it2) return Py_False;
                if(*it1 < *it2) { ++it1; }
                else { ++it2; }
            }
        } else {
            PyObject *other = args[0];
            if(SetObject_len(self) == 0 || PyObject_Length(other) == 0) return Py_True;
            PyObject *iterator = PyObject_GetIter(other);
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                if(self->ext().has_item(elem)){
                    Py_DECREF(iterator);
                    Py_DECREF(elem);
                    return Py_False;
                }
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        return Py_True;
    }

    PyObject *SetObject_issubset(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "isdisjoint() takes exactly one argument");
            return NULL;
        }
        
        if (SetObject_Check(args[0])) {
            SetObject *other = (SetObject*)args[0];
            if(SetObject_len(self) == 0 || SetObject_len(other) == 0) return Py_True;

            auto it1 = self->ext().begin();
            auto it2 = other->ext().begin();
            auto it1End = self->ext().end();
            auto it2End = other->ext().end();

            while(it1 != it1End)
            {
                if(it2 == it2End) {
                    return Py_False;
                }
                if(*it1 == *it2){
                    ++it1;
                }
                ++it2;
            }
        } else {
            PyObject *other = args[0];
            if(SetObject_len(self) == 0 || PyObject_Length(other) == 0) return Py_True;
            PyObject *iterator = PyObject_GetIter(self);
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                if(!PySequence_Contains(other, elem)){
                    Py_DECREF(iterator);
                    Py_DECREF(elem);
                    return Py_False;
                }
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        return Py_True;
    }

    PyObject * SetObject_issuperset(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "issuperset() takes exactly one argument");
            return NULL;
        }
        if(SetObject_Check(args[0])) {

            SetObject *other = (SetObject*)args[0];
            PyObject *py_self = (PyObject*)self;
            return SetObject_issubset(other, &py_self,1);
        } else {
            PyObject *other = args[0];
            if(SetObject_len(self) == 0 || PyObject_Length(other) == 0) return Py_True;
            PyObject *iterator = PyObject_GetIter(other);
            PyObject *elem;
            while ((elem = PyIter_Next(iterator))) {
                if(!self->ext().has_item(elem)){
                    Py_DECREF(iterator);
                    Py_DECREF(elem);
                    return Py_False;
                }
                Py_DECREF(elem);
            }
            Py_DECREF(iterator);
        }
        return Py_True;
    }

    PyObject *SetObject_copy(SetObject *set_obj)
    {
        // make actual DBZero instance, use default fixture
        auto set_object = SetObject_new(&SetObjectType, NULL, NULL);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        set_obj->ext().copy(&set_object->ext(), *fixture);
        (*fixture)->getLangCache().add(set_object->ext().getAddress(), set_object, true);
        return set_object;
    }

    PyObject * SetObject_union_binary(SetObject *self, PyObject * obj) {        
        return SetObject_union(self, &obj, 1);
    }

    PyObject * SetObject_union(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {        
        if (nargs == 0) {
            PyErr_SetString(PyExc_TypeError, "union() takes more than 0 arguments");
            return NULL;
        }
        auto fixture = self->ext().getMutableFixture();
        SetObject *copy = (SetObject* )SetObject_copy(self);
        for(Py_ssize_t i =0; i < nargs; ++i) {
            if(SetObject_Check(args[i])){
                SetObject *other = (SetObject* )args[i];
                copy->ext().bulkInsertUnique(other->ext().begin(), other->ext().end());
            } else {
                PyObject * elem;
                PyObject *iterator = PyObject_GetIter(args[i]);
                while ((elem = PyIter_Next(iterator))) {
                    auto hash = PyObject_Hash(elem);
                    copy->ext().append(fixture, hash, elem);
                    Py_DECREF(elem);
                }
                Py_DECREF(iterator);
            }
        }
        return copy;
    }

    void SetObject_intersection(FixtureLock &fixture, SetObject * set_obj, PyObject *it1, PyObject *elem1, 
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
            auto hash = PyObject_Hash(elem1);
            set_obj->ext().append(fixture, hash, elem1);
            Py_DECREF(elem1);
            Py_DECREF(elem2);
            elem1 = PyIter_Next(it1);
            elem2 = PyIter_Next(it2);
        }
        return SetObject_intersection(fixture, set_obj, it1 ,elem1, it2, elem2);
    }
    
    PyObject *SetObject_intersection_binary(SetObject *self, PyObject * obj) {        
        return SetObject_intersection_func(self, &obj, 1);
    }

    PyObject *SetObject_intersection_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs)
    {        
        if (nargs == 0) {
            PyErr_SetString(PyExc_TypeError, "intersection() takes more than 0 arguments");
            return NULL;
        }
        
        SetObject *set_obj = makeSet(nullptr, nullptr, 0);
        PyObject *elem1, *elem2, *it2;
        PyObject *it1 = PyObject_GetIter((PyObject*)self);
        auto fixture = self->ext().getMutableFixture();
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            it2 = PyObject_GetIter(args[i]);
            elem1 = PyIter_Next(it1);
            elem2 = PyIter_Next(it2);
            set_obj = makeSet(nullptr, nullptr, 0);
            SetObject_intersection(fixture, set_obj, it1, elem1, it2, elem2);
            Py_DECREF(it1);
            Py_DECREF(it2);
            it1 = PyObject_GetIter((PyObject*)set_obj);        
        }
        return set_obj;
    }

    void SetObject_difference(FixtureLock &fixture, SetObject * set_obj, PyObject *it1, PyObject *elem1,
        PyObject *it2, PyObject *elem2, bool symmetric)
    {
        if (elem1 == nullptr && elem2 == nullptr) {
            return;
        }
        if (elem1 == nullptr) {
            do {
                if(symmetric) {
                    auto hash = PyObject_Hash(elem2);
                    set_obj->ext().append(fixture, hash, elem2);
                }
                Py_DECREF(elem2);
            } while((elem2 = PyIter_Next(it2)));
            return;
        }
        if(elem2 == nullptr){
            do {
                auto hash = PyObject_Hash(elem1);
                set_obj->ext().append(fixture, hash, elem1);
                Py_DECREF(elem1);
            } while((elem1 = PyIter_Next(it1)));
            return;
        }
        if (elem1 < elem2) {
            auto hash = PyObject_Hash(elem1);
            set_obj->ext().append(fixture, hash, elem1);
            Py_DECREF(elem1);
            elem1 = PyIter_Next(it1);
        } else if (elem1 > elem2) {
            if (symmetric) {
                auto hash = PyObject_Hash(elem2);
                set_obj->ext().append(fixture, hash, elem1);
            }
            Py_DECREF(elem2);
            elem2 = PyIter_Next(it2);
        } else if (elem1 == elem2) {
            Py_DECREF(elem1);
            Py_DECREF(elem2);
            elem1 = PyIter_Next(it1);
            elem2 = PyIter_Next(it2);
        }
        return SetObject_difference(fixture, set_obj, it1 ,elem1, it2, elem2, symmetric);
    }

    PyObject * SetObject_difference(SetObject *self, PyObject *const *args, Py_ssize_t nargs, bool symmetric)
    {
        
        if (nargs == 0) {
            PyErr_SetString(PyExc_TypeError, "difference() takes more than 0 arguments");
            return NULL;
        }
        SetObject *set_obj = makeSet(nullptr, nullptr, 0);
        PyObject *elem1, *elem2, *it2;
        PyObject *it1 = PyObject_GetIter((PyObject*)self);
        auto fixture = self->ext().getMutableFixture();
        for(Py_ssize_t i = 0; i < nargs; ++i) {
            it2 = PyObject_GetIter(args[i]);
            elem1 = PyIter_Next(it1);
            elem2 = PyIter_Next(it2);
            set_obj = makeSet(nullptr, nullptr, 0);
            SetObject_difference(fixture, set_obj, it1, elem1, it2, elem2, symmetric);
            Py_DECREF(it1);
            Py_DECREF(it2);
            it1 = PyObject_GetIter((PyObject*)set_obj);        
        }
        return set_obj;
    }

    PyObject *SetObject_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs) {
        return SetObject_difference(self, args, nargs, false);
    }

    PyObject *SetObject_symmetric_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs) {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "symmetric_difference() takes exacly 1 argument");
            return NULL;
        }
        return SetObject_difference(self, args, nargs, true);
    }
    
    PyObject *SetObject_difference_binary(SetObject *self, PyObject * obj) {        
        return SetObject_difference(self, &obj, 1, false);
    }

    PyObject *SetObject_symmetric_difference_binary(SetObject *self, PyObject * obj) {        
        return SetObject_difference(self, &obj, 1, true);
    }

    PyObject *SetObject_remove(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs, bool throw_ex)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "remove() takes exactly one argument");
            return NULL;
        }
        auto hash = PyObject_Hash(args[0]);
        auto fixture = set_obj->ext().getMutableFixture();
        if (set_obj->ext().remove(fixture, hash) == false && throw_ex) {
            PyErr_SetString(PyExc_KeyError, "Element not found");
            return NULL;
        }
        Py_RETURN_NONE;
    }

    PyObject *SetObject_remove(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs) {
        return SetObject_remove(set_obj, args, nargs, true);
    }

    PyObject *SetObject_discard(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs) {
        return SetObject_remove(set_obj, args, nargs, false);
    }

    PyObject *SetObject_pop(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        auto obj = set_obj->ext().pop();
        if(obj == nullptr){
            PyErr_SetString(PyExc_KeyError, "Cannot pop from empty set");
            return NULL;
        }
        return obj.steal();
    }

    PyObject *SetObject_clear(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs)
    {
        set_obj->ext().clear();
        Py_RETURN_NONE;
    }

    PyObject *SetObject_update(SetObject *self, PyObject * ob)
    {
        PyObject* it = PyObject_GetIter(ob);
        PyObject* item;
        auto &set_impl = self->ext();
        auto fixture = set_impl.getMutableFixture();        
        while ((item = PyIter_Next(it))) {
            auto hash = PyObject_Hash(item);
            set_impl.append(fixture, hash, item);
            Py_DECREF(item);
        }
        Py_DECREF(it);
        Py_INCREF(self);
        return self;
    }

    PyObject *SetObject_intersection_in_place(SetObject *self, PyObject * ob)
    {
        PyObject* it = PyObject_GetIter(self);
        PyObject* item;
        std::list<size_t> hashes;
        while ((item = PyIter_Next(it))) {
            if(!PySequence_Contains(ob, item)){
                auto hash = PyObject_Hash(item);
                hashes.push_back(hash);
            }
            Py_DECREF(item);
        }
        auto &set_impl = self->ext();
        auto fixture = set_impl.getMutableFixture();
        for (auto hash: hashes) {
            set_impl.remove(fixture, hash);
        }
        Py_DECREF(it);
        Py_INCREF(self);
        return self;
    }

    PyObject *SetObject_difference_in_place(SetObject *self, PyObject * ob)
    {
        PyObject* it = PyObject_GetIter(ob);
        PyObject* item;
        std::list<size_t> hashes;
        auto &set_impl = self->ext();
        auto fixture = set_impl.getMutableFixture();
        while ((item = PyIter_Next(it))) {
            auto hash = PyObject_Hash(item);
            set_impl.remove(fixture, hash);
            Py_DECREF(item);
        }
        Py_DECREF(it);
        Py_INCREF(self);
        return self;
    }

    PyObject *SetObject_symmetric_difference_in_place(SetObject *self, PyObject * ob)
    {
        std::list<size_t> hashes_to_remove;
        std::list<PyObject *> items_to_add;
        PyObject* it = PyObject_GetIter(ob);
        PyObject* item;
        auto &set_impl = self->ext();
        auto fixture = set_impl.getMutableFixture();
        while ((item = PyIter_Next(it))) {
            if (set_impl.has_item(item)) {
                auto hash = PyObject_Hash(item);
                hashes_to_remove.push_back(hash);
            } else {
                items_to_add.push_back(item);
            }
        }
        for (auto hash: hashes_to_remove) {
            set_impl.remove(fixture, hash);
        }
        for (auto item: items_to_add) {
            auto hash = PyObject_Hash(item);
            set_impl.append(fixture, hash, item);
            Py_DECREF(item);
        }
        Py_DECREF(it);
        Py_INCREF(self);
        return self;
    }

}