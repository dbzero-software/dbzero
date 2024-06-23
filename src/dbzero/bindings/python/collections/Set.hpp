#include <dbzero/object_model/set/Set.hpp>

namespace db0::python 
{
    
    using SetObject = PyWrapper<db0::object_model::Set>;
    
    SetObject *SetObject_new(PyTypeObject *type, PyObject *, PyObject *);
    SetObject *SetDefaultObject_new();
    void SetObject_del(SetObject* self);
    Py_ssize_t SetObject_len(SetObject *);
    PyObject *SetObject_add(SetObject *, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_remove(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_discard(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_isdisjoint(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_issubset(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_issuperset(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    int SetObject_SetItem(SetObject *set_obj, Py_ssize_t i, PyObject *);
    PyObject *SetObject_copy(SetObject *set_obj);
    PyObject *SetObject_union(SetObject *set_obj, PyObject *const *args, Py_ssize_t narg);
    PyObject * SetObject_intersection_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_symmetric_difference_func(SetObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_pop(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);
    PyObject *SetObject_clear(SetObject *set_obj, PyObject *const *args, Py_ssize_t nargs);
    extern PyTypeObject SetObjectType;
    
    // as number
    PyObject * SetObject_intersection_binary(SetObject *self, PyObject * obj);
    PyObject * SetObject_union_binary(SetObject *self, PyObject * obj);
    PyObject * SetObject_difference_binary(SetObject *self, PyObject * obj);
    PyObject * SetObject_symmetric_difference_binary(SetObject *self, PyObject * obj);
    PyObject * SetObject_symmetric_difference_in_place(SetObject *self, PyObject * ob);
    PyObject * SetObject_difference_in_place(SetObject *self, PyObject * ob);
    PyObject * SetObject_update(SetObject *self, PyObject * ob);
    PyObject * SetObject_intersection_in_place(SetObject *self, PyObject * ob);

    SetObject *makeDB0Set(db0::swine_ptr<Fixture> &, PyObject *const *args, Py_ssize_t nargs);
    SetObject *makeSet(PyObject *, PyObject *const *args, Py_ssize_t nargs);

    bool SetObject_Check(PyObject *);
    
}