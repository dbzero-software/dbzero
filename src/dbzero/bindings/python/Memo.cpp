#include "Memo.hpp"
#include "PyToolkit.hpp"
#include <iostream>
#include <object.h>
#include <dbzero/object_model/object.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/to_string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"
#include "PyClassFields.hpp"
#include "Utils.hpp"
#include "Types.hpp"

namespace db0::python

{
    
    MemoTypeDecoration::MemoTypeDecoration(const char *prefix_name, const char *type_id)
        : m_prefix_name_ptr(prefix_name)
        , m_type_id(type_id)
    {
    }

    std::uint64_t MemoTypeDecoration::getFixtureUUID()
    {
        if (m_prefix_name_ptr && !m_fixture_uuid) {
            // initialize fixture by prefix name and keep UUID for future use
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(m_prefix_name_ptr);
            m_fixture_uuid = fixture->getUUID();
        }
        return m_fixture_uuid;
    }

    void MemoTypeDecoration::close() {
        m_fixture_uuid = 0;
    }
    
    MemoObject *tryMemoObject_new(PyTypeObject *py_type, PyObject *, PyObject *)
    {
        MemoTypeDecoration &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture(decor.getFixtureUUID());
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find py type associated DBZero class with the ClassFactory
        auto type = class_factory.getOrCreateType(py_type);
        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        // prepare a new DB0 instance of a known DB0 class
        db0::object_model::Object::makeNew(&memo_obj->ext(), type);
        return memo_obj;
    }
    
    MemoObject *MemoObject_new(PyTypeObject *py_type, PyObject *args, PyObject *kwargs) {
        return reinterpret_cast<MemoObject*>(runSafe(tryMemoObject_new, py_type, args, kwargs));
    }
    
    PyObject *tryMemoObject_new_singleton(PyTypeObject *py_type, PyObject *, PyObject *)
    {
        MemoTypeDecoration &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture(decor.getFixtureUUID());
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find py type associated DBZero class with the ClassFactory
        auto type = class_factory.getOrCreateType(py_type);
        
        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        // try unloading associated singleton if such exists
        if (!type->unloadSingleton(&memo_obj->ext())) {
            // create singleton instance
            db0::object_model::Object::makeNew(&memo_obj->ext(), type);
        }
        return memo_obj;
    }
    
    PyObject *MemoObject_new_singleton(PyTypeObject *py_type, PyObject *args, PyObject *kwargs) {
        return runSafe(tryMemoObject_new_singleton, py_type, args, kwargs);
    }
    
    MemoObject *MemoObjectStub_new(PyTypeObject *py_type) {
        return reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
    }

    PyObject *MemoObject_alloc(PyTypeObject *self, Py_ssize_t nitems) {
        return PyType_GenericAlloc(self, nitems);
    }
    
    void MemoObject_del(MemoObject* memo_obj)
    {
        // destroy associated DB0 Object instance        
        memo_obj->destroy();
        Py_TYPE(memo_obj)->tp_free((PyObject*)memo_obj);
    }
    
    int MemoObject_init(MemoObject* self, PyObject* args, PyObject* kwds)
    {
        // the instance may already exist (e.g. if this is a singleton)
        if (!self->ext().hasInstance()) {
            auto py_type = Py_TYPE(self);
            auto base_type = py_type->tp_base;

            // invoke tp_init from base type (wrapped pyhon class)
            if (base_type->tp_init((PyObject*)self, args, kwds) < 0) {
                return -1;
            }
            
            // invoke post-init on associated DBZero object
            auto &object = self->ext();
            db0::FixtureLock fixture(object.getFixture());
            fixture->onUpdated();
            object.postInit(fixture);
            // register weak-ref with the lang cache
            fixture->getLangCache().add(object.getAddress(), self, false);
        }

        return 0;
    }
    
    void MemoObject_drop(MemoObject* memo_obj)
    {
        // since objects are destroyed by GC0 drop is only responsible for marking
        // singletons as unreferenced
        if (memo_obj->ext().isSingleton()) {
            // the acutal destroy will be performed by the GC0
            memo_obj->ext().unSingleton();
            return;
        }

        if (!memo_obj->ext().hasInstance()) {
            return;
        }
        
        if (memo_obj->ext().hasRefs()) {
            PyErr_SetString(PyExc_RuntimeError, "Object has references");
            return;    
        }
        
        // create a null placeholder in place of the original instance to mark as deleted
        memo_obj->ext().~Object();
        db0::object_model::Object::makeNull(&memo_obj->ext());
    }
    
    PyObject *tryMemoObject_getattro(MemoObject *self, PyObject *attr)
    {
        // The method resolution order for Memo types is following:
        // 1. User type members (class members such as methods)
        // 2. DB0 object extension methods
        // 3. DB0 object members (attributes)
        // 4. User instance members (e.g. attributes set during __postinit__)

        auto res = _PyObject_GetDescrOptional(reinterpret_cast<PyObject*>(self), attr);
        if (res) {
            return res;
        }

        self->ext().getFixture()->refreshIfUpdated();
        return self->ext().get(PyUnicode_AsUTF8(attr)).steal();
    }
    
    PyObject *MemoObject_getattro(MemoObject *self, PyObject *attr) {
        return runSafe(tryMemoObject_getattro, self, attr);
    }
    
    int MemoObject_setattro(MemoObject *self, PyObject *attr, PyObject *value)
    {
        // assign value to a DB0 attribute
        Py_XINCREF(value);
        try {
            // must materialize the object before setting as an attribute
            if (!db0::object_model::isMaterialized(value)) {
                auto fixture = self->ext().getMutableFixture();
                db0::object_model::materialize(fixture, value);
            }

            auto &memo = self->ext();
            if (memo.hasInstance()) {
                auto fixture = self->ext().getMutableFixture();
                memo.set(fixture, PyUnicode_AsUTF8(attr), value);
            } else {
                memo.setPreInit(PyUnicode_AsUTF8(attr), value);
            }
        } catch (const std::exception &e) {
            Py_XDECREF(value);
            PyErr_SetString(PyExc_AttributeError, e.what());
            return -1;
        }
        Py_XDECREF(value);
        return 0;
    }
    
    bool isEqual(MemoObject *lhs, MemoObject *rhs) {
        return lhs->ext() == rhs->ext();
    }
    
    PyObject *MemoObject_rq(MemoObject *memo_obj, PyObject *other, int op)
    {
        bool eq_result = false;
        if (PyMemo_Check(other)) {
            eq_result = isEqual(memo_obj, reinterpret_cast<MemoObject*>(other));
        }

        switch (op)
        {
            case Py_EQ:
                return PyBool_fromBool(eq_result);
            case Py_NE:
                return PyBool_fromBool(!eq_result);
            default:
                return Py_NotImplemented;
        }
    }

    PyTypeObject *castToType(PyObject *obj)
    {
        if (!PyType_Check(obj)) {
            PyErr_SetString(PyExc_TypeError, "Argument must be a type");
            return NULL;
        }
        return reinterpret_cast<PyTypeObject *>(obj);
    }
    
    std::pair<const char*, const char*> createWrappedTypeName(const char *py_type_name) 
    {
        const char *type_name = nullptr, *full_type_name = nullptr;
        {
            std::stringstream str;
            str << "Memo_" << py_type_name;            
            type_name = PyToolkit::getTypeManager().getPooledString(str.str());
        }

        {
            std::stringstream str;
            str << "dbzero_ce." << type_name;
            full_type_name = PyToolkit::getTypeManager().getPooledString(str.str());
        }

        return { type_name, full_type_name };
    }

    PyObject *findModule(PyObject *module_name)
    {
        PyObject *sys_module = PyImport_ImportModule("sys");
        PyObject *modules_dict = PyObject_GetAttrString(sys_module, "modules");
        auto result = PyDict_GetItem(modules_dict, module_name);

        Py_DECREF(sys_module);
        Py_DECREF(modules_dict);
        return result;
    }
    
    // Copy a python dict
    PyObject *copyDict(PyObject *dict)
    {
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        PyObject *new_dict = PyDict_New();

        while (PyDict_Next(dict, &pos, &key, &value)) {
            PyDict_SetItem(new_dict, key, value);
        }

        return new_dict;
    }
    
    PyTypeObject *wrapPyType(PyTypeObject *py_class, bool is_singleton, const char *prefix_name, const char *type_id)
    {        
        Py_INCREF(py_class);
        PyObject *py_module = findModule(PyObject_GetAttrString((PyObject*)py_class, "__module__"));
        if (!PyModule_Check(py_module)) {
            PyErr_SetString(PyExc_TypeError, "Not a module");
            return NULL;
        }
        
        if (py_class->tp_dict == nullptr) {
            PyErr_SetString(PyExc_TypeError, "Type has no tp_dict");
            return NULL;
        }
        
        if (py_class->tp_itemsize != 0) {
            PyErr_SetString(PyExc_TypeError, "Variable-length types not supported");
            return NULL;
        }

        // Create a new python type which inherits after dbzero_ce.Object (tp_base)
        // implements methods from py_class and overrides the following methods:
        // __init__, __getattr__, __setattr__, __delattr__, __getattribute__
        
        // 3.9.x compatible PyTypeObject
        auto &type_manager = PyToolkit::getTypeManager();
        char *data = new char[sizeof(PyHeapTypeObject) + sizeof(MemoTypeDecoration)];
        PyHeapTypeObject *ht_new_type = new (data) PyHeapTypeObject();
        new (data + sizeof(PyHeapTypeObject)) 
        MemoTypeDecoration(type_manager.getPooledString(prefix_name), type_manager.getPooledString(type_id));
        // Construct base type as a copy of the original type
        PyTypeObject *base_type = new PyTypeObject(*py_class);
        Py_INCREF(base_type);

        *ht_new_type = *reinterpret_cast<PyHeapTypeObject*>(py_class);
        PyTypeObject *new_type = (PyTypeObject*)ht_new_type;
        auto [type_name, full_type_name] = createWrappedTypeName(py_class->tp_name);
        new_type->tp_name = full_type_name;
        // remove Py_TPFLAGS_READY flag so that the type can be initialized by the PyType_Ready
        new_type->tp_flags = py_class->tp_flags & ~Py_TPFLAGS_READY;
        // extend basic size with pointer to a DBZero object instance        
        new_type->tp_basicsize = py_class->tp_basicsize + sizeof(db0::object_model::Object);
        // distinguish between singleton and non-singleton types
        new_type->tp_new = is_singleton ? reinterpret_cast<newfunc>(MemoObject_new_singleton) : reinterpret_cast<newfunc>(MemoObject_new);
        new_type->tp_dealloc = reinterpret_cast<destructor>(MemoObject_del);
        // override the init function
        new_type->tp_init = reinterpret_cast<initproc>(MemoObject_init);
        // make copy of the types dict
        new_type->tp_dict = copyDict(py_class->tp_dict);
        // getattr / setattr are obsolete
        new_type->tp_getattr = 0;
        new_type->tp_setattr = 0;
        // redirect to Memo_ functions
        new_type->tp_getattro = reinterpret_cast<getattrofunc>(MemoObject_getattro);
        new_type->tp_setattro = reinterpret_cast<setattrofunc>(MemoObject_setattro);
        // set original class (copy) as a base class
        new_type->tp_base = base_type;
        new_type->tp_richcompare = (richcmpfunc)MemoObject_rq;
        
        // method resolution order, tp_mro and tp_bases are filled in by PyType_Ready
        new_type->tp_mro = 0;
        new_type->tp_bases = 0;
        
        if (PyType_Ready(new_type) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to initialize new memo type");
            return NULL;
        }

        new_type->tp_str = reinterpret_cast<reprfunc>(MemoObject_str);
        new_type->tp_repr = reinterpret_cast<reprfunc>(MemoObject_str);
        base_type->tp_str = reinterpret_cast<reprfunc>(MemoObject_str);
        base_type->tp_repr = reinterpret_cast<reprfunc>(MemoObject_str);
        PyToolkit::getTypeManager().addMemoType(new_type, nullptr);
        Py_INCREF(new_type);
        // register new type with the module where the original type was located
        PyModule_AddObject(py_module, type_name, reinterpret_cast<PyObject*>(new_type));

        // add class fields class member to access memo type information
        PyObject *py_class_fields = PyClassFields_create(new_type);
        if (PyDict_SetItemString(new_type->tp_dict, "__fields__", py_class_fields) < 0) {
            Py_DECREF(py_class_fields);
            PyErr_SetString(PyExc_RuntimeError, "Failed to set __fields__");
            return NULL;
        }

        return new_type;
    }
    
    PyObject *wrapPyClass(PyObject *, PyObject *args, PyObject *kwargs)
    {        
        PyObject* class_obj;
        PyObject *singleton = Py_False;
        PyObject *py_prefix_name = nullptr;
        PyObject *py_type_id = nullptr;
        static const char *kwlist[] = { "input", "singleton", "prefix", "id", NULL };
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOO", const_cast<char**>(kwlist), &class_obj, &singleton, &py_prefix_name, &py_type_id)) {
            PyErr_SetString(PyExc_TypeError, "Invalid input arguments");
            return NULL;
        }

        bool is_singleton = PyObject_IsTrue(singleton);
        const char *prefix_name = py_prefix_name ? PyUnicode_AsUTF8(py_prefix_name) : nullptr;
        const char *type_id = py_type_id ? PyUnicode_AsUTF8(py_type_id) : nullptr;
        return reinterpret_cast<PyObject*>(wrapPyType(castToType(class_obj), is_singleton, prefix_name, type_id));
    }
    
    bool PyMemoType_Check(PyTypeObject *type) {
        return type->tp_init == reinterpret_cast<initproc>(MemoObject_init);
    }
    
    bool PyMemo_Check(PyObject *obj) {
        return PyMemoType_Check(Py_TYPE(obj));
    }

    bool PyMemoType_IsSingleton(PyTypeObject *type) {
        return type->tp_new == reinterpret_cast<newfunc>(MemoObject_new_singleton);
    }
    
    PyObject *MemoObject_GetFieldLayout(MemoObject *self)
    {
        auto field_layout = self->ext().getFieldLayout();        
        auto &pos_vt = field_layout.m_pos_vt_fields;
        auto &index_vt = field_layout.m_index_vt_fields;
        
        PyObject *py_pos_vt = PyList_New(pos_vt.size());
        for (std::size_t i = 0; i < pos_vt.size(); ++i) {        
            auto type_name = db0::to_string(pos_vt[i]);
            PyList_SET_ITEM(py_pos_vt, i, PyUnicode_FromString(type_name.c_str()));
        }
        
        PyObject *py_index_vt = PyDict_New();
        for (auto &item: index_vt) {
            auto type_name = db0::to_string(item.second);
            PyDict_SetItem(py_index_vt, PyLong_FromLong(item.first), PyUnicode_FromString(type_name.c_str()));
        }
        
        PyObject *py_result = PyDict_New();
        PyDict_SetItemString(py_result, "pos_vt", py_pos_vt);
        PyDict_SetItemString(py_result, "index_vt", py_index_vt);        
        return py_result;
    }
    
    PyObject *MemoObject_DescribeObject(MemoObject *self)
    {
        auto py_field_layout = MemoObject_GetFieldLayout(self);
        PyObject *py_result = PyDict_New();
        PyDict_SetItemString(py_result, "field_layout", py_field_layout);
        PyDict_SetItemString(py_result, "uuid", tryGetUUID(self));
        PyDict_SetItemString(py_result, "type", PyUnicode_FromString(self->ext().getType().getName().c_str()));
        PyDict_SetItemString(py_result, "size_of", PyLong_FromLong(self->ext()->sizeOf()));
        return py_result;
    }

    PyObject *MemoObject_IsTag(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "Invalid number of arguments");
            return NULL;
        }
        
        if (!PyMemo_Check(args[0])) {
            PyErr_SetString(PyExc_TypeError, "Invalid object type");
            return NULL;
        }

        auto &memo_obj = *reinterpret_cast<MemoObject*>(args[0]);
        return PyBool_FromLong(memo_obj.ext().isTag());
    }
    
    PyObject *MemoObject_str(MemoObject *pythis)
    {
        std::stringstream str;
        auto &memo = pythis->ext();
        str << "<" << Py_TYPE(pythis)->tp_name;
        if (memo.hasInstance()) {
            str << " instance uuid=" << PyUnicode_AsUTF8(tryGetUUID(pythis));
        } else {
            str << " (uninitialized)";
        }
        str << ">";
        return PyUnicode_FromString(str.str().c_str());
    }
    
    void PyMemoType_get_info(PyTypeObject *type, PyObject *dict)
    {                
        auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)type + sizeof(PyHeapTypeObject));
        PyDict_SetItemString(dict, "singleton", PyBool_FromLong(PyMemoType_IsSingleton(type)));
        PyDict_SetItemString(dict, "prefix", PyUnicode_FromString(decor.m_prefix_name_ptr));
    }

    void PyMemoType_close(PyTypeObject *type)
    {
        auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)type + sizeof(PyHeapTypeObject));
        decor.close();
    }
    
}