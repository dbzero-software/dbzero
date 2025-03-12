#include "Memo.hpp"
#include "PyToolkit.hpp"
#include <iostream>
#include <object.h>
#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"
#include "Utils.hpp"
#include "Types.hpp"
#include <dbzero/object_model/object.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/to_string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/types/PyClassFields.hpp>

namespace db0::python

{
    
    MemoTypeDecoration::MemoTypeDecoration(const char *prefix_name, const char *type_id, const char *file_name,
        std::vector<std::string> &&init_vars)
        : m_prefix_name_ptr(prefix_name)
        , m_type_id(type_id)
        , m_file_name(file_name)
        , m_init_vars(std::move(init_vars))
    {
    }
    
    std::uint64_t MemoTypeDecoration::getFixtureUUID(AccessType access_type)
    {
        if (m_prefix_name_ptr && !m_fixture_uuid) {
            // initialize fixture by prefix name and keep UUID for future use
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(m_prefix_name_ptr, access_type);
            m_fixture_uuid = fixture->getUUID();
        }
        return m_fixture_uuid;
    }
    
    void MemoTypeDecoration::close() {
        m_fixture_uuid = 0;
    }
    
    const char *MemoTypeDecoration::getPrefixName() const {
        return m_prefix_name_ptr ? m_prefix_name_ptr : "";
    }

    const std::vector<std::string> &MemoTypeDecoration::getInitVars() const {
        return m_init_vars;
    }
    
    MemoObject *tryMemoObject_new(PyTypeObject *py_type, PyObject *, PyObject *)
    {
        MemoTypeDecoration &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));
        // NOTE: read-only fixture access is sufficient here since objects are lazy-initialized
        // i.e. the actual dbzero instance is created on postInit
        // this is also important for dynamically scoped clases (where read/write access may not be possible on default fixture)
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(decor.getFixtureUUID(), AccessType::READ_ONLY);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find py type associated dbzero class with the ClassFactory
        auto type = class_factory.tryGetOrCreateType(py_type);
        // if type cannot be retrieved due to access mode then deferr this operation (fallback)
        if (!type) {
            auto type_initializer = [py_type](db0::swine_ptr<Fixture> &fixture) {
                auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
                return class_factory.getOrCreateType(py_type);
            };
            MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
            // prepare a new DB0 instance of a known DB0 class
            db0::object_model::Object::makeNew(&memo_obj->modifyExt(), std::move(type_initializer));
            return memo_obj;
        }

        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        // prepare a new DB0 instance of a known DB0 class
        db0::object_model::Object::makeNew(&memo_obj->modifyExt(), type);
        return memo_obj;
    }
    
    MemoObject *PyAPI_MemoObject_new(PyTypeObject *py_type, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return reinterpret_cast<MemoObject*>(runSafe(tryMemoObject_new, py_type, args, kwargs));
    }
    
    PyObject *tryMemoObject_open_singleton(PyTypeObject *py_type, const Fixture &fixture)
    {
        auto &class_factory = fixture.get<db0::object_model::ClassFactory>();
        // find py type associated dbzero class with the ClassFactory
        auto type = class_factory.tryGetExistingType(py_type);
        
        if (!type) {
            return nullptr;
        }

        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        // try unloading associated singleton if such exists
        if (!type->unloadSingleton(&memo_obj->modifyExt())) {
            py_type->tp_dealloc(memo_obj);
            return nullptr;
        }
        return memo_obj;
    }
    
    PyObject *tryMemoObject_open_singleton(PyTypeObject *py_type)
    {
        MemoTypeDecoration &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));
        // try opening from a known prefix
        if (decor.getFixtureUUID()) {
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(decor.getFixtureUUID(), AccessType::READ_ONLY);
            return tryMemoObject_open_singleton(py_type, *fixture);
        }

        // if prefix not known then scan all fixtures
        PyObject *py_singleton = nullptr;
        PyToolkit::getPyWorkspace().getWorkspace().forEachFixture([&](const Fixture &fixture) {
            py_singleton = tryMemoObject_open_singleton(py_type, fixture);
            return py_singleton == nullptr;
        });

        return py_singleton;
    }
    
    PyObject *tryMemoObject_new_singleton(PyTypeObject *py_type, PyObject *, PyObject *)
    {
        auto result = tryMemoObject_open_singleton(py_type);
        if (result) {
            return result;
        }

        MemoTypeDecoration &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));
        // NOTE: read-only access is sufficient because the object is lazy-initialized
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(decor.getFixtureUUID(), AccessType::READ_ONLY);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find py type associated dbzero class with the ClassFactory
        auto type = class_factory.tryGetOrCreateType(py_type);
        if (!type) {
            // if type cannot be retrieved due to access mode then deferr this operation (fallback)
            auto type_initializer = [py_type](db0::swine_ptr<Fixture> &fixture) {
                auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
                return class_factory.getOrCreateType(py_type);
            };
            MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
            db0::object_model::Object::makeNew(&memo_obj->modifyExt(), type_initializer);
            return memo_obj;
        }
        
        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        db0::object_model::Object::makeNew(&memo_obj->modifyExt(), type);
        return memo_obj;
    }
    
    PyObject *PyAPI_MemoObject_new_singleton(PyTypeObject *py_type, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryMemoObject_new_singleton, py_type, args, kwargs);
    }
    
    shared_py_object<MemoObject*> MemoObjectStub_new(PyTypeObject *py_type) {
        return { reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0)), false };
    }
    
    PyObject *MemoObject_alloc(PyTypeObject *self, Py_ssize_t nitems) {
        return PyType_GenericAlloc(self, nitems);
    }
    
    void MemoObject_del(MemoObject* memo_obj)
    {
        PY_API_FUNC
        // destroy associated DB0 Object instance
        memo_obj->destroy();        
        Py_TYPE(memo_obj)->tp_free((PyObject*)memo_obj);
    }
    
    int PyAPI_MemoObject_init(MemoObject* self, PyObject* args, PyObject* kwds)
    {
        PY_API_FUNC
        // the instance may already exist (e.g. if this is a singleton)
        if (!self->ext().hasInstance()) {
            auto py_type = Py_TYPE(self);
            auto base_type = py_type->tp_base;
            
            // invoke tp_init from base type (wrapped pyhon class)
            if (base_type->tp_init((PyObject*)self, args, kwds) < 0) {
                PyObject *ptype, *pvalue, *ptraceback;
                PyErr_Fetch(&ptype, &pvalue, &ptraceback);
                if (ptype == PyToolkit::getTypeManager().getBadPrefixError()) {
                    // from pvalue
                    std::uint64_t fixture_uuid = PyLong_AsUnsignedLong(pvalue);
                    auto type = self->ext().getClassPtr();
                    if (type->isExistingSingleton(fixture_uuid)) {
                        // drop existing instance
                        self->ext().destroy();
                        // unload singleton from a different fixture
                        if (!type->unloadSingleton(&self->modifyExt(), fixture_uuid)) {
                            PyErr_SetString(PyExc_RuntimeError, "Unloading singleton failed");
                            return -1;
                        }
                        return 0;
                    }
                }
                
                // Unrecognized error
                PyErr_Restore(ptype, pvalue, ptraceback);
                return -1;
            }
                        
            // invoke post-init on associated dbzero object
            auto &object = self->modifyExt();
            db0::FixtureLock fixture(object.getFixture());
            object.postInit(fixture);
            // need to call modifyExt again after postInit because the instance has just been created
            // and potentially needs to be included in the AtomicContext
            self->modifyExt();
            fixture->getLangCache().add(object.getAddress(), self);
        }

        return 0;
    }
    
    void MemoObject_drop(MemoObject* memo_obj)
    {
        // since objects are destroyed by GC0 drop is only responsible for marking
        // singletons as unreferenced
        if (memo_obj->ext().isSingleton()) {
            memo_obj->modifyExt().unSingleton();
            // the acutal destroy will be performed by the GC0 once removed from the LangCache
            auto &lang_cache = memo_obj->ext().getFixture()->getLangCache();
            lang_cache.erase(memo_obj->ext().getAddress());
            return;
        }
        
        if (!memo_obj->ext().hasInstance()) {
            return;
        }
        
        if (memo_obj->ext().hasRefs()) {
            PyErr_SetString(PyExc_RuntimeError, "delete failed: object has references");
            return;    
        }
         
        // create a null placeholder in place of the original instance to mark as deleted
        auto &lang_cache = memo_obj->ext().getFixture()->getLangCache();
        auto obj_addr = memo_obj->ext().getAddress();
        memo_obj->destroy();
        db0::object_model::Object::makeNull((void*)(&memo_obj->ext()));
        // remove instance from the lang cache
        lang_cache.erase(obj_addr);
    }
    
    PyObject *tryMemoObject_getattro(MemoObject *memo_obj, PyObject *attr)
    {
        // The method resolution order for Memo types is following:
        // 1. User type members (class members such as methods)
        // 2. DB0 object extension methods
        // 3. DB0 object members (attributes)
        // 4. User instance members (e.g. attributes set during __postinit__)                
        
        /* FIXME: 
        auto res = _PyObject_GetDescrOptional(reinterpret_cast<PyObject*>(memo_obj), attr);
        if (res) {
            return res;
        }
        */
        
        memo_obj->ext().getFixture()->refreshIfUpdated();
        auto member = memo_obj->ext().tryGet(PyUnicode_AsUTF8(attr));
        
        if (member) {
            return member.steal();
        }
        
        return _PyObject_GenericGetAttrWithDict(reinterpret_cast<PyObject*>(memo_obj), attr, NULL, 0);

        // raise AttributeError
        /*
        if (!member) {
            std::stringstream _str;
            _str << "AttributeError: " << PyUnicode_AsUTF8(attr) << " not found";
            PyErr_SetString(PyExc_AttributeError, _str.str().c_str());
            return nullptr;
        }
        return member.steal();
        */
    }
    
    PyObject *PyAPI_MemoObject_getattro(MemoObject *self, PyObject *attr)
    {
        PY_API_FUNC
        return runSafe(tryMemoObject_getattro, self, attr);
    }
    
    int PyAPI_MemoObject_setattro(MemoObject *self, PyObject *attr, PyObject *value)
    {
        PY_API_FUNC
        // assign value to a DB0 attribute
        Py_XINCREF(value);
        try {
            // must materialize the object before setting as an attribute
            if (!db0::object_model::isMaterialized(value)) {
                db0::FixtureLock lock(self->ext().getFixture());
                db0::object_model::materialize(lock, value);
            }
            
            if (self->ext().hasInstance()) {
                db0::FixtureLock lock(self->ext().getFixture());
                self->modifyExt().set(lock, PyUnicode_AsUTF8(attr), value);
            } else {
                // considered as a non-mutating operation
                self->ext().setPreInit(PyUnicode_AsUTF8(attr), value);
            }
        } catch (const std::exception &e) {
            Py_XDECREF(value);
            PyErr_SetString(PyExc_AttributeError, e.what());
            return -1;
        } catch (...) {
            Py_XDECREF(value);
            PyErr_SetString(PyExc_AttributeError, "Unknown exception");
            return -1;
        }
        Py_XDECREF(value);
        return 0;
    }
    
    bool isEqual(MemoObject *lhs, MemoObject *rhs) {
        return lhs->ext() == rhs->ext();
    }
    
    PyObject *PyAPI_MemoObject_rq(MemoObject *memo_obj, PyObject *other, int op)
    {
        PY_API_FUNC
        PyObject * obj_memo = reinterpret_cast<PyObject*>(memo_obj);
        // if richcompare is overriden by the python class, call the python class implementation
        if (obj_memo->ob_type->tp_base->tp_richcompare != PyType_Type.tp_richcompare) {
            // if the base class richcompare is the same as the memo richcompare don't call the base class richcompare
            // to avoid infinite recursion
            if(obj_memo->ob_type->tp_base->tp_richcompare != (richcmpfunc)PyAPI_MemoObject_rq){
                return obj_memo->ob_type->tp_base->tp_richcompare(reinterpret_cast<PyObject*>(memo_obj), other, op);
            }
        }
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
                Py_RETURN_NOTIMPLEMENTED;
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
    
    PyTypeObject *wrapPyType(PyTypeObject *py_class, bool is_singleton, const char *prefix_name,
        const char *type_id, const char *file_name, std::vector<std::string> &&init_vars)
    {
        Py_INCREF(py_class);
        PyObject *py_module = findModule(PyObject_GetAttrString((PyObject*)py_class, "__module__"));
        if (!py_module || !PyModule_Check(py_module)) {
            Py_DECREF(py_class);
            THROWF(db0::InternalException) << "Type related module not found: " << py_class->tp_name;
        }
        
        if (py_class->tp_dict == nullptr) {
            Py_DECREF(py_class);
            THROWF(db0::InternalException) << "Type has no tp_dict: " << py_class->tp_name;
        }
        
        if (py_class->tp_itemsize != 0) {
            Py_DECREF(py_class);
            THROWF(db0::InternalException) << "Variable-length types not supported: " << py_class->tp_name;                        
        }
        
        // Create a new python type which inherits after dbzero_ce.Object (tp_base)
        // implements methods from py_class and overrides the following methods:
        // __init__, __getattr__, __setattr__, __delattr__, __getattribute__
        
        // 3.9.x compatible PyTypeObject
        auto &type_manager = PyToolkit::getTypeManager();
        char *data = new char[sizeof(PyHeapTypeObject) + sizeof(MemoTypeDecoration)];
        PyHeapTypeObject *ht_new_type = new (data) PyHeapTypeObject();
        new (data + sizeof(PyHeapTypeObject)) MemoTypeDecoration(
            type_manager.getPooledString(prefix_name), 
            type_manager.getPooledString(type_id),
            type_manager.getPooledString(file_name),
            std::move(init_vars)
        );

        // Construct base type as a copy of the original type
        PyTypeObject *base_type = new PyTypeObject(*py_class);
        Py_INCREF(base_type);

        *ht_new_type = *reinterpret_cast<PyHeapTypeObject*>(py_class);
        PyTypeObject *new_type = (PyTypeObject*)ht_new_type;
        auto [type_name, full_type_name] = createWrappedTypeName(py_class->tp_name);
        new_type->tp_name = full_type_name;
        // remove Py_TPFLAGS_READY flag so that the type can be initialized by the PyType_Ready
        new_type->tp_flags = py_class->tp_flags & ~Py_TPFLAGS_READY;
        // extend basic size with pointer to a dbzero object instance
        new_type->tp_basicsize = py_class->tp_basicsize + sizeof(db0::object_model::Object);
        // distinguish between singleton and non-singleton types
        new_type->tp_new = is_singleton ? reinterpret_cast<newfunc>(PyAPI_MemoObject_new_singleton) : reinterpret_cast<newfunc>(PyAPI_MemoObject_new);
        new_type->tp_dealloc = reinterpret_cast<destructor>(MemoObject_del);
        // override the init function
        new_type->tp_init = reinterpret_cast<initproc>(PyAPI_MemoObject_init);
        // make copy of the types dict
        new_type->tp_dict = copyDict(py_class->tp_dict);
        // getattr / setattr are obsolete
        new_type->tp_getattr = 0;
        new_type->tp_setattr = 0;
        // redirect to Memo_ functions
        new_type->tp_getattro = reinterpret_cast<getattrofunc>(PyAPI_MemoObject_getattro);
        new_type->tp_setattro = reinterpret_cast<setattrofunc>(PyAPI_MemoObject_setattro);
        // set original class (copy) as a base class
        new_type->tp_base = base_type;
        new_type->tp_richcompare = (richcmpfunc)PyAPI_MemoObject_rq;
        // method resolution order, tp_mro and tp_bases are filled in by PyType_Ready
        new_type->tp_mro = 0;
        new_type->tp_bases = 0;
        
        if (PyType_Ready(new_type) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to initialize new memo type");
            return NULL;
        }

        if (PyType_Type.tp_str == py_class->tp_str) {
            new_type->tp_str = reinterpret_cast<reprfunc>(MemoObject_str);
            base_type->tp_str = reinterpret_cast<reprfunc>(MemoObject_str);
        }

        if (PyType_Type.tp_repr == py_class->tp_repr) {
            new_type->tp_repr = reinterpret_cast<reprfunc>(MemoObject_str);
            base_type->tp_repr = reinterpret_cast<reprfunc>(MemoObject_str);
        }
        PyToolkit::getTypeManager().addMemoType(new_type, type_id);
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
    
    PyObject *PyAPI_wrapPyClass(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        PyObject* class_obj;
        PyObject *singleton = Py_False;
        PyObject *py_prefix_name = nullptr;
        PyObject *py_type_id = nullptr;
        PyObject *py_file_name = nullptr;
        PyObject *py_init_vars = nullptr;

        static const char *kwlist[] = { "input", "singleton", "prefix", "id", "py_file", "py_init_vars", NULL };
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOOOO", const_cast<char**>(kwlist), &class_obj, &singleton,
            &py_prefix_name, &py_type_id, &py_file_name, &py_init_vars))
        {
            PyErr_SetString(PyExc_TypeError, "Invalid input arguments");
            return NULL;
        }
        
        bool is_singleton = PyObject_IsTrue(singleton);
        const char *prefix_name = py_prefix_name ? PyUnicode_AsUTF8(py_prefix_name) : nullptr;
        const char *type_id = py_type_id ? PyUnicode_AsUTF8(py_type_id) : nullptr;        
        const char *file_name = (py_file_name && py_file_name != Py_None) ? PyUnicode_AsUTF8(py_file_name) : nullptr;
        std::vector<std::string> init_vars;
        if (py_init_vars) {
            if (!PyList_Check(py_init_vars)) {
                PyErr_SetString(PyExc_TypeError, "Expected list of strings");
                return NULL;
            }
            Py_ssize_t size = PyList_Size(py_init_vars);
            for (Py_ssize_t i = 0; i < size; ++i) {
                PyObject *item = PyList_GetItem(py_init_vars, i);
                if (!PyUnicode_Check(item)) {
                    PyErr_SetString(PyExc_TypeError, "Expected list of strings");
                    return NULL;
                }
                init_vars.push_back(PyUnicode_AsUTF8(item));
            }
        }
        
        return reinterpret_cast<PyObject*>(
            runSafe(wrapPyType, castToType(class_obj), is_singleton, prefix_name, type_id, file_name, std::move(init_vars))
        );
    }
    
    bool PyMemoType_Check(PyTypeObject *type) {
        return type->tp_init == reinterpret_cast<initproc>(PyAPI_MemoObject_init);
    }
    
    PyObject *PyAPI_PyMemo_Check(PyObject *self, PyObject *const * args, Py_ssize_t nargs) {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "is_memo requires 1 argument");
            return NULL;
        }
        int is_memo = runSafe<-1>(PyMemo_Check, args[0]);
        if(is_memo == -1){
            return nullptr;
        }
        return PyBool_FromLong(is_memo);
    }

    bool PyMemo_Check(PyObject *obj) {
        return PyMemoType_Check(Py_TYPE(obj));
    }
    
    bool PyMemoType_IsSingleton(PyTypeObject *type) {
        return type->tp_new == reinterpret_cast<newfunc>(PyAPI_MemoObject_new_singleton);
    }
    
    PyObject *MemoObject_GetFieldLayout(MemoObject *self)
    {        
        auto field_layout = self->ext().getFieldLayout();        
        auto &pos_vt = field_layout.m_pos_vt_fields;
        auto &index_vt = field_layout.m_index_vt_fields;
        
        // report pos-vt members
        PyObject *py_pos_vt = PyList_New(pos_vt.size());
        for (std::size_t i = 0; i < pos_vt.size(); ++i) {        
            auto type_name = db0::to_string(pos_vt[i]);
            PyList_SET_ITEM(py_pos_vt, i, PyUnicode_FromString(type_name.c_str()));
        }
        
        // report index-vt members
        PyObject *py_index_vt = PyDict_New();
        for (auto &item: index_vt) {
            auto type_name = db0::to_string(item.second);
            PyDict_SetItem(py_index_vt, PyLong_FromLong(item.first), PyUnicode_FromString(type_name.c_str()));
        }
        
        // report kv-index members
        PyObject *py_kv_index = PyDict_New();
        for (auto &item: field_layout.m_kv_index_fields) {
            auto type_name = db0::to_string(item.second);
            PyDict_SetItem(py_kv_index, PyLong_FromLong(item.first), PyUnicode_FromString(type_name.c_str()));
        }

        PyObject *py_result = PyDict_New();
        PyDict_SetItemString(py_result, "pos_vt", py_pos_vt);
        PyDict_SetItemString(py_result, "index_vt", py_index_vt);
        PyDict_SetItemString(py_result, "kv_index", py_kv_index);
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
    
    PyObject *tryMemoObject_str(MemoObject *self)
    {
        std::stringstream str;
        auto &memo = self->ext();
        str << "<" << Py_TYPE(self)->tp_name;
        if (memo.hasInstance()) {
            str << " instance uuid=" << PyUnicode_AsUTF8(tryGetUUID(self));
        } else {
            str << " (uninitialized)";
        }
        str << ">";
        return PyUnicode_FromString(str.str().c_str());
    }

    PyObject *MemoObject_str(MemoObject *self)
    {
        PY_API_FUNC
        return runSafe(tryMemoObject_str, self);
    }
    
    void MemoType_get_info(PyTypeObject *type, PyObject *dict)
    {                      
        auto &decor = MemoTypeDecoration::get(type);
        PyDict_SetItemString(dict, "singleton", PyBool_FromLong(PyMemoType_IsSingleton(type)));
        PyDict_SetItemString(dict, "prefix", PyUnicode_FromString(decor.getPrefixName()));
    }
    
    void MemoType_close(PyTypeObject *type)
    {        
        auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)type + sizeof(PyHeapTypeObject));
        decor.close();
    }
    
    PyObject *MemoObject_set_prefix(MemoObject *py_obj, const char *prefix_name)
    {        
        if (prefix_name) {
            // can use "ext" since setFixtue is a non-mutating operation
            auto &obj = const_cast<db0::object_model::Object&>(py_obj->ext());
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(prefix_name, AccessType::READ_WRITE);            
            obj.setFixture(fixture);
        }
        Py_RETURN_NONE;
    }
    
    PyObject *tryGetAttributes(PyTypeObject *type)
    {
        auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)type + sizeof(PyHeapTypeObject));
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(decor.getFixtureUUID(), AccessType::READ_ONLY);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();        
        return tryGetClassAttributes(*class_factory.getExistingType(type));
    }
    
    PyObject *tryGetAttrAs(MemoObject *memo_obj, PyObject *attr, PyTypeObject *py_type)
    {
        memo_obj->ext().getFixture()->refreshIfUpdated();
        auto member = memo_obj->ext().tryGetAs(PyUnicode_AsUTF8(attr), py_type);
        if (member) {
            return member.steal();
        }

        return _PyObject_GenericGetAttrWithDict(reinterpret_cast<PyObject*>(memo_obj), attr, NULL, 0);
    }
    
    PyObject *getKwargsForMethod(PyObject* method, PyObject* kwargs)
    {
        PyObject *inspec_module = PyImport_ImportModule("inspect");
        PyObject *signatue = PyObject_CallMethod(inspec_module, "signature", "O", method);
        PyObject *parameters = PyObject_GetAttrString(signatue, "parameters");
        PyObject *iterator = PyObject_GetIter(parameters);
        PyObject *elem;
        PyObject *py_result = PyDict_New();
        while ((elem = PyIter_Next(iterator))) {
            if (PyDict_Contains(kwargs, elem) == 1) {
                PyDict_SetItem(py_result, elem, PyDict_GetItem(kwargs, elem));
            }
            Py_DECREF(elem);
        }
        Py_DECREF(iterator);
        Py_DECREF(parameters);
        Py_DECREF(signatue);
        Py_DECREF(inspec_module);
        return py_result;
    }
    
    PyObject *tryLoadMemo(MemoObject *memo_obj, PyObject *kwargs, PyObject *py_exclude)
    {
        PyObject *py_result = PyDict_New();
        auto load_method = tryMemoObject_getattro(memo_obj, PyUnicode_FromString("__load__"));
                
        if (load_method) {
            if (py_exclude != nullptr && py_exclude != Py_None && PySequence_Check(py_exclude)) {
                PyErr_SetString(PyExc_AttributeError, "Cannot exlude values when __load__ is implemented");
                return nullptr;
            }
            
            PyObject * result;
            if (kwargs != nullptr) {
                PyObject *args = PyTuple_New(0);
                PyObject * method_kwargs = getKwargsForMethod(load_method, kwargs);
                result = PyObject_Call(load_method, args, method_kwargs);
                Py_DECREF(args);
            } else {
                result = PyObject_CallObject(load_method, nullptr);
            }
            if (result == nullptr) {
                return result;
            }
            Py_DECREF(load_method);
            return runSafe(tryLoad, result, kwargs, nullptr);
        }
        
        // reset Python error
        // FIXME: optimization opportunity if we're able to eliminate error message formatting
        PyErr_Clear();
        
        bool has_error = false;
        memo_obj->ext().forAll([py_result, memo_obj, py_exclude, kwargs, &has_error](const std::string &key, PyTypes::ObjectSharedPtr) {
            if (!has_error) {
                auto key_obj = PyUnicode_FromString(key.c_str());
                auto attr = PyAPI_MemoObject_getattro(memo_obj, key_obj);
                if (!attr) {
                    has_error = true;
                    return;
                }
                
                if (py_exclude == nullptr || py_exclude == Py_None || PySequence_Contains(py_exclude, key_obj) == 0) {
                    PyObject *res = runSafe(tryLoad, attr, kwargs, nullptr);
                    if (res == nullptr) {
                        has_error = true;
                    } else {
                        PyDict_SetItemString(py_result, key.c_str(), res);
                    }
                }
                Py_DECREF(attr);
                Py_DECREF(key_obj);
            }
        });
        if (has_error) {
            return nullptr;
        }
        return py_result;
    }

}