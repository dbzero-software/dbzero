// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Memo.hpp"
#include "PyToolkit.hpp"
#include <iostream>
#include <object.h>
#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"
#include "Utils.hpp"
#include "Types.hpp"
#include "Migration.hpp"
#include "PyHash.hpp"
#include <dbzero/object_model/object.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/to_string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/types/PyClassFields.hpp>
#include <dbzero/bindings/TypeId.hpp>

#ifndef Py_TPFLAGS_MANAGED_DICT
#define Py_TPFLAGS_MANAGED_DICT 0
#endif

namespace db0::python

{

    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    using TypeObjectSharedPtr = PyTypes::TypeObjectSharedPtr;
    
    // @return type name / full type name (tp_name)
    std::pair<std::string, std::string> getMemoTypeName(shared_py_object<PyTypeObject*> py_class)
    {
        std::stringstream str;
        str << "Memo_" << (*py_class)->tp_name;
        auto type_name = str.str();
        auto full_type_name = std::string("dbzero.") + type_name;
        return { type_name, full_type_name };
    }
    
    template <typename MemoImplT>
    MemoImplT *tryMemoObject_new(const MemoTypeDecoration &decor, PyTypeObject *py_type, PyObject *, PyObject *)
    {        
        // NOTE: read-only fixture access is sufficient here since objects are lazy-initialized
        // i.e. the actual dbzero instance is created on postInit
        // this is also important for dynamically scoped clases (where read/write access may not be possible on default fixture)

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(decor.getFixtureUUID(), AccessType::READ_ONLY);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find py type associated dbzero class with the ClassFactory
        auto type = class_factory.tryGetOrCreateType(py_type);
        MemoImplT *memo_obj = reinterpret_cast<MemoImplT*>(py_type->tp_alloc(py_type, 0));
        
        // if type cannot be retrieved due to access mode then defer this operation (fallback)
        if (type) {
            // prepare a new dbzero instance of a known db0 class
            memo_obj->makeNew(type);
        } else {
            auto type_initializer = [py_type](db0::swine_ptr<Fixture> &fixture) {
                auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
                return class_factory.getOrCreateType(py_type);
            };
            // prepare a new db0 instance of a known db0 class
            memo_obj->makeNew(std::move(type_initializer));
        }
        
        return memo_obj;
    }
    
    Py_hash_t PyAPI_MemoHash(MemoObject *self)
    {
        PY_API_FUNC
        auto fixture = self->ext().getFixture();
        return runSafe(getPyHashImpl<TypeId::MEMO_OBJECT>, fixture, self);
    }
    
    template <typename MemoImplT>
    PyObject *tryMemoObject_str(MemoImplT *self)
    {
        std::stringstream str;
        auto &memo = self->ext();
        str << "<" << Py_TYPE(self)->tp_name;
        if (memo.hasInstance()) {
            str << " instance uuid=" << PyUnicode_AsUTF8(*Py_OWN(tryGetUUID(self)));
        } else {
            str << " (uninitialized)";
        }
        str << ">";
        return PyUnicode_FromString(str.str().c_str());
    }

    template <typename MemoImplT>
    PyObject *PyAPI_MemoObject_str(MemoImplT *self)
    {
        PY_API_FUNC
        return runSafe(tryMemoObject_str<MemoImplT>, self);
    }

    template <typename MemoImplT>
    PyObject *PyAPI_MemoObject_new(PyTypeObject *py_type, PyObject *args, PyObject *kwargs)
    {
        const auto &decor = MemoTypeDecoration::get(py_type);
        PY_API_FUNC
        return runSafe(tryMemoObject_new<MemoImplT>, decor, py_type, args, kwargs);
    }
    
    void tryGetScope(PyTypeObject *py_type, PyObject *args, PyObject *kwargs,
        std::string &px_name, std::uint64_t &fixture_uuid)
    {
        auto &decor = MemoTypeDecoration::get(py_type);        
        
        // if the type is dynamic scoped, then try resolving it dynamically
        if (decor.hasDynPrefix()) {
            px_name = decor.getDynPrefix(args, kwargs);
            if (!px_name.empty()) {
                return;
            }
        }
        
        // otherwise either retrieve static scope or just the default fixture
        fixture_uuid = decor.getFixtureUUID();
    }
    
    db0::swine_ptr<Fixture> tryGetFixture(const std::string &px_name, std::uint64_t fixture_uuid,
        AccessType access_type)
    {
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        if (!px_name.empty()) {
            // get existing or create a new prefix
            if (workspace.hasFixture(px_name) || access_type == AccessType::READ_WRITE) {
                return workspace.getFixture(px_name, access_type);
            }
            // prefix not found and could not be created
            return {};            
        }
        return workspace.getFixture(fixture_uuid, access_type);
    }

    PyObject *tryMemoObject_new_singleton(PyTypeObject *py_type, PyObject *args, PyObject *kwargs)
    {
        std::string px_name;
        std::uint64_t fixture_uuid = 0;
        // try resolve type specific scope: static, dynamic or default
        tryGetScope(py_type, args, kwargs, px_name, fixture_uuid);
        // NOTE: read-only access is sufficient because the object is lazy-initialized
        auto fixture = tryGetFixture(px_name, fixture_uuid, AccessType::READ_ONLY);

        // try opening existing singleton from an existing fixture
        if (fixture) {
            auto result = tryMemoObject_open_singleton(py_type, *fixture);
            if (result) {
                return result;
            }
        }
        
        // fixture, does not exist, try creating a new one
        if (!fixture) {
            fixture = tryGetFixture(px_name, fixture_uuid, AccessType::READ_WRITE);
        }
        
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find py type associated dbzero class with the ClassFactory
        auto type = class_factory.tryGetOrCreateType(py_type);
        MemoObject *memo_obj = nullptr;
        if (type) {
            memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
            memo_obj->makeNew(type);
        } else {
            // if type cannot be retrieved due to access mode then deferr this operation (fallback)
            auto type_initializer = [py_type](db0::swine_ptr<Fixture> &fixture) {
                auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
                return class_factory.getOrCreateType(py_type);
            };
            memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
            memo_obj->makeNew(type_initializer);
        }
        
        return memo_obj;
    }
    
    PyObject *PyAPI_MemoObject_new_singleton(PyTypeObject *py_type, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return runSafe(tryMemoObject_new_singleton, py_type, args, kwargs);
    }
    
    MemoObject *MemoObjectStub_new(PyTypeObject *py_type) {
        return reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
    }
    
    PyObject *MemoObject_alloc(PyTypeObject *self, Py_ssize_t nitems) {
        return PyType_GenericAlloc(self, nitems);
    }
    
    template <typename MemoImplT>
    void PyAPI_MemoObject_del(MemoImplT *memo_obj)
    {
        PY_API_FUNC
        // destroy associated db0 Object instance
        memo_obj->destroy();
        if (Py_IsInitialized())
        {
            // Skip deallocation during/after Python finalization
            // Python Garbage Collector might be finalized (i.e. destroyed) at this point
            Py_TYPE(memo_obj)->tp_free((PyObject*)memo_obj);
        }
    }

    template <typename MemoImplT>
    int MemoObject_traverse(MemoImplT *self, visitproc visit, void *arg)
    {
        // No Python object references to traverse in memo objects
        // They only contain dbzero native objects
        return 0;
    }

    template <typename MemoImplT>
    int MemoObject_clear(MemoImplT *self)
    {
        // No Python object references to clear in memo objects  
        // They only contain dbzero native objects
        return 0;
    }
    
    template <typename MemoImplT>
    int PyAPI_MemoObject_init(MemoImplT *self, PyObject* args, PyObject* kwds)
    {
        using Class = db0::object_model::Class;
        using TagIndex = db0::object_model::TagIndex;
        using ExtT = typename MemoImplT::ExtT;

        PY_API_FUNC
        // the instance may already exist (e.g. if this is a singleton)        
        if (!self->ext().hasInstance()) {
            auto py_type = Py_TYPE(self);
            auto base_type = py_type->tp_base;
            
            // invoke tp_init from base type (wrapped pyhon class)
            if (base_type->tp_init((PyObject*)self, args, kwds) < 0) {
                // mark object as defunct
                self->ext().setDefunct();
                PyObject *ptype, *pvalue, *ptraceback;
                PyErr_Fetch(&ptype, &pvalue, &ptraceback);
                if (ptype == PyToolkit::getTypeManager().getBadPrefixError()) {
                    // from pvalue
                    std::uint64_t fixture_uuid = PyLong_AsUnsignedLong(pvalue);
                    auto type = self->ext().getClassPtr();
                    if (type->isExistingSingleton(fixture_uuid)) {
                        // drop existing instance
                        // NOTE: may use ext() because destroy does not mutate the instance itself
                        const_cast<ExtT&>(self->ext()).destroy();
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
            const Class *class_ptr = &object.getType();
            if (!class_ptr || !class_ptr->isNoCache()) {
                fixture->getLangCache().add(object.getAddress(), self);
            }
            
            // finally, unless opted-out, assign the type tag(s) of the entire type hierarchy            
            if (class_ptr && class_ptr->assignDefaultTags()) {
                auto &tag_index = fixture->get<TagIndex>();
                while (class_ptr) {
                    tag_index.addTag(self, class_ptr->getAddress(), true);
                    class_ptr = class_ptr->getBaseClassPtr();
                }
            }
        }
        
        return 0;
    }
    
    void MemoObject_drop(MemoObject* memo_obj)
    {        
        // since objects are destroyed by GC0 drop is only responsible for marking
        // singletons as unreferenced
        if (memo_obj->ext().isSingleton()) {
            db0::FixtureLock lock(memo_obj->ext().getFixture());
            memo_obj->modifyExt().unSingleton(lock);
            if (!memo_obj->ext().isNoCache()) {
                // the actual destroy will be performed by the GC0 once removed from the LangCache
                auto &lang_cache = memo_obj->ext().getFixture()->getLangCache();
                lang_cache.erase(memo_obj->ext().getAddress());
            }

            return;
        }
        
        if (!memo_obj->ext().hasInstance()) {
            return;
        }
        
        if (memo_obj->ext().hasRefs()) {
            THROWF(db0::InputException) << "Cannot delete a memo object with references";
        }
        
        // create a null placeholder in place of the original instance to mark as deleted
        auto &lang_cache = memo_obj->ext().getFixture()->getLangCache();
        bool no_cache = memo_obj->ext().isNoCache();
        auto obj_addr = memo_obj->ext().getUniqueAddress();
        db0::FixtureLock lock(memo_obj->ext().getFixture());
        memo_obj->modifyExt().dropInstance(lock);
        // remove instance from the lang cache
        if (!no_cache) {
            lang_cache.erase(obj_addr);
        }        
    }
    
    bool isPersistentAttrName(const char *attr_name) {
        // non-persistent attribute names start with _X__ prefix
        return !(attr_name[0] == '_' && attr_name[1] == 'X' && attr_name[2] == '_' && attr_name[3] == '_');
    }

    template <typename MemoImplT>
    PyObject *tryMemoObject_getattro(MemoImplT *memo_obj, PyObject *attr)
    {
        // The method resolution order for Memo types is following:
        // 1. User type members (class members such as methods)
        // 2. db0 object extension methods
        // 3. db0 object members (attributes)
        // 4. User instance members (e.g. attributes set during __postinit__)
        const char *attr_name = PyUnicode_AsUTF8(attr);
        if (!attr_name) {
            PyErr_SetString(PyExc_AttributeError, "Invalid attribute name");
            return nullptr;
        }

        if (isPersistentAttrName(attr_name)) {
            memo_obj->ext().getFixture()->refreshIfUpdated();
            auto member = memo_obj->ext().tryGet(PyUnicode_AsUTF8(attr));
            
            if (member.get()) {
                return member.steal();
            }
        }
        
        // Fallback to type-level attribute lookup only (no instance dict)
        return PyObject_GenericGetAttr(reinterpret_cast<PyObject*>(memo_obj), attr);
    }
    
    template <typename MemoImplT>
    PyObject *PyAPI_MemoObject_getattro(MemoImplT *self, PyObject *attr)
    {
        PY_API_FUNC
        return runSafe(tryMemoObject_getattro<MemoImplT>, self, attr);
    }
    
    template <typename MemoImplT>
    int PyAPI_MemoObject_setattro(MemoImplT *self, PyObject *attr, PyObject *value);

    // regular memo object specialization
    template <>
    int PyAPI_MemoObject_setattro<MemoObject>(MemoObject *self, PyObject *attr, PyObject *value)
    {
        PY_API_FUNC        

        // assign value to a dbzero attribute
        const char* attr_name = PyUnicode_AsUTF8(attr);
        if (!attr_name) {
            return -1;
        }
        
        if (isPersistentAttrName(attr_name)) {
            try {
                // must materialize the object before setting as an attribute
                if (value && !db0::object_model::isMaterialized(value)) {
                    db0::FixtureLock lock(self->ext().getFixture());
                    db0::object_model::materialize(lock, value);
                }
                
                if (self->ext().hasInstance()) {
                    db0::FixtureLock lock(self->ext().getFixture());
                    self->modifyExt().set(lock, attr_name, value);
                } else {
                    // considered as a non-mutating operation
                    self->ext().setPreInit(attr_name, value);
                }
            } catch (const std::exception &e) {
                PyErr_SetString(PyExc_AttributeError, e.what());
                return -1;
            } catch (...) {            
                PyErr_SetString(PyExc_AttributeError, "Unknown exception");
                return -1;
            }
            return 0;
        } else {
            // Handle the non-persistent (_X__***) attribute assignment
            auto py_type = Py_TYPE(self);
            if (!py_type->tp_base) {
                PyErr_SetString(PyExc_AttributeError, "Cannot set non-persistent attribute");
                return -1;
            }
            
            // Forward to base class setattro
            return py_type->tp_base->tp_setattro((PyObject*)self, attr, value);
        }                
    }
    
    // immutable memo object specialization
    template <>
    int PyAPI_MemoObject_setattro<MemoImmutableObject>(MemoImmutableObject *self, PyObject *attr, PyObject *value)
    {
        PY_API_FUNC
        // assign value to a dbzero attribute
        try {
            // must materialize the object before setting as an attribute
            if (value && !db0::object_model::isMaterialized(value)) {
                db0::FixtureLock lock(self->ext().getFixture());
                db0::object_model::materialize(lock, value);
            }
            
            if (self->ext().hasInstance()) {
                PyErr_SetString(PyExc_AttributeError, "Cannot modify an immutable memo object");
                return -1;
            } else {
                // considered as a non-mutating operation
                self->ext().setPreInit(PyUnicode_AsUTF8(attr), value);
            }
        } catch (const std::exception &e) {
            PyErr_SetString(PyExc_AttributeError, e.what());
            return -1;
        } catch (...) {            
            PyErr_SetString(PyExc_AttributeError, "Unknown exception");
            return -1;
        }      
        
        return 0;
    }

    template <typename MemoImplT>
    bool isSame(MemoImplT *lhs, MemoImplT *rhs) {
        return lhs->ext() == rhs->ext();
    }
    
    template <typename MemoImplT>
    PyObject *PyAPI_MemoObject_rq(MemoImplT *memo_obj, PyObject *other, int op)
    {
        PY_API_FUNC
        PyObject * obj_memo = reinterpret_cast<PyObject*>(memo_obj);
        // if richcompare is overriden by the python class, call the python class implementation
        if (obj_memo->ob_type->tp_base->tp_richcompare != PyType_Type.tp_richcompare) {
            // if the base class richcompare is the same as the memo richcompare don't call the base class richcompare
            // to avoid infinite recursion
            if (obj_memo->ob_type->tp_base->tp_richcompare != (richcmpfunc)PyAPI_MemoObject_rq<MemoImplT>) {
                return obj_memo->ob_type->tp_base->tp_richcompare(reinterpret_cast<PyObject*>(memo_obj), other, op);
            }
        }
        
        bool eq_result = false;
        if (PyMemo_Check<MemoImplT>(other)) {
            eq_result = isSame(memo_obj, reinterpret_cast<MemoImplT*>(other));
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
    
    PyObject *findModule(PyObject *module_name)
    {
        auto sys_module = Py_OWN(PyImport_ImportModule("sys"));
        if (!sys_module) {
            return nullptr;            
        }

        auto modules_dict = Py_OWN(PyObject_GetAttrString(*sys_module, "modules"));
        if (!modules_dict) {
            return nullptr;            
        }

        return Py_NEW(PyDict_GetItem(*modules_dict, module_name));
    }
    
    // Copy a python dict
    PyObject *copyDict(PyObject *dict)
    {
        PyObject *key, *value;
        Py_ssize_t pos = 0;
        auto new_dict = Py_OWN(PyDict_New());

        while (PyDict_Next(dict, &pos, &key, &value)) {
            PySafeDict_SetItem(*new_dict, Py_BORROW(key), Py_BORROW(value));
        }
        
        return new_dict.steal();
    }
    
    // Regular memo slots
    static PyType_Slot MemoObject_common_slots[] = {
        {Py_tp_new, (void *)PyAPI_MemoObject_new<MemoObject>},
        {Py_tp_dealloc, (void *)(PyAPI_MemoObject_del<MemoObject>)},
        {Py_tp_init, (void *)PyAPI_MemoObject_init<MemoObject>},
        {Py_tp_getattro, (void *)PyAPI_MemoObject_getattro<MemoObject>},
        {Py_tp_setattro, (void *)PyAPI_MemoObject_setattro<MemoObject>},
        {Py_tp_richcompare, (void *)PyAPI_MemoObject_rq<MemoObject>},
        {Py_tp_hash, (void *)PyAPI_MemoHash},
        {Py_tp_traverse, (void *)MemoObject_traverse<MemoObject>},
        {Py_tp_clear, (void *)MemoObject_clear<MemoObject>},        
        {0, 0}
    };
    
    // Immutable memo slots
    static PyType_Slot MemoImmutableObject_common_slots[] = {
        {Py_tp_new, (void *)PyAPI_MemoObject_new<MemoImmutableObject>},
        {Py_tp_dealloc, (void *)(PyAPI_MemoObject_del<MemoImmutableObject>)},
        {Py_tp_init, (void *)PyAPI_MemoObject_init<MemoImmutableObject>},
        {Py_tp_getattro, (void *)PyAPI_MemoObject_getattro<MemoImmutableObject>},
        // set available only on pre-initialized objects
        {Py_tp_setattro, (void *)PyAPI_MemoObject_setattro<MemoImmutableObject>},
        {Py_tp_richcompare, (void *)PyAPI_MemoObject_rq<MemoImmutableObject>},
        {Py_tp_hash, (void *)PyAPI_MemoHash},
        {Py_tp_traverse, (void *)MemoObject_traverse<MemoImmutableObject>},
        {Py_tp_clear, (void *)MemoObject_clear<MemoImmutableObject>},        
        {0, 0}
    };
    
    template <typename MemoImplT>
    PyType_Slot* getCommonSlots();

    template <> PyType_Slot *getCommonSlots<MemoObject>() {
        return MemoObject_common_slots;
    }

    template <> PyType_Slot *getCommonSlots<MemoImmutableObject>() {
        return MemoImmutableObject_common_slots;
    }
    
    template <typename MemoImplT>
    PyTypeObject *PyMemoType_FromSpec(PyTypeObject *base_class, const char *tp_name, bool is_singleton)
    {
        // fill-in type specific slots first
        std::vector<PyType_Slot> slots;
        // fill-in common slots first
        auto slot_ptr = getCommonSlots<MemoImplT>();
        while (slot_ptr->slot || slot_ptr->pfunc) {
#if PY_VERSION_HEX < 0x030B0000  // Python < 3.11
            // Include all slots including traverse/clear for GC compatibility
            slots.push_back(*slot_ptr);
#else
            // Skip traverse/clear slots for Python 3.11+ where GC is disabled
            if (slot_ptr->slot != Py_tp_traverse && slot_ptr->slot != Py_tp_clear) {
                slots.push_back(*slot_ptr);
            }
#endif
            ++slot_ptr;
        }
        
        if (is_singleton) {
            // NOTE: singletons are not supported for immutable memo objects
            slots.push_back({Py_tp_new, (void *)PyAPI_MemoObject_new_singleton});
        } else {
            slots.push_back({Py_tp_new, (void *)PyAPI_MemoObject_new<MemoImplT>});
        }
        
        slots.push_back({0, 0});
        
        // Enable GC for Python 3.10 compatibility - required for inheritance hierarchies
        std::uint32_t flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
#if PY_VERSION_HEX < 0x030B0000  // Python < 3.11
        flags |= Py_TPFLAGS_HAVE_GC;
#endif
        flags &= ~Py_TPFLAGS_MANAGED_DICT;
        
        auto type_spec = PyType_Spec {
            .name = tp_name,
            .basicsize = MemoImplT::sizeOf(),            
            .flags = flags,
            .slots = slots.data()
        };
        
        auto bases = Py_OWN(PySafeTuple_Pack(Py_BORROW(base_class)));
        auto tp_result = Py_OWN((PyTypeObject*)PyType_FromSpecWithBases(&type_spec, *bases));
        (*tp_result)->tp_dict = copyDict(base_class->tp_dict);
        // disable weak-refs (important for Python 3.11.x)
        (*tp_result)->tp_weaklistoffset = 0;
#if PY_VERSION_HEX < 0x030B0000  // Python < 3.11
        (*tp_result)->tp_dictoffset = MemoImplT::getDictOffset();        
#else
        // will use managed dict for Python 3.11+
        (*tp_result)->tp_dictoffset = 0;
#endif
        
        // replace default __str__ and __repr__ implementations
        if (base_class->tp_str == PyType_Type.tp_str) {
            (*tp_result)->tp_str = reinterpret_cast<reprfunc>(PyAPI_MemoObject_str<MemoImplT>);
            base_class->tp_str = reinterpret_cast<reprfunc>(PyAPI_MemoObject_str<MemoImplT>);
        }
        
        if (base_class->tp_repr == PyType_Type.tp_repr) {
            (*tp_result)->tp_repr = reinterpret_cast<reprfunc>(PyAPI_MemoObject_str<MemoImplT>);
            base_class->tp_repr = reinterpret_cast<reprfunc>(PyAPI_MemoObject_str<MemoImplT>);
        }

        return tp_result.steal();
    }
    
    PyObject *wrapPyType(PyTypeObject *base_class, bool is_singleton, bool no_default_tags, const char *prefix_name,
        const char *type_id, const char *file_name, std::vector<std::string> &&init_vars, PyObject *py_dyn_prefix_callable,
        std::vector<Migration> &&migrations, bool no_cache, bool immutable)
    {
        auto py_class = Py_BORROW(base_class);
        auto py_module = Py_OWN(findModule(*Py_OWN(PyObject_GetAttrString((PyObject*)*py_class, "__module__"))));
        if (!py_module || !PyModule_Check(*py_module)) {            
            THROWF(db0::InternalException) << "Type related module not found: " << (*py_class)->tp_name;
        }
        
        if ((*py_class)->tp_dict == nullptr) {
            THROWF(db0::InternalException) << "Type has no tp_dict: " << (*py_class)->tp_name;
        }
        
        if ((*py_class)->tp_itemsize != 0) {
            THROWF(db0::InternalException) << "Variable-length types not supported: " << (*py_class)->tp_name;
        }
        
        auto [type_name, full_type_name] = getMemoTypeName(py_class);
        TypeObjectSharedPtr new_type = nullptr;
        
        // For Python 3.10 compatibility: ensure tp_name string persists beyond this scope
        // by using pooled string to avoid segfault due to tp_name pointer being copied literally
        auto &type_manager = PyToolkit::getTypeManager();
        const char* safe_tp_name = type_manager.getPooledString(full_type_name);
        
        // NOTE: MemoObject and MemoImmutableObject have different implementations
        if (immutable) {
            new_type = Py_OWN(PyMemoType_FromSpec<MemoImmutableObject>(base_class, safe_tp_name, is_singleton));
        } else {
            new_type = Py_OWN(PyMemoType_FromSpec<MemoObject>(base_class, safe_tp_name, is_singleton));
        }
        if (!new_type) {
            return nullptr;
        }
        MemoFlags type_flags = no_default_tags ? MemoFlags { MemoOptions::NO_DEFAULT_TAGS } : MemoFlags();
        if (no_cache) {
            type_flags.set(MemoOptions::NO_CACHE);
        }
        if (immutable) {
            type_flags.set(MemoOptions::IMMUTABLE);
        }
        auto type_info = MemoTypeDecoration(
            py_module,
            prefix_name,
            type_manager.getPooledString(type_id),
            type_manager.getPooledString(file_name),
            std::move(init_vars),
            type_flags,
            py_dyn_prefix_callable,
            std::move(migrations)
        );
        
        // add to memo type registry
        PyToolkit::getTypeManager().addMemoType(*new_type, type_id, std::move(type_info));
        // register new type with the module where the original type was located
        PySafeModule_AddObject(*py_module, type_name.c_str(), new_type);
        
        // add class fields class member to access memo type information
        auto py_class_fields = Py_OWN(PyClassFields_create(*new_type));
        if (PySafeDict_SetItemString((*new_type)->tp_dict, "__fields__", py_class_fields) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to set __fields__");
            return nullptr;
        }
        
        return (PyObject*)new_type.steal();
    }
    
    PyObject *tryWrapPyClass(PyObject *args, PyObject *kwargs)
    {
        PyObject *class_obj = nullptr;
        PyObject *py_singleton = nullptr;
        PyObject *py_no_default_tags = nullptr;
        PyObject *py_prefix_name = nullptr;
        PyObject *py_type_id = nullptr;
        PyObject *py_file_name = nullptr;
        PyObject *py_init_vars = nullptr;
        PyObject *py_dyn_prefix = nullptr;
        // migrations are only processed for singleton types
        PyObject *py_migrations = nullptr;
        PyObject *py_no_cache = nullptr;
        PyObject *py_immutable = nullptr;
        
        static const char *kwlist[] = { "input", "singleton", "no_default_tags", "prefix", "id", "py_file", "py_init_vars", 
            "py_dyn_prefix", "py_migrations", "no_cache", "immutable", NULL };
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOOOOOOOOO", const_cast<char**>(kwlist), &class_obj, &py_singleton,
            &py_no_default_tags, &py_prefix_name, &py_type_id, &py_file_name, &py_init_vars, &py_dyn_prefix, &py_migrations, &py_no_cache, &py_immutable))
        {            
            return NULL;
        }
        
        bool is_singleton = py_singleton && PyObject_IsTrue(py_singleton);
        bool no_default_tags = py_no_default_tags && PyObject_IsTrue(py_no_default_tags);
        bool no_cache = py_no_cache && PyObject_IsTrue(py_no_cache);
        bool immutable = py_immutable && PyObject_IsTrue(py_immutable);
        const char *prefix_name = (py_prefix_name && py_prefix_name != Py_None) ? PyUnicode_AsUTF8(py_prefix_name) : nullptr;
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
        
        if (py_dyn_prefix == Py_None) {
            py_dyn_prefix = nullptr;
        }
        
        // check if py_dyn_prefix is a callable
        if (py_dyn_prefix) {
            if (!PyCallable_Check(py_dyn_prefix)) {
                PyErr_SetString(PyExc_TypeError, "Expected callable: py_dyn_prefix");
                return NULL;
            }
        }
        
        auto migrations = extractMigrations(py_migrations);
        return wrapPyType(castToType(class_obj), is_singleton, no_default_tags, prefix_name, type_id, file_name, 
            std::move(init_vars), py_dyn_prefix, std::move(migrations), no_cache, immutable
        );
    }
    
    PyObject *PyAPI_wrapPyClass(PyObject *, PyObject *args, PyObject *kwargs)
    {
        PY_API_FUNC
        return reinterpret_cast<PyObject*>(runSafe(tryWrapPyClass, args, kwargs));
    }
    
    PyObject *tryPyMemoCheck(PyObject *py_obj)
    {
        if (PyAnyMemo_Check(py_obj) || (PyType_Check(py_obj) && PyAnyMemoType_Check(reinterpret_cast<PyTypeObject*>(py_obj)))) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_FALSE;
    }
    
    PyObject *PyAPI_PyMemo_Check(PyObject *self, PyObject *const * args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "is_memo requires 1 argument");
            return NULL;
        }
        return runSafe(tryPyMemoCheck, args[0]);
    }
    
    bool PyMemoType_IsSingleton(PyTypeObject *type) {
        return type->tp_new == reinterpret_cast<newfunc>(PyAPI_MemoObject_new_singleton);
    }
    
    template <typename MemoImplT>
    PyObject *MemoObject_GetFieldLayout(MemoImplT *self)
    {        
        auto field_layout = self->ext().getFieldLayout();
        auto &pos_vt = field_layout.m_pos_vt_fields;
        auto &index_vt = field_layout.m_index_vt_fields;
        
        // report pos-vt members
        auto py_pos_vt = Py_OWN(PyList_New(pos_vt.size()));
        for (std::size_t i = 0; i < pos_vt.size(); ++i) {
            auto type_name = db0::to_string(pos_vt[i]);
            PySafeList_SetItem(*py_pos_vt, i, Py_OWN(PyUnicode_FromString(type_name.c_str())));
        }
        
        // report index-vt members
        auto py_index_vt = Py_OWN(PyDict_New());
        for (auto &item: index_vt) {
            auto type_name = db0::to_string(item.second);
            PySafeDict_SetItem(*py_index_vt, Py_OWN(PyLong_FromLong(item.first)), 
                Py_OWN(PyUnicode_FromString(type_name.c_str())));
        }
        
        // report kv-index members
        auto py_kv_index = Py_OWN(PyDict_New());
        for (auto &item: field_layout.m_kv_index_fields) {
            auto type_name = db0::to_string(item.second);
            PySafeDict_SetItem(*py_kv_index, Py_OWN(PyLong_FromLong(item.first)), 
                Py_OWN(PyUnicode_FromString(type_name.c_str())));
        }
        
        auto py_result = Py_OWN(PyDict_New());
        if (!py_result) {
            return nullptr;
        }

        PySafeDict_SetItemString(*py_result, "pos_vt", py_pos_vt);
        PySafeDict_SetItemString(*py_result, "index_vt", py_index_vt);
        PySafeDict_SetItemString(*py_result, "kv_index", py_kv_index);
        return py_result.steal();
    }
    
    template <typename MemoImplT>
    PyObject *MemoObject_DescribeObject(MemoImplT *self)
    {
        auto py_field_layout = Py_OWN(MemoObject_GetFieldLayout(self));
        auto py_result = Py_OWN(PyDict_New());
        PySafeDict_SetItemString(*py_result, "field_layout", py_field_layout);
        PySafeDict_SetItemString(*py_result, "uuid", Py_OWN(tryGetUUID(self)));
        PySafeDict_SetItemString(*py_result, "type", Py_OWN(PyUnicode_FromString(self->ext().getType().getName().c_str())));
        PySafeDict_SetItemString(*py_result, "size_of", Py_OWN(PyLong_FromLong(self->ext()->sizeOf())));
        return py_result.steal();
    }
    
    void MemoType_get_info(PyTypeObject *type, PyObject *dict)
    {                      
        auto &decor = MemoTypeDecoration::get(type);
        PySafeDict_SetItemString(dict, "singleton", Py_OWN(PyBool_FromLong(PyMemoType_IsSingleton(type))));
        PySafeDict_SetItemString(dict, "prefix", Py_OWN(PyUnicode_FromString(decor.getPrefixName())));
    }
    
    void MemoType_close(PyTypeObject *type) {
        MemoTypeDecoration::get(type).close();
    }
    
    template <typename MemoImplT>
    PyObject *MemoObject_set_prefix(MemoImplT *py_obj, const char *prefix_name)
    {
        if (prefix_name) {
            using ObjectT = typename MemoImplT::ExtT;
            // can use "ext" since setFixture is a non-mutating operation
            auto &obj = const_cast<ObjectT&>(py_obj->ext());
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(prefix_name, AccessType::READ_WRITE);            
            obj.setFixture(fixture);
            return PyUnicode_FromString(prefix_name);
        }
        Py_RETURN_NONE;
    }
        
    PyObject *tryGetAttributes(PyTypeObject *type)
    {
        auto &decor = MemoTypeDecoration::get(type);
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        auto fixture = workspace.getFixture(decor.getFixtureUUID(), AccessType::READ_ONLY);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // for scoped types, we raise an error if class not found
        if (decor.isScoped()) {
            return tryGetClassAttributes(*class_factory.getExistingType(type));
        } else {
            // otherwise we check the default prefix and also scan other open prefixes
            // in search for the class
            auto type_ptr = class_factory.tryGetExistingType(type);
            workspace.forEachFixture([&](const Fixture &existing_fixture) {
                if (!type_ptr && existing_fixture != *fixture) {
                    auto &class_factory = existing_fixture.get<db0::object_model::ClassFactory>();
                    type_ptr = class_factory.tryGetExistingType(type);
                }
                return !type_ptr;
            });
            if (!type_ptr) {
                THROWF(db0::InputException) << "Class not found: " << PyToolkit::getTypeName(type);
            }
            return tryGetClassAttributes(*type_ptr);
        }
    }
    
    template <typename MemoImplT>
    PyObject *tryGetAttrAs(MemoImplT *memo_obj, PyObject *attr, PyTypeObject *py_type)
    {
        memo_obj->ext().getFixture()->refreshIfUpdated();
        auto member = memo_obj->ext().tryGetAs(PyUnicode_AsUTF8(attr), py_type);
        if (member.get()) {
            return member.steal();
        }

        return PyObject_GenericGetAttr(reinterpret_cast<PyObject*>(memo_obj), attr);
    }
    
    PyObject *getKwargsForMethod(PyObject* method, PyObject* kwargs)
    {
        auto inspect_module = Py_OWN(PyImport_ImportModule("inspect"));
        if (!inspect_module) {
            return nullptr;
        }
        auto signature = Py_OWN(PyObject_CallMethod(*inspect_module, "signature", "O", method));
        if (!signature) {
            return nullptr;
        }

        auto parameters = Py_OWN(PyObject_GetAttrString(*signature, "parameters"));
        if (!parameters) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to get parameters");
            return nullptr;
        }

        auto iterator = Py_OWN(PyObject_GetIter(*parameters));
        if (!iterator) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to get iterator");            
            return nullptr;
        }
        
        ObjectSharedPtr elem;
        auto py_result = Py_OWN(PyDict_New());
        Py_FOR(elem, iterator) {
            if (PyDict_Contains(kwargs, *elem) == 1) {
                PySafeDict_SetItem(*py_result, elem, Py_BORROW(PyDict_GetItem(kwargs, *elem)));
            }
        }
        
        return py_result.steal();
    }
    
    PyObject* executeLoadFunction(PyObject * load_method, PyObject *kwargs, PyObject *py_exclude,
        std::unordered_set<const void*> *load_stack_ptr)
    {
        if (py_exclude != nullptr && py_exclude != Py_None && PySequence_Check(py_exclude)) {
            PyErr_SetString(PyExc_AttributeError, "Cannot exclude values when __load__ is implemented");
            return nullptr;
        }
        
        ObjectSharedPtr result;
        if (kwargs != nullptr) {
            auto method_kwargs = Py_OWN(getKwargsForMethod(load_method, kwargs));
            if (!method_kwargs) {
                return nullptr;
            }
            auto args = Py_OWN(PyTuple_New(0));
            result = Py_OWN(PyObject_Call(load_method, *args, *method_kwargs));
        } else {
            result = Py_OWN(PyObject_CallObject(load_method, nullptr));
        }
        if (!result) {
            return nullptr;
        }
        return tryLoad(*result, kwargs, nullptr, load_stack_ptr);
    }
    
    template <typename MemoImplT>
    PyObject *tryLoadMemo(MemoImplT *memo_obj, PyObject *kwargs, PyObject *py_exclude,
        std::unordered_set<const void*> *load_stack_ptr, bool load_all)
    {
        if (!load_all) {
            // Find custom __load__ method (unless load all requested)
            auto load_method = Py_OWN(tryMemoObject_getattro(memo_obj, *Py_OWN(PyUnicode_FromString("__load__"))));
            if (load_method.get()) {
                return executeLoadFunction(*load_method, kwargs, py_exclude, load_stack_ptr);
            }
        }
        
        // reset Python error
        // FIXME: optimization opportunity if we're able to eliminate error message formatting
        PyErr_Clear();
        
        auto py_result = Py_OWN(PyDict_New());
        bool has_error = false;
        memo_obj->ext().forAll([&py_result, memo_obj, py_exclude, kwargs, &has_error, load_stack_ptr]
            (const std::string &key, PyTypes::ObjectSharedPtr)
        {
            auto key_obj = Py_OWN(PyUnicode_FromString(key.c_str()));
            auto attr = Py_OWN(PyAPI_MemoObject_getattro(memo_obj, *key_obj));
            if (!attr) {
                has_error = true;
                return false;
            }
            
            if (py_exclude == nullptr || py_exclude == Py_None || PySequence_Contains(py_exclude, *key_obj) == 0) {
                auto res = Py_OWN(tryLoad(*attr, kwargs, nullptr, load_stack_ptr, false));
                if (!res) {
                    has_error = true;
                } else {
                    PySafeDict_SetItemString(*py_result, key.c_str(), res);                    
                }
            }
            return !has_error;
        });
        if (has_error) {            
            return nullptr;
        }
        return py_result.steal();
    }
    
    PyObject *tryCompareMemo(MemoObject *py_memo_1, MemoObject *py_memo_2)
    {
        if (py_memo_1 == py_memo_2) {
            Py_RETURN_TRUE;
        }
        
        if (py_memo_1->ext().equalTo(py_memo_2->ext())) {
            Py_RETURN_TRUE;
        }
        Py_RETURN_FALSE;
    }
    
    PyObject *tryGetSchema(PyTypeObject *py_type)
    {
        using SchemaTypeId = db0::object_model::SchemaTypeId;
        
        if (!PyAnyMemoType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "Expected a memo type");
            return nullptr;
        }
        
        auto &decor = MemoTypeDecoration::get(py_type);
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(decor.getFixtureUUID(), AccessType::READ_ONLY);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find py type associated dbzero class with the ClassFactory
        auto type = class_factory.getExistingType(py_type);
        
        auto py_schema = Py_OWN(PyDict_New());
        auto &type_manager = PyToolkit::getTypeManager();
        type->getSchema([&](const std::string &key, SchemaTypeId primary_type, const std::vector<SchemaTypeId> &all_types) {
            auto py_key = Py_OWN(PyUnicode_FromString(key.c_str()));            
            auto py_schema_item = Py_OWN(PyDict_New());
            PySafeDict_SetItemString(*py_schema_item, "primary_type", type_manager.tryGetTypeObject(asNative(getTypeId(primary_type))));
            auto py_all_types = Py_OWN(PyList_New(all_types.size()));
            for (std::size_t i = 0; i < all_types.size(); ++i) {
                auto type_obj = type_manager.tryGetTypeObject(asNative(getTypeId(all_types[i])));
                PySafeList_SetItem(*py_all_types, i, type_obj);
            }
            PySafeDict_SetItemString(*py_schema_item, "all_types", py_all_types);
            PySafeDict_SetItem(*py_schema, py_key, py_schema_item);
        });
        
        return py_schema.steal();
    }
    
    PyObject *PyAPI_getSchema(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {        
        if (nargs != 1) {            
            PyErr_SetString(PyExc_TypeError, "get_schema requires exactly 1 argument");            
            return NULL;
        }

        if (!PyType_Check(args[0])) {
            PyErr_SetString(PyExc_TypeError, "get_schema: type expected");
            return NULL;
        }

        PY_API_FUNC
        return runSafe(tryGetSchema, reinterpret_cast<PyTypeObject*>(args[0]));
    }
    
    bool PyAnyMemoType_Check(PyTypeObject *type)
    {
        assert(type);
        return type->tp_dealloc == reinterpret_cast<destructor>((void(*)(MemoObject*))PyAPI_MemoObject_del<MemoObject>) ||
               type->tp_dealloc == reinterpret_cast<destructor>((void(*)(MemoImmutableObject*))PyAPI_MemoObject_del<MemoImmutableObject>);
    }

    template <typename MemoImplT>
    bool PyMemo_Check(PyObject *obj)
    {
        assert(obj);
        // needs to stay as 2 lines to proper compile on window
        auto expected = reinterpret_cast<destructor>(
            static_cast<void(*)(MemoImplT*)>(&PyAPI_MemoObject_del<MemoImplT>)
        );
        return obj->ob_type->tp_dealloc == expected;        
    }
    
    template <typename MemoImplT>
    bool PyMemoType_Check(PyTypeObject *type)
    {
        assert(type);
        // needs to stay as 2 lines to proper compile on windows
        auto expected = reinterpret_cast<destructor>(
            static_cast<void(*)(MemoImplT*)>(&PyAPI_MemoObject_del<MemoImplT>)
        );
        return type->tp_dealloc == expected;
    }
    
    template bool PyMemo_Check<MemoObject>(PyObject *);
    template bool PyMemo_Check<MemoImmutableObject>(PyObject *);
    template bool PyMemoType_Check<MemoObject>(PyTypeObject *);
    template bool PyMemoType_Check<MemoImmutableObject>(PyTypeObject *);
    template PyObject *MemoObject_DescribeObject<MemoObject>(MemoObject *);
    template PyObject *MemoObject_DescribeObject<MemoImmutableObject>(MemoImmutableObject *);
    template PyObject *MemoObject_set_prefix<MemoObject>(MemoObject *, const char *);
    template PyObject *MemoObject_set_prefix<MemoImmutableObject>(MemoImmutableObject *, const char *);
    template PyObject *tryGetAttrAs<MemoObject>(MemoObject *, PyObject *, PyTypeObject *);
    template PyObject *tryGetAttrAs<MemoImmutableObject>(MemoImmutableObject *, PyObject *, PyTypeObject *);
    template PyObject *tryLoadMemo<MemoObject>(MemoObject *, PyObject*, PyObject*,
        std::unordered_set<const void*> *, bool load_all = false);
    template PyObject *tryLoadMemo<MemoImmutableObject>(MemoImmutableObject *, PyObject*, PyObject*,
        std::unordered_set<const void*> *, bool load_all = false);
    
}
