// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyInternalAPI.hpp"
#include "PyToolkit.hpp"
#include "Memo.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/TypeUtils.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/workspace/WorkspaceView.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/workspace/Utils.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/tags/QueryObserver.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/memory/SlabAllocator.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/workspace/Config.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>

namespace db0::python

{
    
    LoadGuard::LoadGuard(std::unordered_set<const void*> *load_stack_ptr, const void *arg_ptr)
        : m_load_stack_ptr(load_stack_ptr)         
    {
        if (m_load_stack_ptr && m_load_stack_ptr->insert(arg_ptr).second) {
            m_arg_ptr = arg_ptr;
        }
    }
    
    LoadGuard::~LoadGuard()
    {
        if (m_load_stack_ptr && m_arg_ptr) {
            m_load_stack_ptr->erase(m_arg_ptr);
        }            
    }

    ObjectId extractObjectId(PyObject *args)
    {
        // extact ObjectId from args
        PyObject *py_object_id;
        if (!PyArg_ParseTuple(args, "O", &py_object_id)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        if (!ObjectId_Check(py_object_id)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        return *reinterpret_cast<ObjectId*>(py_object_id);
    }
    
    shared_py_object<PyObject*> tryFetchFrom(db0::Snapshot &snapshot, PyObject *py_id, PyTypeObject *type_arg,
        const char *prefix_name)
    {
        assert(py_id);
        if (PyUnicode_Check(py_id)) {
            // fetch by UUID        
            auto uuid = PyUnicode_AsUTF8(py_id);
            return fetchObject(snapshot, ObjectId::fromBase32(uuid), type_arg);
        }

        if (PyType_Check(py_id)) {
            auto id_type = reinterpret_cast<PyTypeObject*>(py_id);
            // check if type_arg is exact or a base of uuid_arg
            if (type_arg && !isBase(id_type, reinterpret_cast<PyTypeObject*>(type_arg))) {
                THROWF(db0::InputException) << "Type mismatch";                                
            }
            return fetchSingletonObject(snapshot, id_type, prefix_name);
        }
        
        THROWF(db0::InputException) << "Invalid argument type" << THROWF_END;
    }
    
    bool tryExistsIn(db0::Snapshot &snapshot, PyObject *py_id, PyTypeObject *type_arg,
        const char *prefix_name)
    {
        assert(py_id);
        if (PyUnicode_Check(py_id)) {
            // exists by UUID
            auto uuid = PyUnicode_AsUTF8(py_id);
            auto object_id = ObjectId::tryFromBase32(uuid);
            if (!object_id) {
                return false;
            }
            return isExistingObject(snapshot, ObjectId::fromBase32(uuid), type_arg);
        }

        if (PyType_Check(py_id)) {
            auto id_type = reinterpret_cast<PyTypeObject*>(py_id);
            // check if type_arg is exact or a base of uuid_arg
            if (type_arg && !isBase(id_type, reinterpret_cast<PyTypeObject*>(type_arg))) {
                return false;
            }
            return isExistingSingleton(snapshot, id_type, prefix_name);
        }
        
        THROWF(db0::InputException) << "Invalid argument type" << THROWF_END;
    }
    
    bool checkObjectIdType(const ObjectId &object_id, PyTypeObject *py_expected_type)
    {
        if (py_expected_type) {
            auto type_id = PyToolkit::getTypeManager().getTypeId(py_expected_type);
            auto pre_storage_class = db0::object_model::TypeUtils::m_storage_class_mapper.getPreStorageClass(type_id, false);
            if (pre_storage_class != db0::getPreStorageClass(object_id.m_storage_class)) {
                return false;
            }
        }
        return true;
    }
    
    bool isExistingObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id, PyTypeObject *py_expected_type)
    {
        using ClassFactory = db0::object_model::ClassFactory;
        using Class = db0::object_model::Class;

        // validate pre-storage class first
        if (py_expected_type && !checkObjectIdType(object_id, py_expected_type)) {
            return false;
        }
        
        auto storage_class = object_id.m_storage_class;
        auto addr = object_id.m_address;
        if (storage_class == db0::object_model::StorageClass::OBJECT_REF) {
            auto &class_factory = db0::object_model::getClassFactory(*fixture);
            // FIXME: this may be sped up if unloading object is avoided
            auto result = PyToolkit::tryUnloadObject(fixture, addr, class_factory, nullptr, addr.getInstanceId());
            if (!result.get()) {
                return false;
            }

            // validate type if requested
            auto &memo = reinterpret_cast<MemoObject*>(result.get())->ext();
            if (py_expected_type) {
                // in other cases the type must match the actual object type
                auto expected_class = class_factory.tryGetExistingType(py_expected_type);
                if (!expected_class) {
                    return false;
                }
                if (memo.getType() != *expected_class) {
                    return false;
                }
            }

            return true;
        } else if (storage_class == db0::object_model::StorageClass::DB0_CLASS) {
            auto &class_factory = db0::object_model::getClassFactory(*fixture);
            return !!class_factory.tryGetTypeByAddr(addr).m_class;
        }

        return false;
    }
    
    shared_py_object<PyObject*> fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id,
        PyTypeObject *py_expected_type)
    {
        using ClassFactory = db0::object_model::ClassFactory;
        using Class = db0::object_model::Class;
        
        // Validate pre-storage class first
        if (py_expected_type && !checkObjectIdType(object_id, py_expected_type)) {
            THROWF(db0::InputException) << "Object ID type mismatch";            
        }
        
        auto storage_class = object_id.m_storage_class;
        auto addr = object_id.m_address;
        if (storage_class == db0::object_model::StorageClass::OBJECT_REF) {
            auto &class_factory = db0::object_model::getClassFactory(*fixture);
            // validate type if requested (no validation for MemoBase)
            if (py_expected_type && !PyToolkit::getTypeManager().isMemoBase(py_expected_type)) {
                // in other cases the type must match the actual object type
                auto expected_class = class_factory.getExistingType(py_expected_type);
                // honor class-specific access flags (e.g. type-level no_cache)
                auto result = PyToolkit::unloadObject(fixture, addr, class_factory, nullptr, addr.getInstanceId(),
                    expected_class->getInstanceFlags()
                );
                auto &memo = reinterpret_cast<MemoObject*>(result.get())->ext();
                if (memo.getType() != *expected_class) {
                    THROWF(db0::InputException) << "Object type mismatch";
                }
                return result;
            } else {
                // unload without type validation
                return PyToolkit::unloadObject(fixture, addr, class_factory, nullptr, addr.getInstanceId());
            }
        } else if (storage_class == db0::object_model::StorageClass::DB0_CLASS) {
            auto &class_factory = db0::object_model::getClassFactory(*fixture);
            auto class_ptr = class_factory.getTypeByAddr(addr).m_class;
            // return as a dbzero class instance
            return makeClass(class_ptr);
        }
        
        THROWF(db0::InputException) << "Invalid object ID" << THROWF_END;
    }
    
    bool tryParseFetchArgs(PyObject *args, PyObject *kwargs, PyObject *&py_id,
        PyObject *&py_type, const char *&prefix_name)
    {
        static const char *kwlist[] = { "identifier", "expected_type", "prefix", NULL };
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Os", const_cast<char**>(kwlist), &py_id, &py_type, &prefix_name)) {
            return false;
        }

        // NOTE: for backwards compatibility, swap parameters if one is a type and the other is UUID
        if (py_id && py_type && PyType_Check(py_id) && PyUnicode_Check(py_type)) {
            std::swap(py_id, py_type);
        }

        if (py_type && !PyType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type: type");
            return false;
        }
        return true;
    }
    
    PyObject *fetchSingletonObject(db0::swine_ptr<Fixture> &fixture, PyTypeObject *py_type)
    {        
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find type associated class with the ClassFactory
        auto type = class_factory.getExistingType(py_type);
        if (!type->isSingleton()) {
            THROWF(db0::InputException) << "Not a dbzero singleton type";
        }

        if (!type->isExistingSingleton()) {
            THROWF(db0::InputException) << "Singleton instance does not exist";
        }

        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        type->unloadSingleton(&memo_obj->modifyExt());
        return memo_obj;
    }
    
    bool isExistingSingleton(db0::swine_ptr<Fixture> &fixture, PyTypeObject *py_type)
    {        
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find type associated class with the ClassFactory
        auto type = class_factory.tryGetExistingType(py_type);
        if (!type || !type->isSingleton()) {
            return false;
        }
        
        return type->isExistingSingleton();
    }

    PyObject *fetchSingletonObject(db0::Snapshot &snapshot, PyTypeObject *py_type, const char *prefix_name)
    {
        // NOTE: singletons only supported for MemoObject types
        if (!PyMemoType_Check<MemoObject>(py_type)) {
            THROWF(db0::InternalException) << "Memo type expected for: " << py_type->tp_name << THROWF_END;
        }
        
        // get either current, scope-related or user requested fixture
        auto maybe_access_type = snapshot.tryGetAccessType();
        db0::swine_ptr<Fixture> fixture;
        if (prefix_name) {
            // try to get fixture by prefix name
            fixture = snapshot.getFixture(prefix_name, maybe_access_type);
        } else {
            fixture = snapshot.getFixture(MemoTypeDecoration::get(py_type).getFixtureUUID(maybe_access_type), 
                maybe_access_type);
        }
        return fetchSingletonObject(fixture, py_type);
    }
    
    bool isExistingSingleton(db0::Snapshot &snapshot, PyTypeObject *py_type, const char *prefix_name)
    {
        // NOTE: singletons only supported for MemoObject types
        if (!PyMemoType_Check<MemoObject>(py_type)) {
            return false;
        }
        
        // get either current, scope-related or user requested fixture
        auto maybe_access_type = snapshot.tryGetAccessType();
        db0::swine_ptr<Fixture> fixture;
        if (prefix_name) {
            // try to get fixture by prefix name
            fixture = snapshot.tryGetFixture(prefix_name, maybe_access_type);
        } else {
            fixture = snapshot.tryGetFixture(MemoTypeDecoration::get(py_type).getFixtureUUID(maybe_access_type), 
                maybe_access_type);
        }
        if (!fixture) {
            return false;
        }
        return isExistingSingleton(fixture, py_type);
    }
        
    void renameMemoClassField(PyTypeObject *py_type, const char *from_name, const char *to_name)
    {        
        using ClassFactory = db0::object_model::ClassFactory;
        auto fixture_uuid = MemoTypeDecoration::get(py_type).getFixtureUUID();

        assert(PyAnyMemoType_Check(py_type));
        assert(from_name);
        assert(to_name);
        
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_WRITE));
        auto &class_factory = lock->get<ClassFactory>();
        // resolve existing DB0 type from python type
        auto type = class_factory.getExistingType(py_type);
        type->renameField(from_name, to_name);
    }
    
#ifndef NDEBUG
    
    PyObject *writeBytes(PyObject *self, PyObject *args)
    {
        // extract string from args
        const char *data;
        if (!PyArg_ParseTuple(args, "s", &data)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        auto addr = db0::writeBytes(*lock, data, strlen(data));
        return PyLong_FromUnsignedLongLong(addr);
    }
    
    PyObject *freeBytes(PyObject *, PyObject *args)
    {
        // extract address from args
        std::uint64_t address;
        if (!PyArg_ParseTuple(args, "K", &address)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        db0::freeBytes(*lock, Address::fromOffset(address));
        Py_RETURN_NONE;
    }

    PyObject *readBytes(PyObject *, PyObject *args)
    {
        // extract address from args
        std::uint64_t address;
        if (!PyArg_ParseTuple(args, "K", &address)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        std::string str_data = db0::readBytes(*lock, Address::fromOffset(address));
        return PyUnicode_FromString(str_data.c_str());
    }
    
#endif

    bool isBase(PyTypeObject *py_type, PyTypeObject *base_type) {
        return PyObject_IsSubclass(reinterpret_cast<PyObject*>(py_type), reinterpret_cast<PyObject*>(base_type));
    }

    shared_py_object<PyObject*> fetchObject(db0::Snapshot &snapshot, ObjectId object_id, PyTypeObject *py_expected_type)
    {        
        auto fixture = snapshot.getFixture(object_id.m_fixture_uuid, AccessType::READ_ONLY);
        assert(fixture);
        fixture->refreshIfUpdated();
        // open from specific fixture
        return fetchObject(fixture, object_id, py_expected_type);
    }

    bool isExistingObject(db0::Snapshot &snapshot, ObjectId object_id, PyTypeObject *py_expected_type)
    {        
        auto fixture = snapshot.getFixture(object_id.m_fixture_uuid, AccessType::READ_ONLY);
        assert(fixture);
        fixture->refreshIfUpdated();        
        return isExistingObject(fixture, object_id, py_expected_type);
    }
    
    PyObject *getSlabMetrics(const db0::SlabAllocator &slab)
    {
        auto py_dict = Py_OWN(PyDict_New());
        PySafeDict_SetItemString(*py_dict, "size", Py_OWN(PyLong_FromUnsignedLong(slab.getSlabSize())));
        PySafeDict_SetItemString(*py_dict, "admin_space_size", Py_OWN(PyLong_FromUnsignedLong(slab.getAdminSpaceSize(true))));
        PySafeDict_SetItemString(*py_dict, "remaining_capacity", Py_OWN(PyLong_FromUnsignedLong(slab.getRemainingCapacity())));
        PySafeDict_SetItemString(*py_dict, "max_alloc_size", Py_OWN(PyLong_FromUnsignedLong(slab.getMaxAllocSize())));
        return py_dict.steal();
    }

    PyObject *tryGetSlabMetrics(db0::Workspace *workspace)
    {
        auto fixture = workspace->getCurrentFixture();        
        auto py_dict = Py_OWN(PyDict_New());
        auto get_slab_metrics = [py_dict](const db0::SlabAllocator &slab, std::uint32_t slab_id) {
            // report remaining capacity as dict item
            auto py_slab_id = Py_OWN(PyLong_FromUnsignedLong(slab_id));
            PySafeDict_SetItem(*py_dict, py_slab_id, Py_OWN(getSlabMetrics(slab)));
        };
        fixture->forAllSlabs(get_slab_metrics);
        return py_dict.steal();
    }

    PyObject *trySetCacheSize(db0::Workspace *workspace, std::size_t new_cache_size) 
    {
        workspace->setCacheSize(new_cache_size);
        Py_RETURN_NONE;
    }
    
    // MEMO_OBJECT specialization
    template <> void dropInstance<TypeId::MEMO_OBJECT>(PyObject *py_wrapper) {
        MemoObject_drop(reinterpret_cast<MemoObject*>(py_wrapper));
    }

    // DB0_INDEX specialization
    template <> void dropInstance<TypeId::DB0_INDEX>(PyObject *py_wrapper) {
        PyWrapper_drop(reinterpret_cast<IndexObject*>(py_wrapper));
    }

    void registerDropInstanceFunctions(std::vector<void (*)(PyObject *)> &functions)
    {
        using TypeId = db0::bindings::TypeId;
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = dropInstance<TypeId::MEMO_OBJECT>;
    }
    
    void dropInstance(db0::bindings::TypeId type_id, PyObject *py_instance)
    {        
        // type-id specializations
        using DropInstanceFunc = void (*)(PyObject *);
        static std::vector<DropInstanceFunc> drop_instance_functions;
        if (drop_instance_functions.empty()) {
            registerDropInstanceFunctions(drop_instance_functions);
        }
        
        assert(static_cast<int>(type_id) < drop_instance_functions.size());
        if (drop_instance_functions[static_cast<int>(type_id)]) {
            drop_instance_functions[static_cast<int>(type_id)](py_instance);
        }
    }
    
    std::uint64_t getTotal(std::pair<std::uint32_t, std::uint32_t> ref_counts, int first_adjuster)
    {
        auto result = ref_counts.first + ref_counts.second;
        // NOTE: this is to hide the auto-assigned references from a type tags        
        if (ref_counts.first > 0) {
            result += first_adjuster;
        }
        return result;
    }

    template <typename MemoImplT>
    PyObject *tryMemoGetRefCount(PyObject *py_object)
    {
        assert(PyMemo_Check<MemoImplT>(py_object));
        auto &memo = reinterpret_cast<MemoImplT*>(py_object)->ext();
        auto fixture = memo.getFixture();
        // NOTE: there might be tag-removal operations buffered for the requested instance
        // in case of a read/write mode conditionally trigger flush in such case
        if (db0::object_model::isObjectPendingUpdate(fixture, memo.getUniqueAddress())) {
            FixtureLock lock(fixture);
            // flush pending updates
            lock->flush();                
        }
        return PyLong_FromLong(getTotal(memo.getRefCounts(), -memo->m_num_type_tags));
    }
    
    PyObject *tryGetRefCount(PyObject *py_object)
    {
        if (PyMemo_Check<MemoObject>(py_object)) {
            return tryMemoGetRefCount<MemoObject>(py_object);
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            return tryMemoGetRefCount<MemoImmutableObject>(py_object);
        } else if (PyClassObject_Check(py_object)) {
            auto ref_counts = reinterpret_cast<ClassObject*>(py_object)->ext().getRefCounts();
            return PyLong_FromLong(getTotal(ref_counts, 0));
        } else if (PyType_Check(py_object)) {
            auto py_type = PyToolkit::getTypeManager().getTypeObject(py_object);
            if (PyToolkit::isAnyMemoType(py_type)) {
                auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
                // sum over all prefixes
                std::uint64_t ref_counts = 0;
                workspace.forEachFixture([&ref_counts, py_type](const db0::Fixture &fixture) {
                    auto type = fixture.get<db0::object_model::ClassFactory>().tryGetExistingType(py_type);
                    if (type) {
                        ref_counts += getTotal(type->getRefCounts(), 0);
                    }
                    return true;
                });
                return PyLong_FromLong(ref_counts);
            }
        }
        THROWF(db0::InputException) << "Unable to retrieve ref count for type: "
            << Py_TYPE(py_object)->tp_name << THROWF_END;
    }
    
    db0::swine_ptr<Fixture> getOptionalPrefixFromArg(db0::Snapshot &workspace, const char *prefix_name) {
        return prefix_name ? workspace.findFixture(prefix_name) : workspace.getCurrentFixture();
    }
    
    db0::swine_ptr<Fixture> getPrefixFromArgs(db0::Snapshot &workspace, PyObject *args,
        PyObject *kwargs, const char *param_name)
    {
        const char *prefix_name = nullptr;
        // optional prefix parameter
        static const char *kwlist[] = { param_name, NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|s", const_cast<char**>(kwlist), &prefix_name)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        return getOptionalPrefixFromArg(workspace, prefix_name);
    }
    
    db0::swine_ptr<Fixture> getPrefixFromArgs(PyObject *args, PyObject *kwargs, const char *param_name) {
        return getPrefixFromArgs(PyToolkit::getPyWorkspace().getWorkspace(), args, kwargs, param_name); 
    }

    db0::swine_ptr<Fixture> tryFindPrefixFromArgs(PyObject *maybe_prefix_name)
    {
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        if (!maybe_prefix_name || maybe_prefix_name == Py_None) {
            return workspace.getCurrentFixture();
        }

        if (!PyUnicode_Check(maybe_prefix_name)) {
            THROWF(db0::InputException) << "Invalid argument type: expected prefix name string";
        }
        auto prefix_name = PyUnicode_AsUTF8(maybe_prefix_name);
        return workspace.tryFindFixture(prefix_name);
    }
    
    std::unique_ptr<BDevStorage> getPrefixStorage(PyObject *maybe_prefix_name,
        std::optional<std::size_t> meta_io_step_size, StorageFlags flags)
    {
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        if (!maybe_prefix_name || !PyUnicode_Check(maybe_prefix_name)) {
            THROWF(db0::InputException) << "Invalid argument type: expected prefix name string";
        }
        auto prefix_name = PyUnicode_AsUTF8(maybe_prefix_name);
        auto &catalog = workspace.getFixtureCatalog();
        auto px_path = catalog.getPrefixFileName(prefix_name).string();
        return std::unique_ptr<BDevStorage>(new BDevStorage(px_path, AccessType::READ_ONLY, {}, meta_io_step_size, flags));
    }
    
    PyObject *tryGetPrefixStats(PyObject *args, PyObject *kwargs)
    {
        auto fixture = getPrefixFromArgs(args, kwargs, "prefix");
        auto stats_dict = Py_OWN(PyDict_New());
        if (!stats_dict) {
            return nullptr;
        }
        
        PySafeDict_SetItemString(*stats_dict, "name", Py_OWN(PyUnicode_FromString(fixture->getPrefix().getName().c_str())));
        PySafeDict_SetItemString(*stats_dict, "uuid", Py_OWN(PyLong_FromLong(fixture->getUUID())));
        PySafeDict_SetItemString(*stats_dict, "dp_size", Py_OWN(PyLong_FromLong(fixture->getPrefix().getPageSize())));

        auto gc0_dict = Py_OWN(PyDict_New());
        if (!gc0_dict) {
            return nullptr;
        }
        PySafeDict_SetItemString(*gc0_dict, "size", Py_OWN(PyLong_FromLong(fixture->getGC0().size())));
        PySafeDict_SetItemString(*stats_dict, "gc0", gc0_dict);

        auto sp_dict = Py_OWN(PyDict_New());
        if (!sp_dict) {
            return nullptr;
        }
        PySafeDict_SetItemString(*sp_dict, "size", Py_OWN(PyLong_FromLong(fixture->getLimitedStringPool().size())));
        PySafeDict_SetItemString(*stats_dict, "string_pool", sp_dict);

        auto cache_dict = Py_OWN(PyDict_New());
        if (!cache_dict) {
            return nullptr;
        }
        fixture->getPrefix().getStats([&](const std::string &name, std::uint64_t value) {
            PySafeDict_SetItemString(*cache_dict, name.c_str(), Py_OWN(PyLong_FromUnsignedLongLong(value)));
        });
        PySafeDict_SetItemString(*stats_dict, "cache", cache_dict);
        return stats_dict.steal();
    }
    
    PyObject *tryGetStorageStats(PyObject *args, PyObject *kwargs)
    {
        auto fixture = getPrefixFromArgs(args, kwargs, "prefix");
        auto stats_dict = Py_OWN(PyDict_New());
        if (!stats_dict) {
            return nullptr;
        }
        auto dirty_size = fixture->getPrefix().getDirtySize();
        // report uncommited data size independently
        PySafeDict_SetItemString(*stats_dict, "dp_size_uncommited", Py_OWN(PyLong_FromUnsignedLongLong(dirty_size)));
        auto stats_callback = [&](const std::string &name, std::uint64_t value) {
            if (name == "dp_size_total") {
                // also include dirty locks stored in-memory
                value += dirty_size;
            }
            PySafeDict_SetItemString(*stats_dict, name.c_str(), Py_OWN(PyLong_FromUnsignedLongLong(value)));
        };
        fixture->getPrefix().getStorage().getStats(stats_callback);
        return stats_dict.steal();
    }
    
#ifndef NDEBUG
    PyObject *formatDRAM_IOMap(const std::unordered_map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t> > &dram_io_map)
    {        
        auto py_dict = Py_OWN(PyDict_New());
        if (!py_dict) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        
        // page_info = std::pair<std::uint64_t, std::uint64_t>
        for (const auto &[page_num, page_info] : dram_io_map) {
            auto py_page_info = Py_OWN(PyTuple_New(2));
            PySafeTuple_SetItem(*py_page_info, 0, Py_OWN(PyLong_FromUnsignedLongLong(page_info.first)));
            PySafeTuple_SetItem(*py_page_info, 1, Py_OWN(PyLong_FromUnsignedLongLong(page_info.second)));
            PySafeDict_SetItem(*py_dict, Py_OWN(PyLong_FromUnsignedLongLong(page_num)), py_page_info);
        }
        return py_dict.steal();
    }
    
    PyObject *tryGetDRAM_IOMap(const Fixture &fixture)
    {
        using DRAM_PageInfo = typename db0::BaseStorage::DRAM_PageInfo;

        std::unordered_map<std::uint64_t, DRAM_PageInfo> dram_io_map;
        fixture.getPrefix().getStorage().getDRAM_IOMap(dram_io_map);
        auto py_dict = Py_OWN(formatDRAM_IOMap(dram_io_map));
        std::vector<db0::BaseStorage::DRAM_CheckResult> dram_check_results;
        fixture.getPrefix().getStorage().dramIOCheck(dram_check_results);
        auto py_check_results = Py_OWN(PyList_New(dram_check_results.size()));
        if (!py_check_results) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        
        for (std::size_t i = 0; i < dram_check_results.size(); ++i) {
            auto py_check_result = Py_OWN(PyDict_New());
            if (!py_check_result) {
                THROWF(db0::MemoryException) << "Out of memory";
            }

            auto &check_result = dram_check_results[i];
            PySafeDict_SetItemString(*py_check_result, "addr", Py_OWN(PyLong_FromUnsignedLongLong(check_result.m_address)));
            PySafeDict_SetItemString(*py_check_result, "page_num", Py_OWN(PyLong_FromUnsignedLongLong(check_result.m_page_num)));
            PySafeDict_SetItemString(*py_check_result, "exp_page_num", Py_OWN(PyLong_FromUnsignedLongLong(check_result.m_expected_page_num)));
            PySafeList_SetItem(*py_check_results, i, py_check_result);
        }

        PySafeDict_SetItemString(*py_dict, "dram_io_discrepancies", py_check_results);
        return py_dict.steal();
    }
    
    PyObject *tryGetDRAM_IOMapFromFile(const char *file_name)
    {
        using DRAM_PageInfo = typename db0::BaseStorage::DRAM_PageInfo;

        db0::BDevStorage storage(file_name, db0::AccessType::READ_ONLY);
        std::unordered_map<std::uint64_t, DRAM_PageInfo> dram_io_map;
        storage.getDRAM_IOMap(dram_io_map);
        return formatDRAM_IOMap(dram_io_map);
    }    
#endif

    bool hasKWArg(PyObject *kwargs, const char *name) {
        return kwargs && PyDict_Contains(kwargs, *Py_OWN(PyUnicode_FromString(name)));
    }
    
    PyObject *tryGetAddress(PyObject *py_obj)
    {
        if (PyAnyMemo_Check(py_obj)) {
            return PyLong_FromUnsignedLongLong(
                reinterpret_cast<MemoAnyObject*>(py_obj)->ext().getAddress().getValue()
            );
        }
        
        // FIXME: implement for other dbzero types
        THROWF(db0::InputException) << "Unable to retrieve address for type: "
            << Py_TYPE(py_obj)->tp_name << THROWF_END;
    }
    
    PyTypeObject *tryGetType(PyObject *py_obj)
    {
        if (PyAnyMemo_Check(py_obj)) {
            auto &memo = reinterpret_cast<MemoAnyObject*>(py_obj)->ext();
            auto fixture = memo.getFixture();
            auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
            if (!class_factory.hasLangType(memo.getType())) {
                THROWF(db0::ClassNotFoundException) << "Could not find type: " <<memo.getType().getName();
            }
            return class_factory.getLangType(memo.getType()).steal();
        }
        auto py_type = Py_TYPE(py_obj);
        Py_INCREF(py_type);
        return py_type;
    }
    
    PyObject *tryGetMemoTypeInfo(PyObject *py_object)
    {
        if (!PyType_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        PyTypeObject *py_type = reinterpret_cast<PyTypeObject*>(py_object);
        if (!PyAnyMemoType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return nullptr;
        }

        auto py_dict = Py_OWN(PyDict_New());
        if (!py_dict) {
            return nullptr;
        }
        
        MemoType_get_info(py_type, *py_dict);
        return py_dict.steal();        
    }
    
    PyObject *tryGetMemoClass(PyObject *py_obj)
    {
        if (!PyAnyMemo_Check(py_obj)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return nullptr;
        }
        auto &memo_obj = reinterpret_cast<MemoAnyObject*>(py_obj)->ext();
        if (!memo_obj.hasInstance()) {
            PyErr_SetString(PyExc_RuntimeError, "Memo object has no instance");
            return nullptr;
        }
        return tryGetTypeInfo(memo_obj.getType());
    }
    
    PyObject *tryLoad(PyObject *py_obj, PyObject* kwargs, PyObject *py_exclude,
        std::unordered_set<const void*> *load_stack_ptr, bool load_all)
    {
        LoadGuard _load_guard(load_stack_ptr, py_obj);
        if (!_load_guard) {
            PyErr_SetString(PyExc_RecursionError, "Recursive loading detected");
            return nullptr;
        }
        
        using TypeId = db0::bindings::TypeId;
        auto &type_manager = PyToolkit::getTypeManager();
        auto type_id_result = type_manager.tryGetTypeId(py_obj);
        if (!type_id_result.has_value() || type_id_result.value() == TypeId::UNKNOWN) {
            if (!load_all) {
                auto load_func = Py_OWN(PyObject_GetAttrString(py_obj, "__load__"));
                if (load_func.get()) {
                    if (PyCallable_Check(*load_func)) {
                        return executeLoadFunction(*load_func, kwargs, py_exclude, load_stack_ptr);
                    }
                }                
            }
            // return unknown types as-is (it's not a dbzero object)
            Py_INCREF(py_obj);
            return py_obj;
        }
        auto type_id = type_id_result.value();
        if (type_manager.isSimplePyTypeId(type_id)) {
            // no conversion needed for simple python types
            Py_INCREF(py_obj);
            return py_obj;
        }
        if (type_id == TypeId::DB0_TUPLE) {
            return tryLoadTuple(reinterpret_cast<TupleObject*>(py_obj), kwargs, load_stack_ptr);
        } else if (type_id == TypeId::TUPLE) {
            // regular Python tuple
            return tryLoadPyTuple(py_obj, kwargs, load_stack_ptr);
        } else if (type_id == TypeId::DB0_LIST) {
            return tryLoadList(reinterpret_cast<ListObject*>(py_obj), kwargs, load_stack_ptr);
        } else if (type_id == TypeId::LIST) {
            // regular Python list
            return tryLoadPyList(py_obj, kwargs, load_stack_ptr);
        } else if (type_id == TypeId::DB0_DICT || type_id == TypeId::DICT) {
            return tryLoadDict(py_obj, kwargs, load_stack_ptr);
        } else if (type_id == TypeId::DB0_SET || type_id == TypeId::SET) {
            return tryLoadSet(py_obj, kwargs, load_stack_ptr);
        } else if (type_id == TypeId::DB0_ENUM_VALUE) {
            return tryLoadEnumValue(reinterpret_cast<PyEnumValue*>(py_obj));
        } else if (type_id == TypeId::MEMO_OBJECT) {
            return tryLoadMemo(reinterpret_cast<MemoObject*>(py_obj), kwargs, py_exclude, load_stack_ptr, load_all);
        } else if (type_id == TypeId::MEMO_IMMUTABLE_OBJECT) {
            return tryLoadMemo(reinterpret_cast<MemoImmutableObject*>(py_obj), kwargs, py_exclude, load_stack_ptr, load_all);
        } else {
            THROWF(db0::InputException) << "__load__ not implemented for type: " 
                << Py_TYPE(py_obj)->tp_name << THROWF_END;
        }
    }
    
    template <typename MemoImplT>
    PyObject *getMaterializedMemoObject(MemoImplT *memo_obj)
    {
        if (memo_obj->ext().hasInstance()) {
            Py_INCREF(memo_obj);
            return memo_obj;
        }
        
        db0::FixtureLock lock(memo_obj->ext().getFixture());
        // materialize by calling postInit
        memo_obj->modifyExt().postInit(lock);
        Py_INCREF(memo_obj);
        return memo_obj;
    }
    
    shared_py_object<PyObject*> tryUnloadObjectFromCache(LangCacheView &lang_cache, Address address,
        std::shared_ptr<db0::object_model::Class> expected_type)
    {        
        auto obj_ptr = lang_cache.get(address);
        if (!obj_ptr.get()) {
            // not found in cache
            return nullptr;
        }
        
        if (expected_type) {
            if (!PyAnyMemo_Check(obj_ptr.get())) {
                THROWF(db0::InputException) << "Invalid object type: " << PyToolkit::getTypeName(obj_ptr.get()) << " (Memo expected)";
            }
            auto &memo = reinterpret_cast<MemoAnyObject*>(obj_ptr.get())->ext();
            // validate type
            if (memo.getType() != *expected_type) {
                THROWF(db0::InputException) << "Memo type mismatch";
            }
        }
        return obj_ptr;
    }
    
    PyObject *tryMemoObject_open_singleton(PyTypeObject *py_type, const Fixture &fixture)
    {
        auto &class_factory = fixture.get<db0::object_model::ClassFactory>();
        // find py type associated dbzero class with the ClassFactory
        auto type = class_factory.tryGetExistingType(py_type);
        
        if (!type) {
            return nullptr;
        }
        
        auto addr = type->getSingletonAddress();
        if (!addr) {
            return nullptr;
        }
        
        // try unloading from cache first
        auto &lang_cache = fixture.getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, addr);
        
        if (obj_ptr.get()) {
            return obj_ptr.steal();
        }
        
        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        // try unloading associated singleton if such exists
        if (!type->unloadSingleton(&memo_obj->modifyExt())) {
            py_type->tp_dealloc(memo_obj);
            return nullptr;
        }

        // once unloaded, check if the singleton needs migration
        auto &decor = MemoTypeDecoration::get(py_type);
        if (decor.hasMigrations()) {
            auto members = memo_obj->ext().getMembers();
            std::unordered_set<Migration*> migrations;
            PyObject *py_result = nullptr;
            bool exec_migrate = false;
            // for all missing members, execute migrations in order
            decor.forAllMigrations(members, [&](Migration &migration) {
                // execute migration once
                // one migration may be associated with multiple members
                if (migrations.insert(&migration).second) {
                    exec_migrate = true;
                    py_result = migration.exec(memo_obj);                    
                    if (!py_result) {
                        return false;
                    }
                }
                return true;
            });
            
            if (exec_migrate && !py_result) {
                // migrate exec failed, return with error set
                py_type->tp_dealloc(memo_obj);
                return py_result;
            }
        }
        
        // add singleton to cache
        lang_cache.add(addr, memo_obj);
        return memo_obj;
    }

    PyObject *tryAssign(PyObject *targets, PyObject *key_values)
    {
        using ObjectSharedPtr = PyTypes::ObjectSharedPtr;

        auto num_targets = PyTuple_Size(targets);
        for (Py_ssize_t i = 0; i < num_targets; ++i) {
            auto target = PyTuple_GetItem(targets, i);

            auto items = Py_OWN(PyDict_Items(key_values));
            if (!items) {
                return nullptr;
            }
            
            auto iter = Py_OWN(PyObject_GetIter(*items));
            if (!iter) {
                return nullptr;
            }

            ObjectSharedPtr item;
            Py_FOR(item, iter) {
                // item is a tuple of (key, value)
                if (!PyTuple_Check(*item) || PyTuple_Size(*item) != 2) {
                    PyErr_SetString(PyExc_TypeError, "Dictionary items must be tuples of (key, value)");
                    return nullptr;
                }

                PyObject *key = PyTuple_GetItem(*item, 0);
                PyObject *value = PyTuple_GetItem(*item, 1);
                
                // invoke __setattr__ on object
                if (!PyUnicode_Check(key)) {
                    PyErr_SetString(PyExc_TypeError, "Dictionary keys must be strings");
                    return nullptr;
                }

                // set the attribute on the object
                if (PyObject_SetAttr(target, key, value) < 0) {
                    PyErr_Format(PyExc_RuntimeError, "Failed to set attribute '%s'", PyUnicode_AsUTF8(key));
                    return nullptr;
                }
            }
        }

        Py_RETURN_NONE;
    }

    PyObject *tryTouch(PyObject *const *args, Py_ssize_t nargs)
    {
        for (Py_ssize_t i = 0; i < nargs; ++i) {
            auto py_obj = args[i];
            if (!PyAnyMemo_Check(py_obj)) {
                THROWF(db0::InputException) << "Invalid object type: " << Py_TYPE(py_obj)->tp_name << " (Memo expected)";
            }
            auto &memo = reinterpret_cast<MemoAnyObject*>(py_obj)->modifyExt();
            if (memo.hasInstance()) {
                db0::FixtureLock lock(memo.getFixture());
                memo.touch();
            }
        }
        
        Py_RETURN_NONE;
    }

    PyObject *tryCopyPrefixImpl(BDevStorage &src_storage, const std::string &output_file_name,
        std::optional<std::uint64_t> page_io_step_size, std::optional<std::uint64_t> meta_io_step_size) 
    {
        // make sure output is file doesn't point to a directory
        if (output_file_name.back() == std::filesystem::path::preferred_separator) {
            PyErr_Format(PyExc_OSError, "Output file points to a directory:  '%s'", output_file_name.c_str());
            return nullptr;
        }
        // make sure output file does not exist
        if (db0::CFile::exists(output_file_name)) {
            PyErr_Format(PyExc_OSError, "Output file already exists:  '%s'", output_file_name.c_str());
            return nullptr;
        }
        
        // use either explicit step size, input step size (if > 1) or default = 4MB
        if (!page_io_step_size) {
            auto &page_io = src_storage.getPageIO();
            auto in_step_size =  page_io.getStepSize();
            page_io_step_size = in_step_size > 1 ? (in_step_size * page_io.getBlockSize()) : (4u << 20);
        }
        
        if (!meta_io_step_size) {
            auto in_meta_step_size = src_storage.getMetaIO().getStepSize();
            meta_io_step_size = in_meta_step_size > 1 ? in_meta_step_size : (1u << 20);
        }
        
        try {
            BDevStorage::create(output_file_name, src_storage.getPageSize(), src_storage.getDRAMPageSize(), page_io_step_size);
            BDevStorage out(output_file_name, db0::AccessType::READ_WRITE);
            // copy entire prefix
            src_storage.copyTo(out);
            out.close();
        } catch (...) {
            // cleanup
            try {
                if (db0::CFile::exists(output_file_name)) {
                    db0::CFile::remove(output_file_name);
                }
            } catch (...) {
                // ignore cleanup errors
            }
            throw;
        }

        Py_RETURN_NONE;
    }
    
    PyObject *tryCopyPrefix(PyObject *args, PyObject *kwargs)
    {
        // arguments: prefix, output
        PyObject *py_prefix = nullptr;
        PyObject *py_output = nullptr;
        PyObject *py_page_io_step_size = nullptr;
        PyObject *py_meta_io_step_size = nullptr;
        static const char *kwlist[] = {"output", "prefix", "page_io_step_size", "meta_io_step_size", NULL};
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOO", const_cast<char**>(kwlist), &py_output, &py_prefix, 
            &py_page_io_step_size, &py_meta_io_step_size)) 
        {
            return NULL;
        }
        
        // retrieve output file name
        std::string output_file_name;
        if (PyUnicode_Check(py_output)) {
            output_file_name = PyUnicode_AsUTF8(py_output);
        } else {
            PyErr_SetString(PyExc_TypeError, "Invalid output argument type");
            return NULL;
        }
        
        std::optional<std::uint64_t> page_io_step_size;
        if (py_page_io_step_size && py_page_io_step_size != Py_None) {
            if (PyLong_Check(py_page_io_step_size)) {
                page_io_step_size = PyLong_AsUnsignedLongLong(py_page_io_step_size);
            } else {
                PyErr_SetString(PyExc_TypeError, "Invalid page_io_step_size argument type");
                return NULL;
            }
        }

        std::optional<std::uint64_t> meta_io_step_size;
        if (py_meta_io_step_size && py_meta_io_step_size != Py_None) {
            if (PyLong_Check(py_meta_io_step_size)) {
                meta_io_step_size = PyLong_AsUnsignedLongLong(py_meta_io_step_size);
            } else {
                PyErr_SetString(PyExc_TypeError, "Invalid meta_io_step_size argument type");
                return NULL;
            }
        }
        
        std::unique_ptr<BDevStorage> storage;
        try {
            auto prefix = tryFindPrefixFromArgs(py_prefix); 
            StorageFlags flags = { StorageOptions::NO_LOAD };
            if (prefix) {
                // open as a copy of an existing prefix
                auto &in = prefix->getPrefix().getStorage().asFile();
                storage = std::unique_ptr<BDevStorage>(new BDevStorage(
                    in.getFileName(), AccessType::READ_ONLY, {}, in.getMetaIO().getStepSize(), flags)
                );
            } else {
                // NOTE: for copy we open the storage as NO_LOAD
                storage = getPrefixStorage(py_prefix, meta_io_step_size, flags);
            }
            auto result = Py_OWN(tryCopyPrefixImpl(*storage, output_file_name, page_io_step_size, meta_io_step_size));
            storage->close();
            return result.steal();
        } catch (...) {
            if (storage) {
                storage->close();
            }
            throw;
        }
    }
    
#ifndef NDEBUG
    PyObject *trySetTestParams(PyObject *py_dict)
    {
        db0::Config config(py_dict);
        if (config.hasKey("sleep_interval")) {
            db0::Settings::__sleep_interval = config.get<unsigned long long>("sleep_interval", 0);            
        }
        if (config.hasKey("write_poison")) {
            db0::Settings::__write_poison = config.get<unsigned int>("write_poison", 0);
        }
        if (config.hasKey("dram_io_flush_poison")) {
            db0::Settings::__dram_io_flush_poison = config.get<unsigned int>("dram_io_flush_poison", 0);   
        }
        Py_RETURN_NONE;
    }
    
    PyObject *tryResetTestParams()
    {        
        db0::Settings::reset();
        Py_RETURN_NONE;
    }
#endif

    template PyObject *getMaterializedMemoObject(MemoObject *);
    template PyObject *getMaterializedMemoObject(MemoImmutableObject *);
    
}