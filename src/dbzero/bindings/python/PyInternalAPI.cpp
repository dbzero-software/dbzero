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
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>

namespace db0::python

{
    
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
    
    shared_py_object<PyObject*> tryFetchFrom(db0::Snapshot &snapshot, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs < 1 || nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "fetch requires 1 or 2 arguments");
            return NULL;
        }

        PyTypeObject *type_arg = nullptr;
        PyObject *uuid_arg = nullptr;
        if (nargs == 1) {
            uuid_arg = args[0];
        } else {
            if (!PyType_Check(args[0])) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type");
                return nullptr;
            }
            type_arg = reinterpret_cast<PyTypeObject*>(args[0]);
            uuid_arg = args[1];
        }
        
        // decode ObjectId from string
        if (PyUnicode_Check(uuid_arg)) {
            auto uuid = PyUnicode_AsUTF8(uuid_arg);
            return fetchObject(snapshot, ObjectId::fromBase32(uuid), type_arg);
        }
        
        if (PyType_Check(uuid_arg)) {
            auto uuid_type = reinterpret_cast<PyTypeObject*>(uuid_arg);
            // check if type_arg is exact or a base of uuid_arg
            if (type_arg && !isBase(uuid_type, reinterpret_cast<PyTypeObject*>(type_arg))) {
                THROWF(db0::InputException) << "Type mismatch";                                
            }
            return fetchSingletonObject(snapshot, uuid_type);
        }

        THROWF(db0::InputException) << "Invalid argument type" << THROWF_END;
    }
    
    shared_py_object<PyObject*> fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id,
        PyTypeObject *py_expected_type)
    {   
        using ClassFactory = db0::object_model::ClassFactory;
        using Class = db0::object_model::Class;

        auto storage_class = object_id.m_typed_addr.getType();
        // use logical address to access the object
        auto addr = db0::makeLogicalAddress(object_id.m_typed_addr, object_id.m_instance_id);
        
        // validate storage class first
        if (py_expected_type) {
            auto type_id = PyToolkit::getTypeManager().getTypeId(py_expected_type);
            if (storage_class != db0::object_model::TypeUtils::m_storage_class_mapper.getStorageClass(type_id)) {
                THROWF(db0::InputException) << "Object ID type mismatch";
            }
        }
        
        if (storage_class == db0::object_model::StorageClass::OBJECT_REF) {
            auto &class_factory = fixture->get<ClassFactory>();
            // validate type if requested
            if (py_expected_type) {
                // unload as MemoBase
                if (PyToolkit::getTypeManager().isMemoBase(py_expected_type)) {
                    return PyToolkit::unloadObject(fixture, addr, class_factory, py_expected_type);
                }

                // in other cases the type must match the actual object type
                auto expected_class = class_factory.getExistingType(py_expected_type);
                auto result = PyToolkit::unloadObject(fixture, addr, class_factory);
                if (reinterpret_cast<MemoObject*>(result.get())->ext().getType() != *expected_class) {
                    THROWF(db0::InputException) << "Object type mismatch";
                }
                return result;
            } else {
                return PyToolkit::unloadObject(fixture, addr, class_factory);
            }
        } else if (storage_class == db0::object_model::StorageClass::DB0_LIST) {
            // unload by logical address
            return PyToolkit::unloadList(fixture, addr);
        } else if (storage_class == db0::object_model::StorageClass::DB0_DICT) {
            // unload by logical address
            return PyToolkit::unloadDict(fixture, addr);
        } else if (storage_class == db0::object_model::StorageClass::DB0_SET) {
            // unload by logical address
            return PyToolkit::unloadSet(fixture, addr);
        } else if (storage_class == db0::object_model::StorageClass::DB0_TUPLE) {
            // unload by logical address
            return PyToolkit::unloadTuple(fixture, addr);
        } else if (storage_class == db0::object_model::StorageClass::DB0_INDEX) {
            // unload by logical address
            return PyToolkit::unloadIndex(fixture, addr);
        } else if (storage_class == db0::object_model::StorageClass::DB0_CLASS) {
            auto &class_factory = fixture->get<ClassFactory>();            
            auto class_ptr = class_factory.getTypeByClassRef(addr).m_class;
            // return as a dbzero class instance
            return makeClass(class_ptr);
        }
        
        THROWF(db0::InputException) << "Invalid object ID" << THROWF_END;
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

    PyObject *fetchSingletonObject(db0::Snapshot &snapshot, PyTypeObject *py_type)
    {
        if (!PyMemoType_Check(py_type)) {
            THROWF(db0::InternalException) << "Memo type expected for: " << py_type->tp_name << THROWF_END;
        }

        // get either curren fixture or a scope-related fixture
        auto fixture = snapshot.getFixture(MemoTypeDecoration::get(py_type).getFixtureUUID());
        return fetchSingletonObject(fixture, py_type);
    }
    
    void renameField(PyTypeObject *py_type, const char *from_name, const char *to_name)
    {        
        using ClassFactory = db0::object_model::ClassFactory;
        auto fixture_uuid = MemoTypeDecoration::get(py_type).getFixtureUUID();

        assert(PyMemoType_Check(py_type));
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
        db0::freeBytes(*lock, address);
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
        std::string str_data = db0::readBytes(*lock, address);
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
    
    PyObject *findIn(db0::Snapshot &snapshot, PyObject* const *args, Py_ssize_t nargs)
    {
        using ObjectIterable = db0::object_model::ObjectIterable;
        using TagIndex = db0::object_model::TagIndex;
        using Class = db0::object_model::Class;        
        
        std::vector<PyObject*> find_args;
        bool no_result = false;
        std::shared_ptr<Class> type;
        PyTypeObject *lang_type = nullptr;
        auto fixture = db0::object_model::getFindParams(snapshot, args, nargs, find_args, type, lang_type, no_result);
        fixture->refreshIfUpdated();
        auto &tag_index = fixture->get<TagIndex>();
        std::vector<std::unique_ptr<db0::object_model::QueryObserver> > query_observers;
        auto query_iterator = tag_index.find(find_args.data(), find_args.size(), type, query_observers, no_result);
        auto iter_obj = PyObjectIterableDefault_new();
        ObjectIterable::makeNew(&(iter_obj.get())->modifyExt(), fixture, std::move(query_iterator), type,
            lang_type, std::move(query_observers));
        return iter_obj.steal();
    }
    
    PyObject *trySerialize(PyObject *py_serializable)
    {
        using TypeId = db0::bindings::TypeId;
        std::vector<std::byte> bytes;
        auto type_id = PyToolkit::getTypeManager().getTypeId(py_serializable);
        db0::serial::write(bytes, type_id);
        
        if (type_id == TypeId::OBJECT_ITERABLE) {
            reinterpret_cast<PyObjectIterable*>(py_serializable)->ext().serialize(bytes);
        } else {
            THROWF(db0::InputException) << "Unsupported or non-serializable type: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
                
        return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    
    PyObject *tryDeserialize(db0::Snapshot *workspace, PyObject *py_bytes)
    {
        using TypeId = db0::bindings::TypeId;

        if (!PyBytes_Check(py_bytes)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type (expected bytes)");
            return NULL;
        }

        Py_ssize_t size;
        char *data = nullptr;
        PyBytes_AsStringAndSize(py_bytes, &data, &size);
        // extract bytes
        std::vector<std::byte> bytes(size);
        std::copy(data, data + size, reinterpret_cast<char*>(bytes.data()));
        
        auto fixture = workspace->getCurrentFixture();
        auto iter = bytes.cbegin(), end = bytes.cend();
        auto type_id = db0::serial::read<TypeId>(iter, end);
        if (type_id == TypeId::OBJECT_ITERABLE) {
            return PyToolkit::unloadObjectIterable(fixture, iter, end).steal();
        } else {
            THROWF(db0::InputException) << "Unsupported serialized type id: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
    }
    
    PyObject *getSlabMetrics(const db0::SlabAllocator &slab)
    {        
        PyObject *py_dict = PyDict_New();
        PyDict_SetItemString(py_dict, "size", PyLong_FromUnsignedLong(slab.getSlabSize()));
        PyDict_SetItemString(py_dict, "admin_space_size", PyLong_FromUnsignedLong(slab.getAdminSpaceSize(true)));
        PyDict_SetItemString(py_dict, "remaining_capacity", PyLong_FromUnsignedLong(slab.getRemainingCapacity()));
        PyDict_SetItemString(py_dict, "max_alloc_size", PyLong_FromUnsignedLong(slab.getMaxAllocSize()));
        return py_dict;
    }

    PyObject *tryGetSlabMetrics(db0::Workspace *workspace)
    {
        auto fixture = workspace->getCurrentFixture();        
        PyObject *py_dict = PyDict_New();
        auto get_slab_metrics = [py_dict](const db0::SlabAllocator &slab, std::uint32_t slab_id) {
            // report remaining capacity as dict item
            PyObject *py_slab_id = PyLong_FromUnsignedLong(slab_id);
            PyDict_SetItem(py_dict, py_slab_id, getSlabMetrics(slab));
        };
        fixture->forAllSlabs(get_slab_metrics);
        return py_dict;
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
    
    PyObject *tryGetRefCount(PyObject *py_object)
    {
        if (PyMemo_Check(py_object)) {
            return PyLong_FromLong(reinterpret_cast<MemoObject*>(py_object)->ext().getRefCount());
        } else if (PyClassObject_Check(py_object)) {
            return PyLong_FromLong(reinterpret_cast<ClassObject*>(py_object)->ext().getRefCount());
        } else if (PyType_Check(py_object)) {
            auto py_type = PyToolkit::getTypeManager().getTypeObject(py_object);
            if (PyToolkit::isMemoType(py_type)) {
                auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
                // sum over all prefixes
                std::uint64_t ref_count = 0;                
                workspace.forEachFixture([&ref_count, py_type](const db0::Fixture &fixture) {
                    auto type = fixture.get<db0::object_model::ClassFactory>().tryGetExistingType(py_type);
                    if (type) {
                        ref_count += type->getRefCount();                        
                    }
                    return true;
                });
                return PyLong_FromLong(ref_count);
            }
        }
        THROWF(db0::InputException) << "Unable to retrieve ref count for type: "
            << Py_TYPE(py_object)->tp_name << THROWF_END;
    }

    db0::swine_ptr<Fixture> getOptionalPrefixFromArg(db0::Snapshot &workspace, const char *prefix_name)
    {
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
    
    PyObject *tryGetPrefixStats(PyObject *args, PyObject *kwargs)
    {
        auto fixture = getPrefixFromArgs(args, kwargs, "prefix");
        auto stats_dict = PyDict_New();
        if (!stats_dict) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        
        PyDict_SetItemString(stats_dict, "name", PyUnicode_FromString(fixture->getPrefix().getName().c_str()));
        PyDict_SetItemString(stats_dict, "uuid", PyLong_FromLong(fixture->getUUID()));
        PyDict_SetItemString(stats_dict, "dp_size", PyLong_FromLong(fixture->getPrefix().getPageSize()));
        auto gc0_dict = PyDict_New();
        if (!gc0_dict) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        PyDict_SetItemString(gc0_dict, "size", PyLong_FromLong(fixture->getGC0().size()));
        PyDict_SetItemString(stats_dict, "gc0", gc0_dict);
        auto sp_dict = PyDict_New();
        if (!sp_dict) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        PyDict_SetItemString(sp_dict, "size", PyLong_FromLong(fixture->getLimitedStringPool().size()));
        PyDict_SetItemString(stats_dict, "string_pool", sp_dict);
        auto cache_dict = PyDict_New();
        if (!cache_dict) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        fixture->getPrefix().getStats([&](const std::string &name, std::uint64_t value) {
            PyDict_SetItemString(cache_dict, name.c_str(), PyLong_FromUnsignedLongLong(value));
        });
        PyDict_SetItemString(stats_dict, "cache", cache_dict);
        return stats_dict;
    }
    
    PyObject *tryGetStorageStats(PyObject *args, PyObject *kwargs)
    {
        auto fixture = getPrefixFromArgs(args, kwargs, "prefix");
        auto stats_dict = PyDict_New();
        if (!stats_dict) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        auto dirty_size = fixture->getPrefix().getDirtySize();
        // report uncommited data size independently
        PyDict_SetItemString(stats_dict, "dp_size_uncommited", PyLong_FromUnsignedLongLong(dirty_size));
        auto stats_callback = [&](const std::string &name, std::uint64_t value) {
            if (name == "dp_size_total") {
                // also include dirty locks stored in-memory
                value += dirty_size;
            }
            PyDict_SetItemString(stats_dict, name.c_str(), PyLong_FromUnsignedLongLong(value));
        };
        fixture->getPrefix().getStorage().getStats(stats_callback);
        return stats_dict;
    }
    
#ifndef NDEBUG
    PyObject *formatDRAM_IOMap(const std::unordered_map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t> > &dram_io_map)
    {        
        PyObject *py_dict = PyDict_New();
        if (!py_dict) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        // page_info = std::pair<std::uint64_t, std::uint64_t>
        for (const auto &[page_num, page_info] : dram_io_map) {
            PyObject *py_page_info = PyTuple_New(2);
            PyTuple_SetItem(py_page_info, 0, PyLong_FromUnsignedLongLong(page_info.first));
            PyTuple_SetItem(py_page_info, 1, PyLong_FromUnsignedLongLong(page_info.second));
            PyDict_SetItem(py_dict, PyLong_FromUnsignedLongLong(page_num), py_page_info);
        }
        return py_dict;
    }
    
    PyObject *tryGetDRAM_IOMap(const Fixture &fixture)
    {
        using DRAM_PageInfo = typename db0::BaseStorage::DRAM_PageInfo;

        std::unordered_map<std::uint64_t, DRAM_PageInfo> dram_io_map;
        fixture.getPrefix().getStorage().getDRAM_IOMap(dram_io_map);
        auto py_dict = formatDRAM_IOMap(dram_io_map);
        std::vector<db0::BaseStorage::DRAM_CheckResult> dram_check_results;
        fixture.getPrefix().getStorage().dramIOCheck(dram_check_results);
        PyObject *py_check_results = PyList_New(dram_check_results.size());
        if (!py_check_results) {
            THROWF(db0::MemoryException) << "Out of memory";
        }
        for (std::size_t i = 0; i < dram_check_results.size(); ++i) {
            PyObject *py_check_result = PyDict_New();
            if (!py_check_result) {
                THROWF(db0::MemoryException) << "Out of memory";
            }
            auto &check_result = dram_check_results[i];
            PyDict_SetItemString(py_check_result, "addr", PyLong_FromUnsignedLongLong(check_result.m_address));
            PyDict_SetItemString(py_check_result, "page_num", PyLong_FromUnsignedLongLong(check_result.m_page_num));
            PyDict_SetItemString(py_check_result, "exp_page_num", PyLong_FromUnsignedLongLong(check_result.m_expected_page_num));
            PyList_SetItem(py_check_results, i, py_check_result);
        }

        PyDict_SetItemString(py_dict, "dram_io_discrepancies", py_check_results);
        return py_dict;
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
        return kwargs && PyDict_Contains(kwargs, PyUnicode_FromString(name));
    }
    
    PyObject *tryGetAddress(PyObject *py_obj)
    {
        if (PyMemo_Check(py_obj)) {
            return PyLong_FromUnsignedLongLong(reinterpret_cast<MemoObject*>(py_obj)->ext().getAddress());
        }
        
        // FIXME: implement for other dbzero types
        THROWF(db0::InputException) << "Unable to retrieve address for type: "
            << Py_TYPE(py_obj)->tp_name << THROWF_END;
    }
    
    PyTypeObject *tryGetType(PyObject *py_obj)
    {
        if (PyMemo_Check(py_obj)) {
            auto &memo = reinterpret_cast<MemoObject*>(py_obj)->ext();
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
    
    PyObject *tryLoad(PyObject *py_obj, PyObject* kwargs, PyObject *py_exclude)
    {
        using TypeId = db0::bindings::TypeId;
        auto &type_manager = PyToolkit::getTypeManager();
        auto type_id = type_manager.getTypeId(py_obj);
        if (type_manager.isSimplePyTypeId(type_id)) {
            // no conversion needed for simple python types
            Py_INCREF(py_obj);
            return py_obj;
        }

        // FIXME: implement for other types
        if (type_id == TypeId::DB0_TUPLE) {
            return tryLoadTuple(reinterpret_cast<TupleObject*>(py_obj), kwargs);
        } else if (type_id == TypeId::TUPLE) {
            // regular Python tuple
            return tryLoadPyTuple(py_obj, kwargs);
        } else if (type_id == TypeId::DB0_LIST) {
            return tryLoadList(reinterpret_cast<ListObject*>(py_obj), kwargs);
        } else if (type_id == TypeId::LIST) {
            // regular Python list
            return tryLoadPyList(py_obj, kwargs);
        } else if (type_id == TypeId::DB0_DICT || type_id == TypeId::DICT) {
            return tryLoadDict(py_obj, kwargs);
        } else if (type_id == TypeId::DB0_SET || type_id == TypeId::SET) {
            return tryLoadSet(py_obj, kwargs);
        } else if (type_id == TypeId::DB0_ENUM_VALUE) {
            return tryLoadEnumValue(reinterpret_cast<PyEnumValue*>(py_obj));
        } else if (type_id == TypeId::MEMO_OBJECT) {
            return tryLoadMemo(reinterpret_cast<MemoObject*>(py_obj), kwargs, py_exclude);
        } else {
            THROWF(db0::InputException) << "Unload not implemented for type: " 
                << Py_TYPE(py_obj)->tp_name << THROWF_END;
        }
    }
    
    PyObject *getMaterializedMemoObject(PyObject *py_obj)
    {
        if (!PyMemo_Check(py_obj)) {
            // simply return self if not a memo object
            Py_INCREF(py_obj);
            return py_obj;
        }
        auto memo_obj = reinterpret_cast<MemoObject*>(py_obj);
        if (memo_obj->ext().hasInstance()) {
            Py_INCREF(py_obj);
            return py_obj;
        }
        
        db0::FixtureLock lock(memo_obj->ext().getFixture());
        // materialize by calling postInit
        memo_obj->modifyExt().postInit(lock);
        Py_INCREF(py_obj);
        return py_obj;
    }
    
    shared_py_object<PyObject*> tryUnloadObjectFromCache(LangCacheView &lang_cache, std::uint64_t address,
        std::shared_ptr<db0::object_model::Class> expected_type)
    {
        auto obj_ptr = lang_cache.get(address);
        if (obj_ptr && expected_type) {
            if (!PyMemo_Check(obj_ptr.get())) {
                THROWF(db0::InputException) << "Invalid object type: " << PyToolkit::getTypeName(obj_ptr.get()) << " (Memo expected)";
            }
            auto &memo = reinterpret_cast<MemoObject*>(obj_ptr.get())->ext();
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

        std::uint64_t addr = type->getSingletonAddress();
        if (!addr) {
            return nullptr;
        }
        
        // try unloading from cache first
        auto &lang_cache = fixture.getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, addr);
        
        if (obj_ptr) {
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
    
    PyObject *tryWeakProxy(PyObject *);    

}