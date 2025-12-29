// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <cstdint>
#include <optional>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include "shared_py_object.hpp"
#include <type_traits>
#include "PyToolkit.hpp"
#include "PySafeAPI.hpp"

// C++20 compatible replacement for PyVarObject_HEAD_INIT(NULL, 0)
// This macro provides designated initializers that work with C++20's requirement
// that all initializers in a structure must be either designated or non-designated
#define PYVAROBJECT_HEAD_INIT_DESIGNATED \
    .ob_base = { \
        .ob_base = { \
            .ob_refcnt = 1, \
            .ob_type = NULL, \
        }, \
        .ob_size = 0, \
    }
    
namespace db0

{

    class Snapshot;

}

namespace db0::object_model

{

    class ObjectIterable;
    
}

namespace db0::python

{   
        
    using ObjectId = db0::object_model::ObjectId;
    using ObjectIterable = db0::object_model::ObjectIterable;
    
    class LoadGuard
    {
    public:
        // Validate if the argument has not been used on the load stack
        LoadGuard(std::unordered_set<const void*> *load_stack_ptr, const void *arg_ptr);
        ~LoadGuard();

        // check if the argument was validated correctly
        operator bool() const {
            return m_arg_ptr != nullptr;
        }

    private:
        std::unordered_set<const void*> *m_load_stack_ptr;
        const void *m_arg_ptr = nullptr;
    };
    
    /**
     * Extarct full object UUID from python args compatible with db0.open()
    */
    ObjectId extractObjectId(PyObject *args);
    
    PyObject *fetchMemoObject(db0::swine_ptr<Fixture> &, ObjectId);

    PyObject *fetchListObject(db0::swine_ptr<Fixture> &, ObjectId);
    
    /**
     * Open dbzero object from a specific fixture
     * with optional type validation
    */
    shared_py_object<PyObject*> fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id, 
        PyTypeObject *py_expected_type = nullptr);

    // Check if object exists with optional type validation
    bool isExistingObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id, 
        PyTypeObject *py_expected_type = nullptr);
    
    void renameMemoClassField(PyTypeObject *py_type, const char *from_name, const char *to_name);
    
    /**
     * Runs a function, catch exeptions and translate into Python errors
     * @tparam ERR_RESULT integer value representing an error (default is 0 / NULL but some APIs may use -1)
    */
    template <int ERR_RESULT = 0, typename T, typename... Args>
    typename std::invoke_result_t<T, Args...> runSafe(T func, Args&&... args)
    {
        using ReturnType = std::invoke_result_t<T, Args...>;


        auto returnError = []() -> ReturnType {
            if constexpr (std::is_constructible_v<ReturnType, int>) {
                return ReturnType(ERR_RESULT);
            } else if constexpr (std::is_pointer_v<ReturnType>) {
                return reinterpret_cast<ReturnType>(ERR_RESULT);
            } else {
                return ReturnType{};
            }
        };

        try {
            auto result = func(std::forward<Args>(args)...);
            if (PyErr_Occurred()) {
                return returnError();
            }
            return result;
        } catch (const db0::BadAddressException &e) {
            PyErr_SetString(PyToolkit::getTypeManager().getReferenceError(), e.what());
            return returnError();
        } catch (const db0::ClassNotFoundException &e) {
            PyErr_SetString(PyToolkit::getTypeManager().getClassNotFoundError(), e.what());
            return returnError();
        } catch (const db0::IndexException &e) {
            PyErr_SetString(PyExc_IndexError, e.what());
            return returnError();
        } 
        #if ENABLE_DEBUG_EXCEPTIONS
            catch (const db0::AbstractException &e) {
                PyErr_SetString(PyExc_RuntimeError, e.what());
                return returnError();
            } 
        #else
            catch (const db0::AbstractException &e) {
                PyErr_SetString(PyExc_RuntimeError, e.getDesc().c_str());
                return returnError();
            }
        #endif 
        catch (const std::exception &e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return returnError();
        } catch (...) {
            PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
            return returnError();
        }

    }
    
    // Universal implementaton for both Workspace and WorkspaceView (aka Snapshot)
    shared_py_object<PyObject*> tryFetchFrom(db0::Snapshot &, PyObject *py_uuid, PyTypeObject *type = nullptr,
        const char *prefix_name = nullptr);    
    bool tryExistsIn(db0::Snapshot &snapshot, PyObject *py_id, PyTypeObject *type_arg,
        const char *prefix_name = nullptr);
        
    shared_py_object<PyObject*> tryUnloadObjectFromCache(LangCacheView &lang_cache, Address address,
        std::shared_ptr<db0::object_model::Class> expected_type = nullptr);
    
    /**
     * Open dbzero object by UUID     
     * @param py_expected_type - expected Python type of the object
    */    
    shared_py_object<PyObject*> fetchObject(db0::Snapshot &, ObjectId object_id, 
        PyTypeObject *py_expected_type = nullptr);
    bool isExistingObject(db0::Snapshot &, ObjectId object_id,
        PyTypeObject *py_expected_type = nullptr);
    
    bool tryParseFetchArgs(PyObject *args, PyObject *kwargs, PyObject *&py_id,
        PyObject *&py_type, const char *&prefix_name);
    
    /**
     * Open dbzero singleton by its corresponding Python type
    */
    PyObject *fetchSingletonObject(db0::Snapshot &, PyTypeObject *py_type, 
        const char *prefix_name = nullptr);        
    // Check if a singleton instance exists
    bool isExistingSingleton(db0::Snapshot &, PyTypeObject *py_type, const char *prefix_name = nullptr);
    
    // Convert a serializable instance to bytes
    PyObject *trySerialize(PyObject *);
    
    // Construct instance from bytes within a specific snapshot's context
    PyObject *tryDeserialize(db0::Snapshot *, PyObject *py_bytes);
    
    PyObject *tryGetSlabMetrics(db0::Workspace *);

    PyObject *trySetCacheSize(db0::Workspace *, std::size_t new_cache_size);
    
    PyObject *tryGetRefCount(PyObject *);
    
    PyObject *tryGetPrefixStats(PyObject *args, PyObject *kwargs);

    PyObject *tryGetStorageStats(PyObject *args, PyObject *kwargs);
    
    PyObject *tryGetAddress(PyObject *py_obj);
    
    PyTypeObject *tryGetType(PyObject *py_obj);
    
    PyObject *tryGetMemoTypeInfo(PyObject *py_obj);
    PyObject *tryGetMemoClass(PyObject *py_obj);
    
    PyObject *tryTouch(PyObject *const *args, Py_ssize_t nargs);
    
    // Load dbzero object to memory
    // @param load_stack_ptr - required to track and avoid circular references
    // @param load_all_depth - if > 0, load all references up to the specified depth without calling __load__ methods
    PyObject *tryLoad(PyObject *, PyObject*, PyObject *py_exlude = nullptr, 
        std::unordered_set<const void*> *load_stack_ptr = nullptr, bool load_all = false);
    
    template <typename MemoImplT>
    PyObject *getMaterializedMemoObject(MemoImplT *py_obj);
    
    // Retrieve prefix (its Fixture objects) from the optional argument "prefix"
    db0::swine_ptr<Fixture> getOptionalPrefixFromArg(db0::Snapshot &workspace, const char *prefix_name);
    db0::swine_ptr<Fixture> getPrefixFromArgs(PyObject *args, PyObject *kwargs, const char *param_name);
    db0::swine_ptr<Fixture> getPrefixFromArgs(db0::Snapshot &, PyObject *args, PyObject *kwargs, 
        const char *param_name);
    
    PyObject *tryMemoObject_open_singleton(PyTypeObject *, const Fixture &);
        
#ifndef NDEBUG
    /**
     * A test function to make an allocation and write random bytes into the current prefix
    */
    PyObject *writeBytes(PyObject *, PyObject *args);
    PyObject *freeBytes(PyObject *, PyObject *args);
    PyObject *readBytes(PyObject *, PyObject *args);
#endif
    
    bool isBase(PyTypeObject *py_type, PyTypeObject *base_type);
    
    // drop unreferenced db0 object with a python representation    
    template <db0::bindings::TypeId> void dropInstance(PyObject *);
    // register TypeId specializations
    void registerDropInstanceFunctions(std::vector<void (*)(PyObject *)> &functions);
    void dropInstance(db0::bindings::TypeId type_id, PyObject *);
    bool hasKWArg(PyObject *kwargs, const char *name);
    
#ifndef NDEBUG
    PyObject *tryGetDRAM_IOMap(const Fixture &);
    // opens BDevStorage and reads DRAM_IO map directly, without opening the prefix
    PyObject *tryGetDRAM_IOMapFromFile(const char *file_name);
    PyObject *trySetTestParams(PyObject *py_dict);
    PyObject *tryResetTestParams();
#endif    
    
    PyObject *tryAssign(PyObject *targets, PyObject *key_values);
    
    PyObject *tryCopyPrefixImpl(db0::swine_ptr<Fixture> &prefix, const std::string &output_file_name,
        std::optional<std::uint64_t> page_io_step_size = {});
    PyObject *tryCopyPrefix(PyObject *args, PyObject *kwargs);
    
    extern template PyObject *getMaterializedMemoObject(MemoObject *);
    extern template PyObject *getMaterializedMemoObject(MemoImmutableObject *);
    
}

