// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyToolkit.hpp"
#include <dbzero/bindings/python/embedded/EmbeddedObject.hpp>
#include <dbzero/bindings/python/embedded/EmbeddedDict.hpp>
#include <dbzero/bindings/python/embedded/EmbeddedSet.hpp>
#include <dbzero/bindings/python/embedded/EmbeddedTuple.hpp>
#include "Memo.hpp"
#include "MemoExpiredRef.hpp"
#include "PyInternalAPI.hpp"
#include "Types.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PyIndex.hpp>
#include <dbzero/bindings/python/collections/PyByteArray.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyWeakSet.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/value/long_weak_ref.hpp>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/types/PyClassFields.hpp>
#include <dbzero/bindings/python/types/PyClass.hpp>
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyTag.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/types/PyDecimal.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>

namespace db0::python

{
    

    PyToolkit::PyWorkspace PyToolkit::m_py_workspace;
    SafeRMutex PyToolkit::m_api_mutex;

    namespace
    {
        std::uint16_t getMemoInstanceId(PyObject *pyObject)
        {
            if (PyMemo_Check<MemoObject>(pyObject)) {
                return reinterpret_cast<MemoObject *>(pyObject)->ext().getInstanceId();
            }
            if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
                return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().getInstanceId();
            }
            return reinterpret_cast<MemoAnyObject *>(pyObject)->ext().getInstanceId();
        }

        bool memoHasRefs(PyObject *pyObject)
        {
            if (PyMemo_Check<MemoObject>(pyObject)) {
                return reinterpret_cast<MemoObject *>(pyObject)->ext().hasRefs();
            }
            if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
                return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().hasRefs();
            }
            return reinterpret_cast<MemoAnyObject *>(pyObject)->ext().hasRefs();
        }

        bool isEmbeddedObject(PyObject *pyObject)
        {
            return pyObject && Py_TYPE(pyObject) == &EmbeddedObjectType;
        }

        std::optional<db0::UniqueAddress> tryGetEmbeddedUniqueAddress(PyObject *pyObject)
        {
            if (PyEmbeddedMemo_Check(pyObject)) {
                return getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(pyObject)).uniqueAddress();
            }
            if (isEmbeddedObject(pyObject)) {
                return reinterpret_cast<EmbeddedObject *>(pyObject)->ext().uniqueAddress();
            }
            return std::nullopt;
        }

        bool shouldCacheEmbeddedObject(PyObject *pyObject)
        {
            if (PyEmbeddedMemo_Check(pyObject)) {
                return !getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(pyObject)).type().isNoCache();
            }
            if (isEmbeddedObject(pyObject)) {
                return !reinterpret_cast<EmbeddedObject *>(pyObject)->ext().type().isNoCache();
            }
            return false;
        }

        PyToolkit::TypeObjectPtr resolveUnloadLangType(
            const PyToolkit::ClassFactory &classFactory, const std::shared_ptr<db0::object_model::Class> &type,
            PyToolkit::TypeObjectSharedPtr langType, PyToolkit::TypeObjectPtr langTypeHint)
        {
            if (!langType) {
                langType = classFactory.getLangType(*type);
            }
            if (!!langType) {
                return langType.get();
            }
            if (langTypeHint) {
                return langTypeHint;
            }
            return PyToolkit::getTypeManager().getMemoBaseType().get();
        }

    }

    PyToolkit::ObjectSharedPtr PyToolkit::unloadEmbeddedInstance(
        db0::swine_ptr<Fixture> &fixture, ObjectPtr rootObject, const db0::object_model::o_tuple_item &item
    )
    {
        switch (item.itemKind()) {
            case StorageClass::NONE:
                return Py_BORROW(Py_None);
            case StorageClass::BOOLEAN:
                return Py_OWN(PyBool_FromLong(item.boolPayload().value()));
            case StorageClass::INT64:
                return Py_OWN(PyLong_FromLongLong(item.intPayload().value()));
            case StorageClass::PACKED_INT32:
                return Py_OWN(PyLong_FromUnsignedLong(item.packedIntPayload().value()));
            case StorageClass::FP_NUMERIC64:
                return Py_OWN(PyFloat_FromDouble(item.doublePayload().value()));
            case StorageClass::PTIME64:
                return Py_OWN(PyLong_FromUnsignedLongLong(item.uint64Payload().value()));
            case StorageClass::DATE:
                return Py_OWN(uint64ToPyDate(item.uint64Payload().value()));
            case StorageClass::DATETIME:
                return Py_OWN(uint64ToPyDatetime(item.uint64Payload().value()));
            case StorageClass::DATETIME_TZ:
                return Py_OWN(uint64ToPyDatetimeWithTZ(item.uint64Payload().value()));
            case StorageClass::TIME:
                return Py_OWN(uint64ToPyTime(item.uint64Payload().value()));
            case StorageClass::TIME_TZ:
                return Py_OWN(uint64ToPyTimeWithTz(item.uint64Payload().value()));
            case StorageClass::DECIMAL:
                return Py_OWN(uint64ToPyDecimal(item.uint64Payload().value()));
            case StorageClass::STRING_REF:
            case StorageClass::EMBEDDED_STRING: {
                auto str = item.stringPayload().get();
                auto result = Py_OWN(PyUnicode_FromStringAndSize(str.get_raw(), str.size()));
                if (!result) {
                    THROWF(db0::InputException) << "Failed to convert embedded string";
                }
                return result;
            }
            case StorageClass::DB0_BYTES:
            case StorageClass::EMBEDDED_BYTES: {
                const auto &bytes = item.bytesPayload();
                auto result = Py_OWN(PyBytes_FromStringAndSize(
                    reinterpret_cast<const char *>(bytes.getBuffer()), bytes.size()
                ));
                if (!result) {
                    THROWF(db0::InputException) << "Failed to convert embedded bytes";
                }
                return result;
            }
            case StorageClass::EMBEDDED_TUPLE: {
                if (!rootObject) {
                    THROWF(db0::InputException) << "Embedded tuple retrieval requires a root memo object";
                }
                const auto &tuple = db0::object_model::o_py_tuple::__const_ref(item.embeddedPayload().begin());
                return makeEmbeddedTuple(rootObject, tuple);
            }
            case StorageClass::EMBEDDED_SET: {
                if (!rootObject) {
                    THROWF(db0::InputException) << "Embedded set retrieval requires a root memo object";
                }
                const auto &set = db0::object_model::o_py_set::__const_ref(item.embeddedPayload().begin());
                return makeEmbeddedSet(rootObject, set);
            }
            case StorageClass::EMBEDDED_DICT: {
                if (!rootObject) {
                    THROWF(db0::InputException) << "Embedded dict retrieval requires a root memo object";
                }
                const auto &dict = db0::object_model::o_py_dict::__const_ref(item.embeddedPayload().begin());
                return makeEmbeddedDict(rootObject, dict);
            }
            case StorageClass::EMBEDDED_OBJECT: {
                if (!rootObject) {
                    THROWF(db0::InputException) << "Embedded object retrieval requires a root memo object";
                }
                const auto &embeddedObject = db0::object_model::o_embedded_object::__const_ref(
                    item.embeddedPayload().begin()
                );
                auto &classFactory = fixture->get<db0::object_model::ClassFactory>();
                auto type = classFactory.getTypeByClassRef(embeddedObject.getClassRef()).m_class;
                auto memoType = classFactory.getLangType(*type);
                if (memoType.get()) {
                    return makeEmbeddedMemoObject(rootObject, embeddedObject, std::move(type), memoType.get());
                }
                return makeEmbeddedObject(rootObject, embeddedObject, std::move(type));
            }
            default:
                THROWF(db0::InputException)
                    << "Unsupported embedded immutable member storage class: " << item.itemKind();
        }
        return {};
    }

    bool PyToolkit::hasMemoInstance(ObjectPtr pyObject)
    {
        if (PyEmbeddedMemo_Check(pyObject)) {
            return true;
        }
        if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
            return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().hasInstance();
        }
        return getTypeManager().extractAnyObject(pyObject).hasInstance();
    }

    UniqueAddress PyToolkit::getMemoUniqueAddress(ObjectPtr pyObject)
    {
        if (PyEmbeddedMemo_Check(pyObject)) {
            return getEmbeddedMemoUniqueAddress(pyObject);
        }
        if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
            return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().getUniqueAddress();
        }
        return getTypeManager().extractAnyObject(pyObject).getUniqueAddress();
    }

    bool PyToolkit::isMemoDead(ObjectPtr pyObject)
    {
        if (PyEmbeddedMemo_Check(pyObject)) {
            return false;
        }
        if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
            return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().isDead();
        }
        return getTypeManager().extractAnyObject(pyObject).isDead();
    }

    bool PyToolkit::isMemoDropped(ObjectPtr pyObject)
    {
        if (PyEmbeddedMemo_Check(pyObject)) {
            return false;
        }
        if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
            return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().isDropped();
        }
        return getTypeManager().extractAnyObject(pyObject).isDropped();
    }

    bool PyToolkit::hasMemoAnyRefs(ObjectPtr pyObject)
    {
        if (PyEmbeddedMemo_Check(pyObject)) {
            return true;
        }
        if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
            return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().hasAnyRefs();
        }
        return getTypeManager().extractAnyObject(pyObject).hasAnyRefs();
    }

    const object_model::Class &PyToolkit::getMemoType(ObjectPtr pyObject)
    {
        if (PyEmbeddedMemo_Check(pyObject)) {
            return getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(pyObject)).type();
        }
        if (PyMemo_Check<MemoImmutableObject>(pyObject)) {
            return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext().getType();
        }
        return getTypeManager().extractAnyObject(pyObject).getType();
    }

    const object_model::o_embedded_object &PyToolkit::getMemoImmutableObject(ObjectPtr pyObject)
    {
        if (PyEmbeddedMemo_Check(pyObject)) {
            return getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(pyObject)).embeddedObject();
        }
        if (isEmbeddedObject(pyObject)) {
            return reinterpret_cast<EmbeddedObject *>(pyObject)->ext().embeddedObject();
        }
        return reinterpret_cast<MemoImmutableObject *>(pyObject)->ext()->getObject();
    }
    
    void PyToolkit::throwErrorWithPyErrorCheck(const std::string& message, const std::string& error_detail) {
        if (PyErr_Occurred()) {
            PyObject *ptype, *pvalue, *ptraceback;
            PyErr_Fetch(&ptype, &pvalue, &ptraceback);
            PyObject* str_repr = PyObject_Str(pvalue);
            const char* error_msg = str_repr ? PyUnicode_AsUTF8(str_repr) : "Unknown Python error";
            std::string error_str(error_msg);
            Py_XDECREF(str_repr);
            Py_XDECREF(ptype);
            Py_XDECREF(pvalue);
            Py_XDECREF(ptraceback);
            THROWF(db0::InputException) << message << error_str << THROWF_END;
        } else {
            THROWF(db0::InputException) << message << error_detail << THROWF_END;
        }
    }
    
    std::string PyToolkit::getFullyQualifiedName(ObjectPtr func_obj) {
        if (!func_obj) {
            THROWF(db0::InputException) << "Null function object" << THROWF_END;
        }

        // Allow Python modules as CALLABLE values by serializing their module name.
        if (PyModule_Check(func_obj)) {
            auto name_obj = Py_OWN(PyObject_GetAttrString(func_obj, "__name__"));
            if (!name_obj) {
                THROWF(db0::InputException) << "Failed to get module name" << THROWF_END;
            }

            const char* name_cstr = PyUnicode_AsUTF8(*name_obj);
            if (!name_cstr) {
                THROWF(db0::InputException) << "Failed to decode module name as UTF-8" << THROWF_END;
            }
            return std::string(name_cstr);
        }

        // Reject bound/unbound methods
        if (PyMethod_Check(func_obj)) {
            THROWF(db0::InputException) << "Methods are not allowed as CALLABLE members" << THROWF_END;
        }

        // Reject built-in C functions
        if (PyCFunction_Check(func_obj)) {
            THROWF(db0::InputException) << "Built-in C functions are not allowed as CALLABLE members" << THROWF_END;
        }

        // Get function's __name__, __qualname__, and __module__
        auto name_obj   = Py_OWN(PyObject_GetAttrString(func_obj, "__name__"));
        auto qualname   = Py_OWN(PyObject_GetAttrString(func_obj, "__qualname__"));
        auto module_obj = Py_OWN(PyObject_GetAttrString(func_obj, "__module__"));

        if (!name_obj || !qualname || !module_obj) {
            THROWF(db0::InputException) << "Failed to get function name, qualname, or module" << THROWF_END;
        }

        // Decode UTF-8 strings
        const char* name_cstr   = PyUnicode_AsUTF8(*name_obj);
        const char* qual_cstr   = PyUnicode_AsUTF8(*qualname);
        const char* module_cstr = PyUnicode_AsUTF8(*module_obj);

        if (!name_cstr || !qual_cstr || !module_cstr) {
            THROWF(db0::InputException) << "Failed to decode function attributes as UTF-8" << THROWF_END;
        }

        // Reject lambdas
        if (strcmp(name_cstr, "<lambda>") == 0) {
            THROWF(db0::InputException) << "Lambda functions are not allowed as CALLABLE members" << THROWF_END;
        }

        // Reject decorated or nested functions (qualname contains <locals>)
        if (strstr(qual_cstr, "<locals>") != nullptr) {
            THROWF(db0::InputException) << "Decorated or nested functions are not allowed as CALLABLE members" << THROWF_END;
        }

        // Construct fully qualified name: module.qualname
        std::stringstream fqn_ss;
        fqn_ss << module_cstr << "." << qual_cstr;
        return fqn_ss.str();
    }
    
    typename PyToolkit::ObjectSharedPtr PyToolkit::getFunctionFromFullyQualifiedName(const char* fqn, size_t size) {
        // Make a copy to tokenize
        char* copy = static_cast<char*>(malloc(size + 1));
        if (!copy) {
            THROWF(db0::InputException) << "Failed to unload CALLABLE: memory allocation failed" << THROWF_END;
        }
        memcpy(copy, fqn, size);
        copy[size] = '\0';

        // First token is the module root
        char* p = strchr(copy, '.');
        if (!p) {  // Bare module name
            auto module = Py_OWN(PyImport_ImportModule(copy));
            free(copy);
            if (!module) {
                throwErrorWithPyErrorCheck("Failed to unload CALLABLE: ",
                    "could not import module");
            }
            return module;
        }
        *p = '\0';
        const char* root = copy;

        // Import the module
        auto module = Py_OWN(PyImport_ImportModule(root));
        if (!module) {
            free(copy);
            throwErrorWithPyErrorCheck("Failed to unload CALLABLE: ", 
                "could not import module");
        }

        auto obj = module;            // Start walking attributes

        char* attr = p + 1;
        while (attr && *attr) {
            char* dot = strchr(attr, '.');
            if (dot) *dot = '\0';

            auto next = Py_OWN(PyObject_GetAttrString(obj.get(), attr));

            if (!next) {                   // Attribute missing
                free(copy);
                throwErrorWithPyErrorCheck("Failed to unload CALLABLE: ", 
                    "attribute missing");
            }
            obj = next;
            attr = dot ? dot + 1 : NULL;
        }
        free(copy);
        return obj;    // New ref; caller DECREFs
    }
    
    std::string PyToolkit::getTypeName(ObjectPtr py_object) {
        return getTypeName(Py_TYPE(py_object));
    }
    
    std::string PyToolkit::getTypeName(TypeObjectPtr py_type) {
        return std::string(py_type->tp_name);
    }
    
    std::string getModuleNameFromFileName(const std::string &file_name)
    {
        // remove extensions and path
        auto pos = file_name.find_last_of('/');
        if (pos == std::string::npos) {
            pos = file_name.find_last_of('\\');
        }
        auto file_name_no_path = file_name.substr(pos + 1);
        pos = file_name_no_path.find_last_of('.');
        if (pos == std::string::npos) {
            return file_name_no_path;
        }
        return file_name_no_path.substr(0, pos);
    }
    
    std::optional<std::string> PyToolkit::tryGetModuleName(TypeObjectPtr py_type)
    {
        auto py_module_name = Py_OWN(PyObject_GetAttrString(reinterpret_cast<ObjectPtr>(py_type), "__module__"));
        if (!py_module_name) {
            return std::nullopt;
        }
        auto result = std::string(PyUnicode_AsUTF8(*py_module_name));
        if (result == "__main__") {
            // for Memo types we can determine the actual module name from the file name
            // (if stored with the type decoration)
            if (PyAnyMemoType_Check(py_type)) {
                // file name may not be available in the type decoration
                auto file_name = MemoTypeDecoration::get(py_type).tryGetFileName();
                if (file_name) {
                    return getModuleNameFromFileName(file_name);
                }
            }
            return std::nullopt;
        }
        return result;
    }

    std::string PyToolkit::getModuleName(TypeObjectPtr py_type)
    {
        auto result = tryGetModuleName(py_type);
        if (!result) {
            THROWF(db0::InputException) << "Could not get module name for class " << getTypeName(py_type);
        }
        return *result;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, Address address,
        TypeObjectPtr lang_class, std::uint16_t instance_id, AccessFlags access_mode)
    {
        auto &class_factory = fixture->get<ClassFactory>();
        return unloadObject(fixture, address, class_factory, lang_class, instance_id, access_mode);
    }
    
    bool PyToolkit::isExistingObject(db0::swine_ptr<Fixture> &fixture, Address address, std::uint16_t instance_id)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address, nullptr);
        
        if (obj_ptr.get()) {
            // only validate instance ID if provided
            if (instance_id) {
                // NOTE: we first must check if this is really a memo object
                if (!isAnyMemoObject(obj_ptr.get())) {
                    return false;
                }
                
                if (getMemoInstanceId(obj_ptr.get()) != instance_id) {
                    return false;
                }
            }
            // NOTE: objects with no references (either from dbzero or other lang types) are considered deleted            
            return PyToolkit::hasLangRefs(*obj_ptr) || memoHasRefs(obj_ptr.get());
        }

        std::size_t sizeOf = 0;
        if (!fixture->isAddressValid(address, db0::object_model::ObjectAnyImpl::REALM_ID, &sizeOf)) {
            return false;
        }
        db0::object_model::ObjectAnyImpl::ObjectStem commonStem(db0::tag_verified(), fixture->myPtr(address), sizeOf);
        if (instance_id && commonStem->m_header.getInstanceId() != instance_id) {
            return false;
        }
        if (commonStem->m_header.isImmutableObject()) {
            return db0::object_model::ObjectImmutableImpl::checkUnload(fixture, address, instance_id, true);
        }
        return db0::object_model::Object::checkUnload(fixture, address, instance_id, true);
    }

    static PyToolkit::ObjectSharedPtr tryUnloadObjectResolved(
        db0::swine_ptr<Fixture> &fixture, Address address, const PyToolkit::ClassFactory &class_factory,
        PyToolkit::TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode,
        const Allocator::AllocationInfo *allocationInfo, bool authorize_data_filter)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address, nullptr);
        
        if (obj_ptr.get()) {
            // only validate instance ID if provided
            if (instance_id) {
                // NOTE: we first must check if this is really a memo object
                if (!PyToolkit::isAnyMemoObject(obj_ptr.get())) {
                    return {};
                }
                if (getMemoInstanceId(obj_ptr.get()) != instance_id) {
                    return {};
                }
            }
            if (authorize_data_filter) {
                authorizeDataFilterFetch(
                    fixture, PyToolkit::getMemoType(obj_ptr.get()), PyToolkit::getMemoUniqueAddress(obj_ptr.get()));
            }
            
            return obj_ptr;
        }
        
        std::size_t sizeOf;
        if (allocationInfo) {
            sizeOf = allocationInfo->size;
        } else {
            sizeOf = 0;
            if (!fixture->isAddressValid(address, db0::object_model::ObjectAnyImpl::REALM_ID, &sizeOf)) {
                return {};
            }
        }

        db0::object_model::ObjectAnyImpl::ObjectStem commonStem(
            db0::tag_verified(), fixture->myPtr(address), sizeOf, access_mode
        );
        if (instance_id && commonStem->m_header.getInstanceId() != instance_id) {
            return {};
        }

        if (commonStem->m_header.isImmutableObject()) {
            auto stem = db0::object_model::ObjectAnyImpl::castStem<
                db0::object_model::ObjectImmutableImpl::ObjectStem
            >(std::move(commonStem));
            auto typeInfo = class_factory.getTypeByClassRef(stem->getClassRef());
            auto type = typeInfo.m_class;
            if (authorize_data_filter) {
                authorizeDataFilterFetch(fixture, *type, UniqueAddress(address, stem->m_header.getInstanceId()));
            }
            lang_type_ptr = resolveUnloadLangType(class_factory, type, typeInfo.m_lang_type, lang_type_ptr);

            auto *memo_ptr = reinterpret_cast<MemoImmutableObject *>(lang_type_ptr->tp_alloc(lang_type_ptr, 0));
            memo_ptr->unload(
                fixture, std::move(stem), type, db0::object_model::ObjectImmutableImpl::with_type_hint{}
            );
            memo_ptr->ext().setLangObject(reinterpret_cast<PyObject *>(memo_ptr));
            obj_ptr = Py_OWN(reinterpret_cast<PyObject *>(memo_ptr));
            if (!memo_ptr->ext().isNoCache()) {
                lang_cache.add(address, obj_ptr.get());
            }
            return obj_ptr;
        }

        auto stem = db0::object_model::ObjectAnyImpl::castStem<
            db0::object_model::Object::ObjectStem
        >(std::move(commonStem));
        auto typeInfo = class_factory.getTypeByClassRef(stem->getClassRef());
        auto type = typeInfo.m_class;
        if (authorize_data_filter) {
            authorizeDataFilterFetch(fixture, *type, UniqueAddress(address, stem->m_header.getInstanceId()));
        }
        lang_type_ptr = resolveUnloadLangType(class_factory, type, typeInfo.m_lang_type, lang_type_ptr);
        
        // construct Python's memo object (placeholder for actual dbzero instance)
        // the associated lang class must be available        
        auto *memo_ptr = MemoObjectStub_new(lang_type_ptr);
        // unload from stem (with type hint)
        memo_ptr->unload(fixture, std::move(stem), type, PyToolkit::Object::with_type_hint{});
        // NOTE: Py_OWN only possible with a proper object
        obj_ptr = Py_OWN((PyObject*)memo_ptr);
        if (!memo_ptr->ext().isNoCache()) {
            lang_cache.add(address, obj_ptr.get());
        }
        return obj_ptr;
    }

    PyToolkit::ObjectSharedPtr PyToolkit::tryUnloadObject(
        db0::swine_ptr<Fixture> &fixture, Address address, const ClassFactory &class_factory,
        TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode,
        bool authorize_data_filter)
    {
        return tryUnloadObjectResolved(
            fixture, address, class_factory, lang_type_ptr, instance_id, access_mode, nullptr,
            authorize_data_filter
        );
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadEmbeddedObject(
        db0::swine_ptr<Fixture> &fixture, Address address, const PyToolkit::ClassFactory &class_factory,
        PyToolkit::TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode,
        ObjectSharedPtr root_object, const Allocator::AllocationInfo *alloc_info, bool authorize_data_filter)
    {
        auto &lang_cache = fixture->getLangCache();
        auto cached_object = lang_cache.get(address);
        if (!!cached_object) {
            if (instance_id) {
                auto cached_address = tryGetEmbeddedUniqueAddress(cached_object.get());
                if (!cached_address || cached_address->getInstanceId() != instance_id) {
                    THROWF(db0::InputException) << "Invalid UUID or object has been deleted";
                }
            }
            if (authorize_data_filter) {
                authorizeDataFilterFetch(
                    fixture, PyToolkit::getMemoType(cached_object.get()),
                    PyToolkit::getMemoUniqueAddress(cached_object.get()));
            }
            return cached_object;
        }

        Allocator::AllocationInfo local_alloc_info;
        if (!alloc_info) {
            local_alloc_info = fixture->findAllocation(address, db0::object_model::ObjectImmutableImpl::REALM_ID);
            alloc_info = &local_alloc_info;
        }
        assert(alloc_info);
        auto embedded_offset = address.getOffset() - alloc_info->address.getOffset();

        // Resolve the root object if not provided
        if (!root_object) {
            root_object = tryUnloadObjectResolved(
                fixture, alloc_info->address, class_factory, lang_type_ptr, instance_id, access_mode,
                alloc_info, false
            );
            if (!root_object) {
                THROWF(db0::InputException) << "Invalid UUID or object has been deleted";
            }
        }
        assert(!!root_object);
        auto *root_memo = reinterpret_cast<PyToolkit::TypeManager::MemoImmutableObject *>(root_object.get());
        auto embedded_object = root_memo->ext().getEmbeddedInstanceAtOffset(embedded_offset);
        if (authorize_data_filter) {
            authorizeDataFilterFetch(
                fixture, PyToolkit::getMemoType(embedded_object.get()),
                PyToolkit::getMemoUniqueAddress(embedded_object.get()));
        }
        if (shouldCacheEmbeddedObject(embedded_object.get())) {
            lang_cache.add(address, embedded_object.get());
        }
        return embedded_object;
    }

    PyToolkit::ObjectSharedPtr PyToolkit::unloadAnyObject(
        db0::swine_ptr<Fixture> &fixture, Address address, const ClassFactory &class_factory,
        TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode,
        bool authorize_data_filter)
    {
        auto allocation = fixture->findAllocation(address, db0::object_model::ObjectImmutableImpl::REALM_ID);
        auto rootObject = tryUnloadObjectResolved(
            fixture, allocation.address, class_factory, lang_type_ptr, instance_id, access_mode, &allocation,
            authorize_data_filter
        );
        if (!rootObject) {
            THROWF(db0::InputException) << "Invalid UUID or object has been deleted";
        }
        if (allocation.address == address) {
            return rootObject;
        }

        return unloadEmbeddedObject(
            fixture, address, class_factory, lang_type_ptr, instance_id, access_mode, rootObject, &allocation,
            authorize_data_filter
        );
    }

    PyToolkit::ObjectSharedPtr PyToolkit::unloadAnyObject(
        db0::swine_ptr<Fixture> &fixture, Address address, std::shared_ptr<Class> type_hint,
        TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode)
    {
        auto allocation = fixture->findAllocation(address, db0::object_model::ObjectImmutableImpl::REALM_ID);
        auto &classFactory = fixture->get<ClassFactory>();
        if (!lang_type_ptr) {
            auto langType = classFactory.hasLangType(*type_hint)
                ? classFactory.getLangType(*type_hint)
                : getTypeManager().getMemoBaseType();
            lang_type_ptr = langType.get();
        }
        auto rootObject = unloadObject(
            fixture, allocation.address, std::move(type_hint), lang_type_ptr, access_mode
        );
        auto *rootMemo = reinterpret_cast<MemoImmutableObject *>(rootObject.get());
        if (instance_id && rootMemo->ext().getInstanceId() != instance_id) {
            THROWF(db0::InputException) << "Invalid UUID or object has been deleted";
        }
        if (allocation.address == address) {
            return rootObject;
        }

        return unloadEmbeddedObject(
            fixture, address, classFactory, lang_type_ptr, instance_id, access_mode, rootObject, &allocation
        );
    }

    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, Address address,
        const ClassFactory &class_factory, TypeObjectPtr lang_type_ptr, std::uint16_t instance_id, AccessFlags access_mode,
        bool authorize_data_filter)
    {
        auto result = tryUnloadObject(
            fixture, address, class_factory, lang_type_ptr, instance_id, access_mode, authorize_data_filter
        );
        if (!result) {
            THROWF(db0::InputException) << "Invalid UUID or object has been deleted";            
        }
        return result;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, Address address,
        std::shared_ptr<Class> type_hint, TypeObjectPtr lang_class_hint, AccessFlags access_mode,
        const ClassFactory *class_factory_ptr)
    {
        assert(lang_class_hint);
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, address);
        
        if (obj_ptr.get()) {
            return obj_ptr;
        }
        
        // NOTE: lang_class may be of a base type (e.g. MemoBase)
        auto *memo_ptr = MemoObjectStub_new(lang_class_hint);
        // unload with type hint
        bool type_hit;
        memo_ptr->unload(fixture, address, type_hint, Object::with_type_hint{}, access_mode, &type_hit);
        if (!type_hit) {
            // we need to resolve the actual lang type on type miss
            if (!class_factory_ptr) {
                class_factory_ptr = &fixture->get<ClassFactory>();
            }
            auto lang_type = class_factory_ptr->getLangType(memo_ptr->ext().getType());
            // then upcast to the exact type if found
            if (!!lang_type) {
                memo_ptr->unsafeUpcastTo(lang_type.get());
            }
        }
        
        // NOTE: Py_OWN only possible with a proper object
        obj_ptr = Py_OWN((PyObject*)memo_ptr);
        if (!memo_ptr->ext().isNoCache()) {
            lang_cache.add(address, obj_ptr.get());
        }
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadExpiredRef(db0::swine_ptr<Fixture> &fixture, Address addr,
        std::uint64_t obj_fixture_uuid, UniqueAddress obj_address)
    {
        // try unloading from cache first
        auto &lang_cache = fixture->getLangCache();
        auto obj_ptr = tryUnloadObjectFromCache(lang_cache, addr);
        
        if (obj_ptr.get()) {
            return obj_ptr;
        }
        
        obj_ptr = MemoExpiredRef_new(obj_fixture_uuid, obj_address);
        lang_cache.add(addr, obj_ptr.get());
        return obj_ptr;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadExpiredRef(db0::swine_ptr<Fixture> &fixture, const LongWeakRef &weak_ref) {
        return unloadExpiredRef(fixture, weak_ref.getAddress(), weak_ref->m_fixture_uuid, weak_ref->m_address);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadList(db0::swine_ptr<Fixture> fixture, Address address, 
        std::uint16_t, AccessFlags access_mode)
    {
        using List = db0::object_model::List;

        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();        
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto list_object = ListDefaultObject_new();
        // retrieve actual dbzero instance
        list_object->unload(fixture, address, access_mode);
        // add list object to cache
        if (!list_object->ext().isNoCache()) {
            lang_cache.add(address, list_object.get());
        }
        return shared_py_cast<PyObject*>(std::move(list_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadByteArray(db0::swine_ptr<Fixture> fixture, 
        Address address, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto byte_array_object = ByteArrayDefaultObject_new();
        // retrieve actual dbzero instance
        byte_array_object->unload(fixture, address, access_mode);
        // add byte_array object to cache
        if (!byte_array_object->ext().isNoCache()) {
            lang_cache.add(address, byte_array_object.get());
        }
        return shared_py_cast<PyObject*>(std::move(byte_array_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadIndex(db0::swine_ptr<Fixture> fixture,
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto py_index = Py_OWN(IndexDefaultObject_new());
        // retrieve actual dbzero instance
        py_index->unload(fixture, address, access_mode);

        // add list object to cache
        // NOTE: in case of Index (which requires a flush on update) we need to cache instance
        // even if accessed as no-cache to prevent premature deletion        
        lang_cache.add(address, py_index.get());

        auto py_index_ptr = py_index.get();
        // Dirty/clean notifications are state transitions, but nested atomic
        // rollback/merge paths may emit a clean notification for work that did
        // not take a matching Python self-reference in this wrapper callback.
        // Keep the callback's own ref balance explicit so an unmatched clean
        // does not drop the LangCache-owned Index wrapper.
        auto dirty_ref_count = std::make_shared<std::uint32_t>(0);
        py_index->ext().setDirtyCallback([py_index_ptr, dirty_ref_count](bool incRef) {
            if (incRef) {
                Py_INCREF(py_index_ptr);
                ++(*dirty_ref_count);
            } else {
                if (*dirty_ref_count > 0) {
                    --(*dirty_ref_count);
                    Py_DECREF(py_index_ptr);
                }
            }
        });
        
        return shared_py_cast<PyObject*>(std::move(py_index));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadSet(db0::swine_ptr<Fixture> fixture,
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }

        auto set_object = SetDefaultObject_new();
        // retrieve actual dbzero instance
        set_object->unload(fixture, address, access_mode);

        // add list object to cache
        if (!set_object->ext().isNoCache()) {
            lang_cache.add(address, set_object.get());
        }
        return shared_py_cast<PyObject*>(std::move(set_object));
    }

    PyToolkit::ObjectSharedPtr PyToolkit::unloadWeakSet(db0::swine_ptr<Fixture> fixture,
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            return object_ptr;
        }

        auto weak_set_object = WeakSetDefaultObject_new();
        weak_set_object->unload(fixture, address, access_mode);

        if (!weak_set_object->ext().isNoCache()) {
            lang_cache.add(address, weak_set_object.get());
        }
        return shared_py_cast<PyObject*>(std::move(weak_set_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadDict(db0::swine_ptr<Fixture> fixture, 
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto dict_object = DictDefaultObject_new();
        // retrieve actual dbzero instance
        dict_object->unload(fixture, address, access_mode);
        
        // add list object to cache
        if (!dict_object->ext().isNoCache()) {
            lang_cache.add(address, *dict_object);
        }
        return shared_py_cast<PyObject*>(std::move(dict_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::unloadTuple(db0::swine_ptr<Fixture> fixture, 
        Address address, std::uint16_t, AccessFlags access_mode)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr.get()) {
            // return from cache
            return object_ptr;
        }
        
        auto tuple_object = TupleDefaultObject_new();
        // retrieve actual dbzero instance        
        tuple_object->unload(fixture, address, access_mode);
        
        // add list object to cache
        if (!tuple_object->ext().isNoCache()) {
            lang_cache.add(address, *tuple_object);
        }
        return shared_py_cast<PyObject*>(std::move(tuple_object));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::deserializeObjectIterable(db0::swine_ptr<Fixture> fixture,
        std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        auto obj_iter = db0::object_model::ObjectIterator::deserialize(fixture, iter, end);
        auto py_iter = PyObjectIterableDefault_new();
        py_iter->makeNew(std::move(*obj_iter));
        return shared_py_cast<PyObject*>(std::move(py_iter));
    }

    const db0::object_model::ObjectIterable &PyToolkit::getPredicateIterable(ObjectPtr py_object)
    {
        if (!PyObjectIterable_Check(py_object)) {
            THROWF(db0::InputException) << "Predicate object must be an ObjectIterable";
        }
        auto &predicate = reinterpret_cast<PyObjectIterable*>(py_object)->ext();
        if (!predicate.isPredicateOnly()) {
            THROWF(db0::InputException) << "Predicate object must be created with db0.predicate";
        }
        return predicate;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::deserializeEnumValue(db0::swine_ptr<Fixture> fixture,
        std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        auto &snapshot = fixture->getWorkspace();
        return db0::object_model::EnumValue::deserialize(snapshot, iter, end);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::deserializeEnumValueRepr(db0::swine_ptr<Fixture> fixture,
        std::vector<std::byte>::const_iterator &iter, 
        std::vector<std::byte>::const_iterator end)
    {
        return db0::object_model::EnumValueRepr::deserialize(fixture, iter, end);
    }
    
    std::uint64_t PyToolkit::getTagFromString(ObjectPtr py_object, db0::pools::RC_LimitedStringPool &string_pool)
    {
        if (!PyUnicode_Check(py_object)) {
            // unable to resolve as tag
            THROWF(db0::InputException) << "Unable to resolve object as tag";
        }
                
        return string_pool.toAddress(string_pool.get(PyUnicode_AsUTF8(py_object)));
    }
    
    std::uint64_t PyToolkit::addTagFromString(ObjectPtr py_object, db0::pools::RC_LimitedStringPool &string_pool, bool &inc_ref)
    {
        if (!PyUnicode_Check(py_object)) {
            // unable to resolve as tag
            THROWF(db0::InputException) << "Unable to resolve object as tag";
        }
        return string_pool.toAddress(string_pool.add(inc_ref, PyUnicode_AsUTF8(py_object)));
    }

    bool PyToolkit::isIterable(ObjectPtr py_object) {
        return Py_TYPE(py_object)->tp_iter != nullptr;
    }

    bool PyToolkit::isString(ObjectPtr py_object) {
        return PyUnicode_Check(py_object);
    }

    bool PyToolkit::isSequence(ObjectPtr py_object) {
        return PySequence_Check(py_object);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::getIterator(ObjectPtr py_object)
    {
        auto py_iterator = Py_OWN(PyObject_GetIter(py_object));
        if (!py_iterator) {
            THROWF(db0::InputException) << "Unable to get iterator" << THROWF_END;
        }
        return py_iterator;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::next(ObjectPtr py_object)
    {
        auto py_next = Py_OWN(PyIter_Next(py_object));
        if (!py_next) {
            // StopIteration exception raised
            PyErr_Clear();
        }

        return py_next;
    }

    std::size_t PyToolkit::length(ObjectPtr py_object)
    {
        Py_ssize_t size = PySequence_Length(py_object);
        if (size < 0) {
            THROWF(db0::InputException) << "Unable to get sequence length" << THROWF_END;
        }
        return size;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::getItem(ObjectPtr py_object, std::size_t i)
    {
        auto item = Py_OWN(PySequence_GetItem(py_object, i));
        if (!item) {
            THROWF(db0::InputException) << "Unable to get sequence item at index ";
        }
        return item;
    }

    PyToolkit::ObjectSharedPtr PyToolkit::getMappingItem(ObjectPtr py_object, ObjectPtr key)
    {
        auto item = Py_OWN(PyObject_GetItem(py_object, key));
        if (!item) {
            THROWF(db0::InputException) << "Unable to get mapping item";
        }
        return item;
    }
    
    bool PyToolkit::isSingleton(TypeObjectPtr py_type) {
        return PyMemoType_IsSingleton(py_type);
    }
    
    bool PyToolkit::isType(ObjectPtr py_object) {
        return PyType_Check(py_object);
    }

    bool PyToolkit::isAnyMemoObject(ObjectPtr py_object) {
        return PyAnyMemo_Check(py_object) || PyEmbeddedMemo_Check(py_object);
    }

    bool PyToolkit::isEmbeddedMemoObject(ObjectPtr py_object) {
        return PyEmbeddedMemo_Check(py_object);
    }

    bool PyToolkit::isMemoObject(ObjectPtr py_object) {
        return PyMemo_Check<MemoObject>(py_object);
    }

    bool PyToolkit::isMemoImmutableObject(ObjectPtr py_object) {
        return PyMemo_Check<MemoImmutableObject>(py_object) || PyEmbeddedMemo_Check(py_object);
    }
    
    PyToolkit::ObjectPtr PyToolkit::getUUID(ObjectPtr py_object) {
        return db0::python::tryGetUUID(py_object);
    }
    
    bool PyToolkit::isEnumValue(ObjectPtr py_object) {
        return PyEnumValue_Check(py_object);
    }

    bool PyToolkit::isFieldDef(ObjectPtr py_object) {
        return PyFieldDef_Check(py_object);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeEnumValue(const EnumValue &value) {
        return shared_py_cast<PyObject*>(makePyEnumValue(value));
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeEnumValueRepr(std::shared_ptr<EnumTypeDef> type_def,
        const char *str_value) 
    {
        return shared_py_cast<PyObject*>(makePyEnumValueRepr(type_def, str_value));
    }
    
    std::string PyToolkit::getLastError()
    {
        PyObject *ptype, *pvalue, *ptraceback;
        PyErr_Fetch(&ptype, &pvalue, &ptraceback);
        PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
        auto pstr = Py_OWN(PyObject_Str(pvalue));
        Py_XDECREF(ptype);
        Py_XDECREF(pvalue);
        Py_XDECREF(ptraceback);

        return PyUnicode_AsUTF8(*pstr);
    }
    
    std::uint64_t PyToolkit::getFixtureUUID(ObjectPtr py_object)
    {
        if (PyType_Check(py_object)) {
            return getFixtureUUID(reinterpret_cast<TypeObjectPtr>(py_object));
        } else if (PyEnumValue_Check(py_object)) {
            return reinterpret_cast<PyEnumValue*>(py_object)->ext().m_fixture.safe_lock()->getUUID();
        } else if (PyAnyMemo_Check(py_object)) {
            return reinterpret_cast<MemoAnyObject*>(py_object)->ext().getFixture()->getUUID();
        } else if (PyEmbeddedMemo_Check(py_object)) {
            return getEmbeddedMemoFixture(py_object)->getUUID();
        } else if (PyObjectIterable_Check(py_object)) {
            return reinterpret_cast<PyObjectIterable*>(py_object)->ext().getFixture()->getUUID();
        } else if (PyObjectIterator_Check(py_object)) {
            return reinterpret_cast<PyObjectIterator*>(py_object)->ext().getFixture()->getUUID();
        } else if (PyTag_Check(py_object)) {
            return reinterpret_cast<PyTag*>(py_object)->ext().tryGetFixtureUUID();
        } else {
            return 0;
        }
    }
    
    std::uint64_t PyToolkit::getFixtureUUID(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFixtureUUID(AccessType::READ_ONLY);
        } else {
            return 0;
        }
    }
    
    bool PyToolkit::isNoDefaultTags(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::NO_DEFAULT_TAGS];
        } else {
            return false;
        }
    }
    
    bool PyToolkit::isNoCache(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::NO_CACHE];
        } else {
            return false;
        }
    }
    
    bool PyToolkit::isImmutable(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::IMMUTABLE];
        } else {
            return false;
        }
    }

    bool PyToolkit::isIntern(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::INTERN];
        } else {
            return false;
        }
    }

    bool PyToolkit::isProtectFields(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::PROTECT_FIELDS];
        } else {
            return false;
        }
    }

    bool PyToolkit::isAccessControl(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags()[MemoOptions::ACCESS_CONTROL];
        } else {
            return false;
        }
    }

    FlagSet<MemoOptions> PyToolkit::getMemoFlags(TypeObjectPtr py_type)
    {
        if (isAnyMemoType(py_type)) {
            return MemoTypeDecoration::get(py_type).getFlags();
        } else {
            return {};
        }
    }

    const char *PyToolkit::getPrefixName(TypeObjectPtr memo_type)
    {
        assert(isAnyMemoType(memo_type));
        return MemoTypeDecoration::get(memo_type).tryGetPrefixName();
    }
    
    const char *PyToolkit::getMemoTypeID(TypeObjectPtr memo_type)
    {
        assert(isAnyMemoType(memo_type));
        return MemoTypeDecoration::get(memo_type).tryGetTypeId();        
    }
    
    const std::vector<std::string> &PyToolkit::getInitVars(TypeObjectPtr memo_type)
    {
        assert(isAnyMemoType(memo_type));
        return MemoTypeDecoration::get(memo_type).getInitVars();
    }
    
    bool PyToolkit::isAnyMemoType(TypeObjectPtr py_type) {
        return PyAnyMemoType_Check(py_type);
    }
    
    void PyToolkit::setError(ObjectPtr err_obj, std::uint64_t err_value) {
        PyErr_SetObject(err_obj, *Py_OWN(PyLong_FromUnsignedLongLong(err_value)));
    }
    
    bool PyToolkit::hasLangRefs(ObjectPtr obj) {
        // NOTE: total number of references must be greater than the extended (inner) reference count
        // NOTE: for regular objects (we use defult = 1 to account for the reference held by the LangCache)
        return Py_REFCNT(obj) > PyEXT_REFCOUNT(obj, 1);
    }
    
    bool PyToolkit::hasAnyLangRefs(ObjectPtr obj, unsigned int ext_ref_count) {
        return Py_REFCNT(obj) > ext_ref_count;
    }
    
    PyObject *getValue(PyObject *py_dict, const std::string &key)
    {
        if (!PyDict_Check(py_dict)) {
            THROWF(db0::InputException) << "Invalid type of object. Dictionary expected" << THROWF_END;
        }
        auto result = PyDict_GetItemString(py_dict, key.c_str());
        if (!result) {
            // key not found
            return nullptr;
        }
        return Py_NEW(result);
    }
    
    std::optional<long> PyToolkit::getLong(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        

        if (!PyLong_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Integer expected but got: " 
                << Py_TYPE(*py_value)->tp_name << THROWF_END;
        }
        return PyLong_AsLong(*py_value);
    }

    std::optional<unsigned long long> PyToolkit::getUnsignedLongLong(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        

        if (!PyLong_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Integer expected but got: " 
                << Py_TYPE(*py_value)->tp_name << THROWF_END;
        }
        return PyLong_AsUnsignedLongLong(*py_value);
    }
    
    std::optional<unsigned int> PyToolkit::getUnsignedInt(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        
        
        if (!PyLong_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Integer expected but got: " 
                << Py_TYPE(*py_value)->tp_name << THROWF_END;
        }
        return PyLong_AsUnsignedLong(*py_value);
    }

    std::optional<bool> PyToolkit::getBool(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        
        if (!PyBool_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". Boolean expected" << THROWF_END;
        }
        return PyObject_IsTrue(*py_value);
    }
    
    std::optional<std::string> PyToolkit::getString(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        if (!py_value) {
            return std::nullopt;
        }        
        if (!PyUnicode_Check(*py_value)) {
            THROWF(db0::InputException) << "Invalid type of: " << key << ". String expected" << THROWF_END;
        }
        return std::string(PyUnicode_AsUTF8(*py_value));
    }
    
    bool PyToolkit::hasKey(ObjectPtr py_object, const std::string &key)
    {
        auto py_value = Py_OWN(getValue(py_object, key));
        return py_value.get() != nullptr;
    }
    
    bool PyToolkit::compare(ObjectPtr py_object1, ObjectPtr py_object2)
    {
        auto result = PyObject_RichCompareBool(py_object1, py_object2, Py_EQ);
        if (result < 0) {
            // comparison failed
            THROWF(db0::InputException) << "Comparison failed" << THROWF_END;
        }
        return result == 1;
    }
    
    bool PyToolkit::isClassObject(ObjectPtr py_object) {
        return PyClassObject_Check(py_object);
    }
    
    SafeRLock PyToolkit::lockApi() {
        return { m_api_mutex };        
    }
    
    SafeRLock PyToolkit::lockPyApi()
    {
        if (m_api_mutex.isOwnedByThisThread()) {            
            // already locked by this thread
            return {};            
        } 

        if (!Py_IsInitialized()) {
            // Simply return the lock after python instance was finalized
            // This is safe because fixture threads should be stopped at this point
            return SafeRLock(m_api_mutex);
        }

        // unlock GIL while waiting for the API mutex
        PyThreadState *__save = PyEval_SaveThread();
        auto result = SafeRLock(m_api_mutex);
        // restore GIL
        PyEval_RestoreThread(__save);
        return result;
    }

    PyToolkit::TypeObjectPtr PyToolkit::getBaseType(TypeObjectPtr py_object) {
        return py_object->tp_base;
    }
    
    PyToolkit::TypeObjectPtr PyToolkit::getBaseMemoType(TypeObjectPtr py_memo_type)
    {
        assert(isAnyMemoType(py_memo_type));
        // first base type is python base. From there we can get the actual base type
        auto base_py_type = getBaseType(py_memo_type);
        if (!base_py_type) {
            return nullptr;
        }
        auto memo_base_type = getBaseType(base_py_type);
        if (memo_base_type && isAnyMemoType(memo_base_type)) {
            return memo_base_type;
        }
        return nullptr;
    }

    bool PyToolkit::isTag(ObjectPtr py_object) {
        return PyTag_Check(py_object);
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeTuple(const std::vector<ObjectSharedPtr> &values)
    {
        auto result = Py_OWN(PyTuple_New(values.size()));
        for (std::size_t i = 0; i < values.size(); ++i) {
            PySafeTuple_SetItem(*result, i, values[i]);
        }
        return result;
    }
    
    PyToolkit::ObjectSharedPtr PyToolkit::makeTuple(std::vector<ObjectSharedPtr> &&values)
    {
        auto result = Py_OWN(PyTuple_New(values.size()));
        for (std::size_t i = 0; i < values.size(); ++i) {
            PySafeTuple_SetItem(*result, i, values[i]);
        }
        return result;
    }
    
    PyToolkit::ObjectPtr *PyToolkit::unpackTuple(ObjectPtr py_tuple)
    {
        if (!PyTuple_Check(py_tuple)) {
            THROWF(db0::InputException) << "Invalid type in unpackTuple";
        }
        return reinterpret_cast<PyTupleObject *>(py_tuple)->ob_item;
    }

    bool PyToolkit::isValid() {
        return Py_IsInitialized();
    }
        
    bool PyToolkit::hasTagRefs(ObjectPtr obj_ptr)
    {
        assert(PyAnyMemo_Check(obj_ptr));
        return reinterpret_cast<MemoAnyObject*>(obj_ptr)->ext().hasTagRefs();
    }
    
    std::unique_ptr<GIL_Lock> PyToolkit::ensureLocked()
    {
        if (!Py_IsInitialized()) {
            return {};
        }
        return std::make_unique<GIL_Lock>();
    }
    
    bool PyToolkit::isValid(ObjectPtr py_object) {
        return py_object != nullptr;
    }
    
    template <typename MemoImplT>
    void incRefMemoImpl(bool is_tag, MemoImplT *memo_obj)
    {
        memo_obj->modifyExt().incRef(is_tag);
    }

    void PyToolkit::incRefMemo(bool is_tag, ObjectPtr py_object)
    {
        if (PyEmbeddedMemo_Check(py_object)) {
            incEmbeddedMemoRef(py_object, is_tag);
        } else if (PyMemo_Check<MemoObject>(py_object)) {
            incRefMemoImpl<MemoObject>(is_tag, reinterpret_cast<MemoObject*>(py_object));
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            incRefMemoImpl<MemoImmutableObject>(is_tag, reinterpret_cast<MemoImmutableObject*>(py_object));
        } else {
            assert(false);
            THROWF(db0::InputException) << "Invalid memo object type for incRefMemo" << THROWF_END;
        }
    }

    template <typename MemoImplT>
    bool decRefMemoImpl(bool is_tag, MemoImplT *memo_obj)
    {
        auto &memo = memo_obj->modifyExt();        
        memo.decRef(is_tag);
        return !memo.hasRefs();
    }

    bool PyToolkit::decRefMemo(bool is_tag, ObjectPtr py_object)
    {
        if (PyEmbeddedMemo_Check(py_object)) {
            auto *rootObject = getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(py_object)).rootObject();
            return decRefMemoImpl<MemoImmutableObject>(is_tag, reinterpret_cast<MemoImmutableObject*>(rootObject));
        } else if (PyMemo_Check<MemoObject>(py_object)) {
            return decRefMemoImpl<MemoObject>(is_tag, reinterpret_cast<MemoObject*>(py_object));
        } else if (PyMemo_Check<MemoImmutableObject>(py_object)) {
            return decRefMemoImpl<MemoImmutableObject>(is_tag, reinterpret_cast<MemoImmutableObject*>(py_object));
        } else {
            assert(false);
            THROWF(db0::InputException) << "Invalid memo object type for decRefMemo" << THROWF_END;
        }
    }

}
