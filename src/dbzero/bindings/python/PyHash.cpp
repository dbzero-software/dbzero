// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyHash.hpp"
#include <Python.h>
#include <cstring>
#include <string>
#include <vector>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/core/utils/hash_func.hpp>

namespace db0::python

{

    using PyHashFunct = std::int64_t (*)(db0::swine_ptr<Fixture> &, PyObject *);
    
    template <> std::int64_t getPyHashImpl<TypeId::STRING>(db0::swine_ptr<Fixture> &, PyObject *key)
    {
        auto unicode_value = PyUnicode_AsUTF8(key);
        return murmurhash64A(unicode_value, std::strlen(unicode_value));
    }

    template <> std::int64_t getPyHashImpl<TypeId::BYTES>(db0::swine_ptr<Fixture> &, PyObject *key)
    {
        auto bytes_value = PyBytes_AsString(key);
        return murmurhash64A(bytes_value, std::strlen(bytes_value));
    }
    
    template <> std::int64_t getPyHashImpl<TypeId::TUPLE>(db0::swine_ptr<Fixture> &fixture, PyObject *key)
    {
        auto tuple_size = PyTuple_Size(key);
        int64_t hash = 0;
        for (int i = 0; i < tuple_size; ++i) {
            auto item = PyTuple_GetItem(key, i);
            hash ^= getPyHash(fixture, item);
        }
        return hash;
    }
    
    template <> std::int64_t getPyHashImpl<TypeId::DB0_TUPLE>(db0::swine_ptr<Fixture> &fixture, PyObject *key)
    {
        TupleObject *tuple_obj = reinterpret_cast<TupleObject*>(key);
        std::int64_t hash = 0;
        for (std::size_t i = 0; i < tuple_obj->ext().getData()->size(); ++i) {
            auto item = tuple_obj->ext().getItem(i);
            hash ^= getPyHash(fixture, *item);
        }
        return hash;
    }
    
    template <> std::int64_t getPyHashImpl<TypeId::DB0_ENUM_VALUE>(db0::swine_ptr<Fixture> &, PyObject *key) {
        return PyToolkit::getTypeManager().extractEnumValue(key).getPermHash();
    }

    template <> std::int64_t getPyHashImpl<TypeId::DB0_ENUM_VALUE_REPR>(db0::swine_ptr<Fixture> &, PyObject *key) {
        return PyToolkit::getTypeManager().extractEnumValueRepr(key).getPermHash();
    }

    template <> std::int64_t getPyHashImpl<TypeId::MEMO_OBJECT>(db0::swine_ptr<Fixture> &, PyObject *key)
    {
        auto &obj = reinterpret_cast<MemoObject*>(key)->ext();
        if (!obj.hasInstance()) {
            THROWF(db0::InputException) << "Memo object is not initialized" << THROWF_END;
        }
        return obj.getAddress().getValue();
    }
    
    // MEMO_TYPE specialization
    template <> std::int64_t getPyHashImpl<TypeId::MEMO_TYPE>(db0::swine_ptr<Fixture> &fixture, PyObject *obj_ptr)
    {
        auto py_type = reinterpret_cast<PyTypeObject*>(obj_ptr);
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        auto type = class_factory.tryGetExistingType(py_type);
        if (!type) {
            auto &type_manager = PyToolkit::getTypeManager();
            auto &memo_type_info = type_manager.getMemoTypeDecoration(py_type);
            // for scoped types must validate if the prefix is matching
            if (memo_type_info.isScoped()) {
                if (fixture->tryGetPrefixName() != memo_type_info.getPrefixName()) {
                    THROWF(db0::InputException) << "Unable to access scoped type from a different prefix: "
                        << type_manager.getLangTypeName(py_type);
                }
            }

            FixtureLock lock(fixture);
            type = class_factory.getOrCreateType(py_type);
        }
        return type->getAddress().getValue();
    }

    std::int64_t getPyHashImpl_for_simple_obj(db0::swine_ptr<Fixture> &, PyObject *key) {
        return PyToolkit::getTypeManager().extractUInt64(key);
    }

    std::int64_t getPyHashDefaultImpl(db0::swine_ptr<Fixture> &, PyObject *key) {
        return PyObject_Hash(key);
    }

    void registerGetHashFunctions(std::vector<PyHashFunct> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::STRING)] = getPyHashImpl<TypeId::STRING>;
        functions[static_cast<int>(TypeId::BYTES)]  = getPyHashImpl<TypeId::BYTES>;
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = getPyHashImpl<TypeId::DB0_TUPLE>;
        functions[static_cast<int>(TypeId::TUPLE)] = getPyHashImpl<TypeId::TUPLE>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE)] = getPyHashImpl<TypeId::DB0_ENUM_VALUE>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE_REPR)] = getPyHashImpl<TypeId::DB0_ENUM_VALUE_REPR>;
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = getPyHashImpl<TypeId::MEMO_OBJECT>;
        functions[static_cast<int>(TypeId::MEMO_TYPE)] = getPyHashImpl<TypeId::MEMO_TYPE>;
        functions[static_cast<int>(TypeId::DATETIME)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::DATETIME_TZ)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::DATE)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::TIME)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::TIME_TZ)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::INTEGER)] = getPyHashImpl_for_simple_obj;
        functions[static_cast<int>(TypeId::DECIMAL)] = getPyHashImpl_for_simple_obj;
    }

    PyObject* getPyHashAsPyObject(db0::swine_ptr<Fixture> &fixture, PyObject *key) {
        return PyLong_FromLong(getPyHash(fixture, key));
    }
    
    std::int64_t getPyHash(db0::swine_ptr<Fixture> &fixture, PyObject *key)
    {
        static std::vector<PyHashFunct> getPyHash_functions;
        if (getPyHash_functions.empty()) {
            registerGetHashFunctions(getPyHash_functions);
        }

        auto type_id = PyToolkit::getTypeManager().getTypeId(key);
        assert(static_cast<int>(type_id) < getPyHash_functions.size());
        auto func_ptr = getPyHash_functions[static_cast<int>(type_id)];
        if (!func_ptr) {
            return getPyHashDefaultImpl(fixture, key);
        }
        return func_ptr(fixture, key);
    }
    
    std::optional<std::pair<std::int64_t, ObjectSharedPtr> > getPyHashIfExists(
        db0::swine_ptr<Fixture> &fixture, PyObject *obj_ptr)
    {
        // if not EnumValueRepr then simply calcualate
        if (!PyEnumValueRepr_Check(obj_ptr)) {
            return std::make_pair(getPyHash(fixture, obj_ptr), ObjectSharedPtr(obj_ptr));
        }
        
        // for EnumValueRepr we need to check if converstion to actual enum is possible        
        auto &enum_factory = fixture->template get<db0::object_model::EnumFactory>();
        auto lang_enum = enum_factory.tryGetEnumLangValue(reinterpret_cast<PyEnumValueRepr*>(obj_ptr)->ext());
        if (!lang_enum) {
            // corresponding EnumValue does not exist
            return std::nullopt;
        }
        return std::make_pair(getPyHash(fixture, *lang_enum), lang_enum);
    }

}