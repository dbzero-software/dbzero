// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Types.hpp"
#include "PyToolkit.hpp"
#include "Memo.hpp"
#include "MemoExpiredRef.hpp"
#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include <dbzero/bindings/python/types/PyEnum.hpp>
#include <dbzero/bindings/python/types/PyObjectId.hpp>
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyIndex.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterable.hpp>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Workspace.hpp>

namespace db0::python

{

    using Serializable = db0::serial::Serializable;
    template <TypeId type_id> db0::swine_ptr<Fixture> getFixtureOf(PyObject*);
    template <TypeId type_id> PyObject *tryGetUUID(PyObject*);

    // OBJECT specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::MEMO_OBJECT>(PyObject *py_value) {
        return reinterpret_cast<MemoObject*>(py_value)->ext().getFixture();
    }

    // LIST specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::DB0_LIST>(PyObject *py_value) {
        return reinterpret_cast<ListObject*>(py_value)->ext().getFixture();
    }

    // DICT specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::DB0_DICT>(PyObject *py_value) {
        return reinterpret_cast<DictObject*>(py_value)->ext().getFixture();
    }
    
    // SET specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::DB0_SET>(PyObject *py_value) {
        return reinterpret_cast<SetObject*>(py_value)->ext().getFixture();
    }

    // TUPLE specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::DB0_TUPLE>(PyObject *py_value) {
        return reinterpret_cast<TupleObject*>(py_value)->ext().getFixture();
    }

    // INDEX specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::DB0_INDEX>(PyObject *py_value) {
        return reinterpret_cast<IndexObject*>(py_value)->ext().getFixture();
    }

    // ENUM value specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::DB0_ENUM_VALUE>(PyObject *py_value) {
        return reinterpret_cast<PyEnumValue*>(py_value)->ext().m_fixture.safe_lock();
    }
    
    // OBJECT_ITERABLE value specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::OBJECT_ITERABLE>(PyObject *py_value) {
        return reinterpret_cast<PyObjectIterable*>(py_value)->ext().getFixture();
    }

    // OBJECT_ITERATOR value specialization
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::OBJECT_ITERATOR>(PyObject *py_value) {
        return reinterpret_cast<PyObjectIterator*>(py_value)->ext().getFixture();
    }
    
    void registerGetFixtureOfFunctions(std::vector<db0::swine_ptr<Fixture> (*)(PyObject*)> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = getFixtureOf<TypeId::MEMO_OBJECT>;
        functions[static_cast<int>(TypeId::DB0_LIST)] = getFixtureOf<TypeId::DB0_LIST>;
        functions[static_cast<int>(TypeId::DB0_DICT)] = getFixtureOf<TypeId::DB0_DICT>;
        functions[static_cast<int>(TypeId::DB0_SET)] = getFixtureOf<TypeId::DB0_SET>;
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = getFixtureOf<TypeId::DB0_TUPLE>;
        functions[static_cast<int>(TypeId::DB0_INDEX)] = getFixtureOf<TypeId::DB0_INDEX>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE)] = getFixtureOf<TypeId::DB0_ENUM_VALUE>;
        functions[static_cast<int>(TypeId::OBJECT_ITERABLE)] = getFixtureOf<TypeId::OBJECT_ITERABLE>;
        functions[static_cast<int>(TypeId::OBJECT_ITERATOR)] = getFixtureOf<TypeId::OBJECT_ITERATOR>;
    }
    
    db0::swine_ptr<Fixture> getFixtureOf(PyObject *object)
    {
        // create member function pointer
        using GetFixtureOfFunc = db0::swine_ptr<Fixture> (*)(PyObject*);
        static std::vector<GetFixtureOfFunc> get_fixture_of_functions;
        if (get_fixture_of_functions.empty()) {
            registerGetFixtureOfFunctions(get_fixture_of_functions);
        }

        auto type_id = PyToolkit::getTypeManager().getTypeId(object);
        assert(static_cast<int>(type_id) < get_fixture_of_functions.size());
        // not all types have a fixture
        if (!get_fixture_of_functions[static_cast<int>(type_id)]) {
            return {};
        }
        assert(get_fixture_of_functions[static_cast<int>(type_id)]);
        return get_fixture_of_functions[static_cast<int>(type_id)](object);
    }
    
    template <typename T> PyObject *tryGetUUIDOf(T *self)
    {
        auto &instance = self->ext();
        if (!instance.hasInstance()) {
            THROWF(db0::InputException) << "Cannot get UUID of an uninitialized object";
        }
        db0::object_model::ObjectId object_id;
        auto fixture = instance.getFixture();
        assert(fixture);
        object_id.m_fixture_uuid = fixture->getUUID();
        object_id.m_address = instance.getUniqueAddress();        
        object_id.m_storage_class = getStorageClass<T>();

        // return as base-32 string
        char buffer[ObjectId::maxEncodedSize() + 1];
        object_id.toBase32(buffer);
        return PyUnicode_FromString(buffer);
    }
    
    // Serializable's UUID implementation
    PyObject *tryGetSerializableUUID(const db0::serial::Serializable *self)
    {
        // return as base-32 string
        char buffer[db0::serial::Serializable::UUID_SIZE];
        self->getUUID(buffer);
        return PyUnicode_FromString(buffer);
    }

    // OBJECT specialization
    template <> PyObject *tryGetUUID<TypeId::MEMO_OBJECT>(PyObject *py_value) {
        return tryGetUUIDOf(reinterpret_cast<MemoObject*>(py_value));
    }

    // OBJECT_ITERABLE specialization
    template <> PyObject *tryGetUUID<TypeId::OBJECT_ITERABLE>(PyObject *py_value) {
        return tryGetSerializableUUID(&reinterpret_cast<PyObjectIterable*>(py_value)->ext());
    }
    
    // MEMO_EXPIRED_REF specialization
    template <> PyObject *tryGetUUID<TypeId::MEMO_EXPIRED_REF>(PyObject *py_value) 
    {
        auto &expired_ref = *reinterpret_cast<MemoExpiredRef*>(py_value);
        db0::object_model::ObjectId object_id;
        object_id.m_fixture_uuid = expired_ref.getFixtureUUID();
        object_id.m_address = expired_ref.getUniqueAddress();        
        object_id.m_storage_class = StorageClass::OBJECT_REF;

        // return as base-32 string
        char buffer[ObjectId::maxEncodedSize() + 1];
        object_id.toBase32(buffer);
        return PyUnicode_FromString(buffer);
    }
    
    void registerTryGetUUIDFunctions(std::vector<PyObject *(*)(PyObject*)> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        // NOTE: for security reasons we only allow UUID retrieval for a strictly limited set of types
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = tryGetUUID<TypeId::MEMO_OBJECT>;
        // the purpose of UUID here is to find identical queries
        functions[static_cast<int>(TypeId::OBJECT_ITERABLE)] = tryGetUUID<TypeId::OBJECT_ITERABLE>;
        // for expired refs UUIDs are still available
        functions[static_cast<int>(TypeId::MEMO_EXPIRED_REF)] = tryGetUUID<TypeId::MEMO_EXPIRED_REF>;
    }
    
    PyObject *tryGetUUID(PyObject *py_value)
    {
        // create member function pointer
        using TryGetUUIDFunc = PyObject *(*)(PyObject*);
        static std::vector<TryGetUUIDFunc> try_get_uuid_functions;
        if (try_get_uuid_functions.empty()) {
            registerTryGetUUIDFunctions(try_get_uuid_functions);
        }

        auto type_id = PyToolkit::getTypeManager().getTypeId(py_value);
        assert(static_cast<int>(type_id) < try_get_uuid_functions.size());
        // not all types have a fixture
        if (!try_get_uuid_functions[static_cast<int>(type_id)]) {
            THROWF(db0::InputException) << "Unable to generate UUID for: " << py_value->ob_type->tp_name << " type";
        }
        assert(try_get_uuid_functions[static_cast<int>(type_id)]);
        return try_get_uuid_functions[static_cast<int>(type_id)](py_value);
    }
    
}