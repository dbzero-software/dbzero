#include "Types.hpp"
#include "PyToolkit.hpp"
#include "Memo.hpp"
#include "ObjectId.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/Dict.hpp>
#include <dbzero/bindings/python/collections/Tuple.hpp>
#include <dbzero/bindings/python/collections/Set.hpp>
#include <dbzero/bindings/python/collections/Index.hpp>
#include <dbzero/bindings/python/PyObjectIterator.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "PyEnum.hpp"

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
    template <> db0::swine_ptr<Fixture> getFixtureOf<TypeId::DB0_ENUM_VALUE>(PyObject *py_value) 
    {
        auto fixture_uuid = reinterpret_cast<PyEnumValue*>(py_value)->ext().m_fixture_uuid;
        return PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid);
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
        /**
        functions[static_cast<int>(TypeId::DB0_BLOCK)] = createMember<TypeId::DB0_BLOCK, PyToolkit>;                                        
        functions[static_cast<int>(TypeId::OBJECT_ITERATOR)] = createMember<TypeId::OBJECT_ITERATOR, PyToolkit>;
        
        */
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
        db0::object_model::ObjectId object_id;
        auto fixture = instance.getFixture();
        assert(fixture);
        object_id.m_fixture_uuid = fixture->getUUID();
        object_id.m_instance_id = instance->m_instance_id;
        object_id.m_typed_addr.setAddress(instance.getAddress());
        object_id.m_typed_addr.setType(getStorageClass<T>());

        // return as base-32 string
        char buffer[ObjectId::encodedSize() + 1];
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

    // LIST specialization
    template <> PyObject *tryGetUUID<TypeId::DB0_LIST>(PyObject *py_value) {
        return tryGetUUIDOf(reinterpret_cast<ListObject*>(py_value));
    }
    
    // INDEX specialization
    template <> PyObject *tryGetUUID<TypeId::DB0_INDEX>(PyObject *py_value) {
        return tryGetUUIDOf(reinterpret_cast<IndexObject*>(py_value));
    }

    // OBJECT_ITERATOR specialization
    template <> PyObject *tryGetUUID<TypeId::OBJECT_ITERATOR>(PyObject *py_value) {
        return tryGetSerializableUUID(&*reinterpret_cast<PyObjectIterator*>(py_value)->ext());
    }

    void registerTryGetUUIDFunctions(std::vector<PyObject *(*)(PyObject*)> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = tryGetUUID<TypeId::MEMO_OBJECT>;
        functions[static_cast<int>(TypeId::DB0_LIST)] = tryGetUUID<TypeId::DB0_LIST>;        
        functions[static_cast<int>(TypeId::DB0_INDEX)] = tryGetUUID<TypeId::DB0_INDEX>;        
        functions[static_cast<int>(TypeId::OBJECT_ITERATOR)] = tryGetUUID<TypeId::OBJECT_ITERATOR>;        
        /**
        functions[static_cast<int>(TypeId::DB0_DICT)] = tryGetUUID<TypeId::DB0_DICT>;
        functions[static_cast<int>(TypeId::DB0_SET)] = tryGetUUID<TypeId::DB0_SET>;        
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = createMember<TypeId::DB0_TUPLE>;
        functions[static_cast<int>(TypeId::DB0_BLOCK)] = createMember<TypeId::DB0_BLOCK, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE)] = createMember<TypeId::DB0_ENUM_VALUE, PyToolkit>;
        */
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
            THROWF(db0::InputException) << "Type " << py_value->ob_type->tp_name << " does not have a UUID";
        }
        assert(try_get_uuid_functions[static_cast<int>(type_id)]);
        return try_get_uuid_functions[static_cast<int>(type_id)](py_value);
    }
    
}