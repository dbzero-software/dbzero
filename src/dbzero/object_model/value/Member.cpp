#include "Member.hpp"
#include <dbzero/core//serialization/Serializable.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/types/PyDecimal.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/object_model/value/long_weak_ref.hpp>

namespace db0::object_model

{
    
    template <typename T> void assureSameFixture(db0::swine_ptr<Fixture> &fixture, T &object,
        bool auto_harden = true)
    {
        if (*fixture != *object.getFixture()) {
            if (object.getRefCount() > 0 || !auto_harden) {
                THROWF(db0::InputException) << "Creating strong reference failed: object from a different prefix" << THROWF_END;
            }
            // auto-harden instead of taking a weak reference
            object.moveTo(fixture);
        }
    }
    
    // INTEGER specialization
    template <> Value createMember<TypeId::INTEGER, PyToolkit>(db0::swine_ptr<Fixture> &fixture, 
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto int_value = PyLong_AsLong(obj_ptr);
        return db0::binary_cast<std::uint64_t, std::int64_t>()(int_value);
    }
    
    // FLOAT specialization
    template <>  Value createMember<TypeId::FLOAT, PyToolkit>(db0::swine_ptr<Fixture> &fixture, 
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto fp_value = PyFloat_AsDouble(obj_ptr);
        return db0::binary_cast<std::uint64_t, double>()(fp_value);
    }

    // STRING specialization
    template <> Value createMember<TypeId::STRING, PyToolkit>(db0::swine_ptr<Fixture> &fixture, 
        PyObjectPtr obj_ptr, StorageClass)
    {
        // create string-ref member and take its address
        return db0::v_object<db0::o_string>(*fixture, PyUnicode_AsUTF8(obj_ptr)).getAddress();
    }
    
    // OBJECT specialization
    template <> Value createMember<TypeId::MEMO_OBJECT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &obj = PyToolkit::getTypeManager().extractMutableObject(obj_ptr);
        assert(obj.hasInstance());
        assureSameFixture(fixture, obj);
        obj.modify().incRef();
        return obj.getAddress();
    }

    // LIST specialization
    template <> Value createMember<TypeId::DB0_LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &list = PyToolkit::getTypeManager().extractMutableList(obj_ptr);
        assureSameFixture(fixture, list);
        list.modify().incRef();
        return list.getAddress();
    }
    
    // INDEX specialization
    template <> Value createMember<TypeId::DB0_INDEX, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &index = PyToolkit::getTypeManager().extractMutableIndex(obj_ptr);
        assureSameFixture(fixture, index);
        index.incRef();
        return index.getAddress();
    }

    // SET specialization
    template <> Value createMember<TypeId::DB0_SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &set = PyToolkit::getTypeManager().extractMutableSet(obj_ptr);
        assureSameFixture(fixture, set);
        set.incRef();
        return set.getAddress();
    }

    // DB0 DICT specialization
    template <> Value createMember<TypeId::DB0_DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &dict = PyToolkit::getTypeManager().extractMutableDict(obj_ptr);
        assureSameFixture(fixture, dict);
        dict.incRef();
        return dict.getAddress();
    }

    // TUPLE specialization
    template <> Value createMember<TypeId::DB0_TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &tuple = PyToolkit::getTypeManager().extractMutableTuple(obj_ptr);
        assureSameFixture(fixture, tuple);
        tuple.incRef();
        return tuple.getAddress();
    }
    
    // LIST specialization
    template <> Value createMember<TypeId::LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto list_ptr = db0::python::tryMake_DB0List(fixture, &obj_ptr, 1);
        if (!list_ptr) {
            THROWF(db0::InputException) << "Failed to create list" << THROWF_END;
        }
        list_ptr.get()->modifyExt().modify().incRef();
        return list_ptr.get()->ext().getAddress();
    }
    
    // SET specialization
    template <> Value createMember<TypeId::SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto set = db0::python::tryMake_DB0Set(fixture, &obj_ptr, 1);
        if (!set) {
            THROWF(db0::InputException) << "Failed to create set" << THROWF_END;
        }
        set.get()->modifyExt().incRef();
        return set.get()->ext().getAddress();
    }
    
    // DICT specialization
    template <> Value createMember<TypeId::DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        PyObject *args = PyTuple_New(1);
        Py_INCREF(obj_ptr);
        PyTuple_SetItem(args, 0, obj_ptr);
        auto dict = db0::python::tryMake_DB0Dict(fixture, args, nullptr);
        Py_DECREF(args);
        if (!dict) {
            THROWF(db0::InputException) << "Failed to create dict" << THROWF_END;
        }
        dict.get()->modifyExt().incRef();
        return dict.get()->ext().getAddress();
    }
    
    // TUPLE specialization
    template <> Value createMember<TypeId::TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto tuple = db0::python::tryMake_DB0Tuple(fixture, &obj_ptr, 1);
        tuple.get()->modifyExt().incRef();
        return tuple.get()->ext().getAddress();
    }
    
    // DATETIME with TIMEZONE specialization
    template <> Value createMember<TypeId::DATETIME_TZ, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        return db0::python::pyDateTimeWithTzToUint64(obj_ptr);
    }

    // DATETIME specialization
    template <> Value createMember<TypeId::DATETIME, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {   
        return db0::python::pyDateTimeToToUint64(obj_ptr);
    }

     // DATE specialization
    template <> Value createMember<TypeId::DATE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        return db0::python::pyDateToUint64(obj_ptr);
    }

    // TIME specialization
    template <> Value createMember<TypeId::TIME, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        return db0::python::pyTimeToUint64(obj_ptr);
    }

    // TIME wit TIMEZONE specialization
    template <> Value createMember<TypeId::TIME_TZ, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        return db0::python::pyTimeWithTzToUint64(obj_ptr);
    }

    // DECIMAL specialization
    template <> Value createMember<TypeId::DECIMAL, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {   
        return db0::python::pyDecimalToUint64(obj_ptr);
    }

    Value createBytesMember(db0::swine_ptr<Fixture> &fixture, const std::byte *bytes, std::size_t size) {
        // FIXME: implement as ObjectBase and incRef
        return db0::v_object<db0::o_binary>(*fixture, bytes, size).getAddress();
    }

    // BYTES specialization
    template <> Value createMember<TypeId::BYTES, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto size = PyBytes_GET_SIZE(obj_ptr);
        auto *str = PyBytes_AsString(obj_ptr);
        return createBytesMember(fixture, reinterpret_cast<std::byte *>(str), size);
    }

    // NONE specialization
    template <> Value createMember<TypeId::NONE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        return 0;
    }
    
    // OBJECT_ITERABLE specialization (serialized member)
    template <> Value createMember<TypeId::OBJECT_ITERABLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &obj_iter = PyToolkit::getTypeManager().extractObjectIterable(obj_ptr);
        std::vector<std::byte> bytes;
        // put TypeId as a header
        db0::serial::write(bytes, TypeId::OBJECT_ITERABLE);
        obj_iter.serialize(bytes);
        return createBytesMember(fixture, bytes.data(), bytes.size());
    }
    
    // ENUM value specialization (serialized member)
    template <> Value createMember<TypeId::DB0_ENUM_VALUE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto enum_value = PyToolkit::getTypeManager().extractEnumValue(obj_ptr);
        // make sure value from the same Fixture is assigned
        assert(enum_value);
        if (*enum_value.m_fixture != *fixture) {
            // migrate enum value to the destination fixture
            enum_value = fixture->get<EnumFactory>().migrateEnumValue(enum_value);
        }
        return enum_value.getUID().asULong();
    }
    
    // ENUM value-repr specialization (serialized member)
    template <> Value createMember<TypeId::DB0_ENUM_VALUE_REPR, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &enum_value_repr = PyToolkit::getTypeManager().extractEnumValueRepr(obj_ptr);
        // convert enum value-repr to enum value
        auto enum_value = fixture->get<EnumFactory>().getEnumValue(enum_value_repr);
        return enum_value.getUID().asULong();
    }

    template <> Value createMember<TypeId::BOOLEAN, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        return obj_ptr == Py_True ? 1 : 0;
    }
    
    // DB0_BYTES_ARRAY specialization
    template <> Value createMember<TypeId::DB0_BYTES_ARRAY, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass)
    {
        auto &byte_array = PyToolkit::getTypeManager().extractMutableByteArray(obj_ptr);
        assureSameFixture(fixture, byte_array);
        byte_array.modify().incRef();
        return byte_array.getAddress();
    }
    
    // DB0_WEAK_PROXY specialization
    template <> Value createMember<TypeId::DB0_WEAK_PROXY, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass storage_class)
    {
        // NOTE: memo object can be extracted from the weak proxy
        const auto &obj = PyToolkit::getTypeManager().extractObject(obj_ptr);
        if (storage_class == StorageClass::OBJECT_LONG_WEAK_REF) {
            LongWeakRef weak_ref(fixture, obj);
            return weak_ref.getAddress();
        } else {
            // short weak ref
            return obj.getUniqueAddress();
        }
    }
    
    template <> void registerCreateMemberFunctions<PyToolkit>(
        std::vector<Value (*)(db0::swine_ptr<Fixture> &, PyObjectPtr, StorageClass)> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::NONE)] = createMember<TypeId::NONE, PyToolkit>;
        functions[static_cast<int>(TypeId::INTEGER)] = createMember<TypeId::INTEGER, PyToolkit>;
        functions[static_cast<int>(TypeId::FLOAT)] = createMember<TypeId::FLOAT, PyToolkit>;
        functions[static_cast<int>(TypeId::STRING)] = createMember<TypeId::STRING, PyToolkit>;
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = createMember<TypeId::MEMO_OBJECT, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_LIST)] = createMember<TypeId::DB0_LIST, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_INDEX)] = createMember<TypeId::DB0_INDEX, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_SET)] = createMember<TypeId::DB0_SET, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_DICT)] = createMember<TypeId::DB0_DICT, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = createMember<TypeId::DB0_TUPLE, PyToolkit>;
        functions[static_cast<int>(TypeId::LIST)] = createMember<TypeId::LIST, PyToolkit>;
        functions[static_cast<int>(TypeId::SET)] = createMember<TypeId::SET, PyToolkit>;
        functions[static_cast<int>(TypeId::DICT)] = createMember<TypeId::DICT, PyToolkit>;
        functions[static_cast<int>(TypeId::TUPLE)] = createMember<TypeId::TUPLE, PyToolkit>;
        functions[static_cast<int>(TypeId::DATETIME)] = createMember<TypeId::DATETIME, PyToolkit>;
        functions[static_cast<int>(TypeId::DATETIME_TZ)] = createMember<TypeId::DATETIME_TZ, PyToolkit>;
        functions[static_cast<int>(TypeId::DECIMAL)] = createMember<TypeId::DECIMAL, PyToolkit>;
        functions[static_cast<int>(TypeId::DATE)] = createMember<TypeId::DATE, PyToolkit>;
        functions[static_cast<int>(TypeId::TIME)] = createMember<TypeId::TIME, PyToolkit>;
        functions[static_cast<int>(TypeId::TIME_TZ)] = createMember<TypeId::TIME_TZ, PyToolkit>;
        functions[static_cast<int>(TypeId::BYTES)] = createMember<TypeId::BYTES, PyToolkit>; 
        functions[static_cast<int>(TypeId::OBJECT_ITERABLE)] = createMember<TypeId::OBJECT_ITERABLE, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE)] = createMember<TypeId::DB0_ENUM_VALUE, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE_REPR)] = createMember<TypeId::DB0_ENUM_VALUE_REPR, PyToolkit>;
        functions[static_cast<int>(TypeId::BOOLEAN)] = createMember<TypeId::BOOLEAN, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_BYTES_ARRAY)] = createMember<TypeId::DB0_BYTES_ARRAY, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_WEAK_PROXY)] = createMember<TypeId::DB0_WEAK_PROXY, PyToolkit>;
    }
    
    // STRING_REF specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::STRING_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        db0::v_object<db0::o_string> string_ref(fixture->myPtr(value.asAddress()));
        auto str_ptr = string_ref->get();
        return PyUnicode_FromStringAndSize(str_ptr.get_raw(), str_ptr.size());
    }

    // INT64 specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::INT64, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyLong_FromLong(value.cast<std::int64_t>());
    }

    // FLOAT specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::FP_NUMERIC64, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyFloat_FromDouble(value.cast<double>());
    }

    // OBJECT_REF specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::OBJECT_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        auto &class_factory = fixture->template get<ClassFactory>();
        return PyToolkit::unloadObject(fixture, value.asAddress(), class_factory);
    }

    // DB0_LIST specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_LIST, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadList(fixture, value.asAddress());
    }
    
    // DB0_INDEX specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_INDEX, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadIndex(fixture, value.asAddress());
    }

    // DB0_SET specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SET, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadSet(fixture, value.asAddress());
    }

    // DB0_DICT specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_DICT, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadDict(fixture, value.asAddress());
    }

    // BYTES specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BYTES, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        db0::v_object<db0::o_binary> bytes = fixture->myPtr(value.asAddress());
        auto bytes_ptr = bytes->getBuffer();
        return PyBytes_FromStringAndSize(reinterpret_cast<const char *>(bytes_ptr), bytes->size());
    }
    
    // DATETIME specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATETIME, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return db0::python::uint64ToPyDatetime(value.cast<std::uint64_t>());
    }

    // DATETIME with TZ specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATETIME_TZ, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return db0::python::uint64ToPyDatetimeWithTZ(value.cast<std::uint64_t>());
    }

    // DATE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return db0::python::uint64ToPyDate(value.cast<std::uint64_t>());
    }

    // Time specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::TIME, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return db0::python::uint64ToPyTime(value.cast<std::uint64_t>());
    }

    // Time with Timezone specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::TIME_TZ, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return db0::python::uint64ToPyTimeWithTz(value.cast<std::uint64_t>());
    }

    // DECIMAL specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DECIMAL, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return db0::python::uint64ToPyDecimal(value.cast<std::uint64_t>());
    }

    // NONE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::NONE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        Py_RETURN_NONE;
    }

    // DB0_TUPLE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_TUPLE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadTuple(fixture, value.asAddress());
    }

    // DB0_SERIALIZED specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SERIALIZED, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        db0::v_object<db0::o_binary> bytes = fixture->myPtr(value.asAddress());
        std::vector<std::byte> buffer;
        buffer.resize(bytes->size());
        std::copy(bytes->getBuffer(), bytes->getBuffer() + bytes->size(), buffer.begin());
        auto iter = buffer.cbegin(), end = buffer.cend();
        auto type_id = db0::serial::read<TypeId>(iter, end);
        if (type_id == TypeId::OBJECT_ITERABLE) {
            return PyToolkit::unloadObjectIterable(fixture, iter, end);
        } else {
            THROWF(db0::InputException) << "Unsupported serialized type id: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
    }
    
    // ENUM value specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_ENUM_VALUE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        auto &enum_factory = fixture->get<EnumFactory>();
        auto enum_value_uid = EnumValue_UID(value.cast<std::uint64_t>());
        return enum_factory.getEnumByUID(enum_value_uid.m_enum_uid)->getLangValue(enum_value_uid).steal();
    }
    
    // ENUM value specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::BOOLEAN, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
         return value.cast<std::uint64_t>() ? Py_True : Py_False;
    }
    
    // DB0_BYTES_ARRAY specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BYTES_ARRAY, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadByteArray(fixture, value.asAddress());
    }
    
    // OBJECT_WEAK_REF
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::OBJECT_WEAK_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        auto address = value.asUniqueAddress();
        if (PyToolkit::isObjectExpired(fixture, address.getAddress(), address.getInstanceId())) {
            // NOTE: expired objects are unloaded as MemoExpiredRef (placeholders)
            return PyToolkit::unloadExpiredRef(fixture, fixture->getUUID(), address);
        } else {
            return PyToolkit::unloadObject(fixture, address);
        }
    }
    
    // OBJECT_LONG_WEAK_REF
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::OBJECT_LONG_WEAK_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        LongWeakRef weak_ref(fixture, value.asAddress());
        auto other_fixture = fixture->getWorkspace().getFixture(weak_ref->m_fixture_uuid);
        auto address = weak_ref->m_address;
        if (PyToolkit::isObjectExpired(other_fixture, address.getAddress(), address.getInstanceId())) {
            // NOTE: expired objects are unloaded as MemoExpiredRef (placeholders)
            return PyToolkit::unloadExpiredRef(fixture, weak_ref);
        } else {
            // unload object from a foreign prefix
            return PyToolkit::unloadObject(other_fixture, address);
        }
    }
    
    template <> void registerUnloadMemberFunctions<PyToolkit>(
        std::vector<typename PyToolkit::ObjectSharedPtr (*)(db0::swine_ptr<Fixture> &, Value, const char *)> &functions)
    {
        functions.resize(static_cast<int>(StorageClass::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(StorageClass::NONE)] = unloadMember<StorageClass::NONE, PyToolkit>;
        functions[static_cast<int>(StorageClass::INT64)] = unloadMember<StorageClass::INT64, PyToolkit>;
        functions[static_cast<int>(StorageClass::FP_NUMERIC64)] = unloadMember<StorageClass::FP_NUMERIC64, PyToolkit>;
        functions[static_cast<int>(StorageClass::STRING_REF)] = unloadMember<StorageClass::STRING_REF, PyToolkit>;
        functions[static_cast<int>(StorageClass::OBJECT_REF)] = unloadMember<StorageClass::OBJECT_REF, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_LIST)] = unloadMember<StorageClass::DB0_LIST, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_INDEX)] = unloadMember<StorageClass::DB0_INDEX, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_SET)] = unloadMember<StorageClass::DB0_SET, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_DICT)] = unloadMember<StorageClass::DB0_DICT, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_TUPLE)] = unloadMember<StorageClass::DB0_TUPLE, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_BYTES)] = unloadMember<StorageClass::DB0_BYTES, PyToolkit>;
        functions[static_cast<int>(StorageClass::DATETIME)] = unloadMember<StorageClass::DATETIME, PyToolkit>;
        functions[static_cast<int>(StorageClass::DATETIME_TZ)] = unloadMember<StorageClass::DATETIME_TZ, PyToolkit>;
        functions[static_cast<int>(StorageClass::DECIMAL)] = unloadMember<StorageClass::DECIMAL, PyToolkit>;
        functions[static_cast<int>(StorageClass::TIME)] = unloadMember<StorageClass::TIME, PyToolkit>;
        functions[static_cast<int>(StorageClass::TIME_TZ)] = unloadMember<StorageClass::TIME_TZ, PyToolkit>;
        functions[static_cast<int>(StorageClass::DATE)] = unloadMember<StorageClass::DATE, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_SERIALIZED)] = unloadMember<StorageClass::DB0_SERIALIZED, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_ENUM_VALUE)] = unloadMember<StorageClass::DB0_ENUM_VALUE, PyToolkit>;
        functions[static_cast<int>(StorageClass::BOOLEAN)] = unloadMember<StorageClass::BOOLEAN, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_BYTES_ARRAY)] = unloadMember<StorageClass::DB0_BYTES_ARRAY, PyToolkit>;
        functions[static_cast<int>(StorageClass::OBJECT_WEAK_REF)] = unloadMember<StorageClass::OBJECT_WEAK_REF, PyToolkit>;
        functions[static_cast<int>(StorageClass::OBJECT_LONG_WEAK_REF)] = unloadMember<StorageClass::OBJECT_LONG_WEAK_REF, PyToolkit>;
    }
    
    template <typename T, typename LangToolkit>
    void unrefObjectBase(db0::swine_ptr<Fixture> &fixture, Address address)
    {
        auto obj_ptr = fixture->getLangCache().get(address);
        if (obj_ptr) {
            db0::FixtureLock lock(fixture);
            // decref cached instance via language specific wrapper type
            auto lang_wrapper = LangToolkit::template getWrapperTypeOf<T>(obj_ptr.get());
            lang_wrapper->modifyExt().decRef();
        } else {
            T object(fixture, address);
            object.decRef();
            // member will be deleted by GC0 if its ref-count = 0
        }
    }
    
    // OBJECT_REF specialization
    template <> void unrefMember<StorageClass::OBJECT_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        unrefObjectBase<Object, PyToolkit>(fixture, value.asAddress());
    }
    
    template <> void unrefMember<StorageClass::DB0_LIST, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value) 
    {
        unrefObjectBase<List, PyToolkit>(fixture, value.asAddress());
    }

    template <> void unrefMember<StorageClass::DB0_INDEX, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value) 
    {
        unrefObjectBase<Index, PyToolkit>(fixture, value.asAddress());
    }

    template <> void unrefMember<StorageClass::DB0_SET, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value) 
    {
        unrefObjectBase<Set, PyToolkit>(fixture, value.asAddress());
    }

    template <> void unrefMember<StorageClass::DB0_DICT, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        unrefObjectBase<Dict, PyToolkit>(fixture, value.asAddress());
    }

    template <> void unrefMember<StorageClass::DB0_TUPLE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        unrefObjectBase<Tuple, PyToolkit>(fixture, value.asAddress());
    }
    
    template <> void unrefMember<StorageClass::DB0_SERIALIZED, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        throw std::runtime_error("Not implemented");
    }

    template <> void unrefMember<StorageClass::DB0_BYTES_ARRAY, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        unrefObjectBase<ByteArray, PyToolkit>(fixture, value.asAddress());
    }
    
    template <> void registerUnrefMemberFunctions<PyToolkit>(
        std::vector<void (*)(db0::swine_ptr<Fixture> &, Value)> &functions)
    {
        functions.resize(static_cast<int>(StorageClass::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(StorageClass::OBJECT_REF)] = unrefMember<StorageClass::OBJECT_REF, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_LIST)] = unrefMember<StorageClass::DB0_LIST, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_INDEX)] = unrefMember<StorageClass::DB0_INDEX, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_SET)] = unrefMember<StorageClass::DB0_SET, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_DICT)] = unrefMember<StorageClass::DB0_DICT, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_TUPLE)] = unrefMember<StorageClass::DB0_TUPLE, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_BYTES_ARRAY)] = unrefMember<StorageClass::DB0_BYTES_ARRAY, PyToolkit>;
        // FIXME: uncomment and refactor when handling of BYTES is fixed (same storage)
        // functions[static_cast<int>(StorageClass::DB0_SERIALIZED)] = unrefMember<StorageClass::DB0_SERIALIZED, PyToolkit>;
    }
    
    bool isMaterialized(PyObjectPtr obj_ptr)
    {
        auto object_ptr = PyToolkit::getTypeManager().tryExtractObject(obj_ptr);
        return !object_ptr || object_ptr->hasInstance();
    }
    
    void materialize(FixtureLock &fixture, PyObjectPtr obj_ptr)
    {
        auto object_ptr = PyToolkit::getTypeManager().tryExtractMutableObject(obj_ptr);
        if (object_ptr && !object_ptr->hasInstance()) {
            object_ptr->postInit(fixture);
        }
    }

}