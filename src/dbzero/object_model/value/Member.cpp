// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Member.hpp"
#include <dbzero/core//serialization/Serializable.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/types/PyDecimal.hpp>
#include <dbzero/object_model/bytes/ByteArray.hpp>
#include <dbzero/object_model/value/long_weak_ref.hpp>
// FIXME: remove Python dependency
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/object/ObjectAnyImpl.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>

namespace db0::object_model

{
    
    template <typename T> void assureSameFixture(db0::swine_ptr<Fixture> &fixture, T &object,
        bool auto_harden = true)
    {
        if (*fixture != *object.getFixture()) {
            if (object.hasRefs() || !auto_harden) {
                THROWF(db0::InputException) << "Creating strong reference failed: object from a different prefix" << THROWF_END;
            }
            // auto-harden instead of taking a weak reference
            object.moveTo(fixture);
        }
    }
    
    // INTEGER specialization
    template <> Value createMember<TypeId::INTEGER, PyToolkit>(db0::swine_ptr<Fixture> &fixture, 
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto int_value = PyLong_AsLongLong(obj_ptr);
        return db0::binary_cast<std::uint64_t, std::int64_t>()(int_value);
    }
    
    // FLOAT specialization
    template <>  Value createMember<TypeId::FLOAT, PyToolkit>(db0::swine_ptr<Fixture> &fixture, 
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto fp_value = PyFloat_AsDouble(obj_ptr);
        return db0::binary_cast<std::uint64_t, double>()(fp_value);
    }

    // STRING specialization
    template <> Value createMember<TypeId::STRING, PyToolkit>(db0::swine_ptr<Fixture> &fixture, 
        PyObjectPtr obj_ptr, StorageClass, AccessFlags access_mode)
    {
        // create string-ref member and take its address
        return db0::v_object<db0::o_string>(*fixture, PyUnicode_AsUTF8(obj_ptr), access_mode).getAddress();
    }
    
    // OBJECT specialization (mutable or immutable)
    template <typename MemoObjectT> Value createObjectMember(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {                
        auto &obj = PyToolkit::getTypeManager().extractMutableObject<MemoObjectT>(obj_ptr);
        assert(obj.hasInstance());
        assureSameFixture(fixture, obj);
        obj.modify().incRef(false);
        return obj.getAddress();
    }

    // LIST specialization
    template <> Value createMember<TypeId::DB0_LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto &list = PyToolkit::getTypeManager().extractMutableList(obj_ptr);
        assureSameFixture(fixture, list);
        list.modify().incRef(false);
        return list.getAddress();
    }
    
    // INDEX specialization
    template <> Value createMember<TypeId::DB0_INDEX, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto &index = PyToolkit::getTypeManager().extractMutableIndex(obj_ptr);
        assureSameFixture(fixture, index);
        index.incRef(false);
        return index.getAddress();
    }
    
    // SET specialization
    template <> Value createMember<TypeId::DB0_SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto &set = PyToolkit::getTypeManager().extractMutableSet(obj_ptr);
        assureSameFixture(fixture, set);
        set.incRef(false);
        return set.getAddress();
    }

    // DB0 DICT specialization
    template <> Value createMember<TypeId::DB0_DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto &dict = PyToolkit::getTypeManager().extractMutableDict(obj_ptr);
        assureSameFixture(fixture, dict);
        dict.incRef(false);
        return dict.getAddress();
    }

    // TUPLE specialization
    template <> Value createMember<TypeId::DB0_TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto &tuple = PyToolkit::getTypeManager().extractMutableTuple(obj_ptr);
        assureSameFixture(fixture, tuple);
        tuple.incRef(false);
        return tuple.getAddress();
    }
    
    // LIST specialization
    template <> Value createMember<TypeId::LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags access_mode)
    {
        auto list_ptr = db0::python::tryMake_DB0List(fixture, &obj_ptr, 1, access_mode);
        if (!list_ptr) {
            THROWF(db0::InputException) << "Failed to create list" << THROWF_END;
        }
        list_ptr.get()->modifyExt().modify().incRef(false);
        return list_ptr.get()->ext().getAddress();
    }
    
    // SET specialization
    template <> Value createMember<TypeId::SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags access_mode)
    {
        auto set = db0::python::tryMake_DB0Set(fixture, &obj_ptr, 1, access_mode);
        if (!set) {
            THROWF(db0::InputException) << "Failed to create set" << THROWF_END;
        }
        set.get()->modifyExt().incRef(false);
        return set.get()->ext().getAddress();
    }
    
    // DICT specialization
    template <> Value createMember<TypeId::DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags access_mode)
    {
        auto args = Py_OWN(PyTuple_New(1));
        PySafeTuple_SetItem(*args, 0, Py_BORROW(obj_ptr));
        auto dict = db0::python::tryMake_DB0Dict(fixture, *args, nullptr, access_mode);
        if (!dict) {
            THROWF(db0::InputException) << "Failed to create dict" << THROWF_END;
        }
        dict.get()->modifyExt().incRef(false);
        return dict.get()->ext().getAddress();
    }
    
    // TUPLE specialization
    template <> Value createMember<TypeId::TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags access_mode)
    {
        auto tuple = db0::python::tryMake_DB0Tuple(fixture, &obj_ptr, 1, access_mode);
        tuple.get()->modifyExt().incRef(false);
        return tuple.get()->ext().getAddress();
    }
    
    // DATETIME with TIMEZONE specialization
    template <> Value createMember<TypeId::DATETIME_TZ, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        return db0::python::pyDateTimeWithTzToUint64(obj_ptr);
    }

    // DATETIME specialization
    template <> Value createMember<TypeId::DATETIME, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {   
        return db0::python::pyDateTimeToToUint64(obj_ptr);
    }

    // DATE specialization
    template <> Value createMember<TypeId::DATE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        return db0::python::pyDateToUint64(obj_ptr);
    }

    // TIME specialization
    template <> Value createMember<TypeId::TIME, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        return db0::python::pyTimeToUint64(obj_ptr);
    }

    // TIME wit TIMEZONE specialization
    template <> Value createMember<TypeId::TIME_TZ, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        return db0::python::pyTimeWithTzToUint64(obj_ptr);
    }

    // DECIMAL specialization
    template <> Value createMember<TypeId::DECIMAL, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {   
        return db0::python::pyDecimalToUint64(obj_ptr);
    }

    Value createBytesMember(db0::swine_ptr<Fixture> &fixture, const std::byte *bytes, std::size_t size,
        AccessFlags access_mode)
    {
        // FIXME: implement as ObjectBase and incRef
        return db0::v_object<db0::o_binary>(*fixture, bytes, size, access_mode).getAddress();
    }

    // BYTES specialization
    template <> Value createMember<TypeId::BYTES, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags access_mode)
    {
        auto size = PyBytes_GET_SIZE(obj_ptr);
        auto safe_str = PyBytes_AsString(obj_ptr);
        return createBytesMember(fixture, reinterpret_cast<std::byte *>(safe_str), size, access_mode);
    }
    
    // NONE specialization
    template <> Value createMember<TypeId::NONE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {        
        return Value::NONE;
    }
    
    // OBJECT_ITERABLE specialization (serialized member)
    template <> Value createMember<TypeId::OBJECT_ITERABLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags access_mode)
    {
        auto &obj_iter = PyToolkit::getTypeManager().extractObjectIterable(obj_ptr);
        std::vector<std::byte> bytes;
        // put TypeId as a header
        db0::serial::write(bytes, TypeId::OBJECT_ITERABLE);
        obj_iter.serialize(bytes);
        return createBytesMember(fixture, bytes.data(), bytes.size(), access_mode);
    }
    
    // ENUM value specialization (serialized member)
    template <> Value createMember<TypeId::DB0_ENUM_VALUE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto enum_value = PyToolkit::getTypeManager().extractEnumValue(obj_ptr);
        // make sure value from the same Fixture is assigned
        assert(enum_value);
        if (!db0::is_same(enum_value.m_fixture, fixture)) {
            // migrate enum value to the destination fixture
            enum_value = fixture->get<EnumFactory>().migrateEnumValue(enum_value);
        }
        return enum_value.getUID().asULong();
    }
    
    // ENUM value-repr specialization (serialized member)
    template <> Value createMember<TypeId::DB0_ENUM_VALUE_REPR, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto &enum_value_repr = PyToolkit::getTypeManager().extractEnumValueRepr(obj_ptr);
        // convert enum value-repr to enum value
        auto enum_value = fixture->get<EnumFactory>().getEnumValue(enum_value_repr);
        return enum_value.getUID().asULong();
    }
    
    template <> Value createMember<TypeId::BOOLEAN, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        // irrespective of the storage class
        return obj_ptr == Py_True ? Value::TRUE : Value::FALSE;
    }
    
    // DB0_BYTES_ARRAY specialization
    template <> Value createMember<TypeId::DB0_BYTES_ARRAY, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {
        auto &byte_array = PyToolkit::getTypeManager().extractMutableByteArray(obj_ptr);
        assureSameFixture(fixture, byte_array);
        byte_array.modify().incRef(false);
        return byte_array.getAddress();
    }
    
    // DB0_WEAK_PROXY specialization
    template <> Value createMember<TypeId::DB0_WEAK_PROXY, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass storage_class, AccessFlags)
    {
        // NOTE: memo object can be extracted from the weak proxy
        using MemoObject = PyToolkit::TypeManager::MemoObject;
        const auto &obj = PyToolkit::getTypeManager().extractObject<MemoObject>(obj_ptr);
        if (storage_class == StorageClass::OBJECT_LONG_WEAK_REF) {
            LongWeakRef weak_ref(fixture, obj);
            return weak_ref.getAddress();
        } else {
            // short weak ref
            return obj.getUniqueAddress();
        }
    }
    
    // MEMO_TYPE specialization
    template <> Value createMember<TypeId::MEMO_TYPE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr, StorageClass, AccessFlags)
    {    
        const auto &type_manager = PyToolkit::getTypeManager();
        auto lang_type = type_manager.getTypeObject(obj_ptr);
        auto &memo_type_info = type_manager.getMemoTypeDecoration(lang_type);

        // for scoped types must validate if the prefix is matching
        if (memo_type_info.isScoped()) {
            // scoped type, can only be referenced from the same fixture
            if (fixture->tryGetPrefixName() != memo_type_info.getPrefixName()) {
                THROWF(db0::InputException) << "Unable to create a reference to a scoped type from a different prefix: "
                    << type_manager.getLangTypeName(lang_type);
            }
        }
        
        // resolve class from the current fixture
        auto &class_factory = fixture->get<ClassFactory>();
        
        auto type = class_factory.tryGetExistingType(lang_type);
        if (!type) {
            // try creating type on the current fixture
            FixtureLock lock(fixture);
            type = class_factory.getOrCreateType(lang_type);
        }
        type->incRef(false);        
        return type->getUniqueAddress();
    }
    
    template <> void registerCreateMemberFunctions<PyToolkit>(
        std::vector<Value (*)(db0::swine_ptr<Fixture> &, PyObjectPtr, StorageClass, AccessFlags)> &functions)
    {
        using MemoObject = PyToolkit::TypeManager::MemoObject;
        using MemoImmutableObject = PyToolkit::TypeManager::MemoImmutableObject;
        
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::NONE)] = createMember<TypeId::NONE, PyToolkit>;
        functions[static_cast<int>(TypeId::INTEGER)] = createMember<TypeId::INTEGER, PyToolkit>;
        functions[static_cast<int>(TypeId::FLOAT)] = createMember<TypeId::FLOAT, PyToolkit>;
        functions[static_cast<int>(TypeId::STRING)] = createMember<TypeId::STRING, PyToolkit>;
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = createObjectMember<MemoObject>;
        functions[static_cast<int>(TypeId::MEMO_IMMUTABLE_OBJECT)] = createObjectMember<MemoImmutableObject>;
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
        functions[static_cast<int>(TypeId::MEMO_TYPE)] = createMember<TypeId::MEMO_TYPE, PyToolkit>;
    }
    
    // STRING_REF specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::STRING_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        db0::v_object<db0::o_string> string_ref(fixture->myPtr(value.asAddress()), access_mode);
        auto str_ptr = string_ref->get();
        auto result = Py_OWN(PyUnicode_FromStringAndSize(str_ptr.get_raw(), str_ptr.size()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to string" << THROWF_END;
        }
        return result;
    }
    
    // INT64 specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::INT64, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        return Py_OWN(PyLong_FromLongLong(value.cast<std::int64_t>()));
    }
    
    // FLOAT specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::FP_NUMERIC64, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        return Py_OWN(PyFloat_FromDouble(value.cast<double>()));
    }

    // OBJECT_REF specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::OBJECT_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto &class_factory = fixture->template get<ClassFactory>();
        return PyToolkit::unloadObject(fixture, value.asAddress(), class_factory);
    }
    
    // DB0_LIST specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_LIST, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        return PyToolkit::unloadList(fixture, value.asAddress(), 0, access_mode);
    }
    
    // DB0_INDEX specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_INDEX, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        return PyToolkit::unloadIndex(fixture, value.asAddress(), 0, access_mode);
    }

    // DB0_SET specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SET, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        return PyToolkit::unloadSet(fixture, value.asAddress(), 0, access_mode);
    }
    
    // DB0_DICT specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_DICT, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        return PyToolkit::unloadDict(fixture, value.asAddress(), 0, access_mode);
    }
    
    // BYTES specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BYTES, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        db0::v_object<db0::o_binary> bytes(fixture->myPtr(value.asAddress()), access_mode);
        auto bytes_ptr = bytes->getBuffer();
        auto result = Py_OWN(PyBytes_FromStringAndSize(reinterpret_cast<const char *>(bytes_ptr), bytes->size()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to bytes" << THROWF_END;
        }
        return result;
    }
    
    // DATETIME specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATETIME, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto result = Py_OWN(db0::python::uint64ToPyDatetime(value.cast<std::uint64_t>()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to Datetime" << THROWF_END;
        }
        return result;
    }

    // DATETIME with TZ specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATETIME_TZ, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto result = Py_OWN(db0::python::uint64ToPyDatetimeWithTZ(value.cast<std::uint64_t>()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to Datetime with TZ" << THROWF_END;
        }
        return result;
    }

    // DATE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto result = Py_OWN(db0::python::uint64ToPyDate(value.cast<std::uint64_t>()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to Date" << THROWF_END;
        }
        return result;
    }

    // Time specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::TIME, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto result = Py_OWN(db0::python::uint64ToPyTime(value.cast<std::uint64_t>()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to Time" << THROWF_END;
        }
        return result;
    }

    // Time with Timezone specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::TIME_TZ, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto result = Py_OWN(db0::python::uint64ToPyTimeWithTz(value.cast<std::uint64_t>()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to Time with TZ" << THROWF_END;
        }
        return result;
    }

    // DECIMAL specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DECIMAL, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto result = Py_OWN(db0::python::uint64ToPyDecimal(value.cast<std::uint64_t>()));
        if (!result) {
            THROWF(db0::InputException) << "Failed to convert to Decimal" << THROWF_END;
        }
        return result;
    }
    
    // NONE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::NONE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        Py_RETURN_NONE;
    }
    
    // DB0_TUPLE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_TUPLE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {    
        return PyToolkit::unloadTuple(fixture, value.asAddress(), 0, access_mode);
    }

    // DB0_SERIALIZED specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SERIALIZED, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        db0::v_object<db0::o_binary> bytes(fixture->myPtr(value.asAddress()), access_mode);
        std::vector<std::byte> buffer;
        buffer.resize(bytes->size());
        std::copy(bytes->getBuffer(), bytes->getBuffer() + bytes->size(), buffer.begin());
        auto iter = buffer.cbegin(), end = buffer.cend();
        auto type_id = db0::serial::read<TypeId>(iter, end);
        if (type_id == TypeId::OBJECT_ITERABLE) {
            return PyToolkit::deserializeObjectIterable(fixture, iter, end);
        } else {
            THROWF(db0::InputException) << "Unsupported serialized type id: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
    }
    
    // ENUM value specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_ENUM_VALUE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto &enum_factory = fixture->get<EnumFactory>();
        auto enum_value_uid = EnumValue_UID(value.cast<std::uint64_t>());
        return enum_factory.getEnumByUID(enum_value_uid.m_enum_uid)->getLangValue(enum_value_uid).steal();
    }
    
    // ENUM value specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::BOOLEAN, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        // NOTE: we use common constant encoding (0 = None, 1 = False, 2 = True)
        return Py_OWN(db0::python::PyBool_fromBool(value.cast<std::uint64_t>() == 2));
    }
    
    // DB0_BYTES_ARRAY specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BYTES_ARRAY, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags access_mode)
    {
        return PyToolkit::unloadByteArray(fixture, value.asAddress(), access_mode);
    }
    
    // OBJECT_WEAK_REF
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::OBJECT_WEAK_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto address = value.asUniqueAddress();
        // NOTE: instance_id not validated since it's a trusted reference
        if (PyToolkit::isExistingObject(fixture, address.getAddress())) {
            return PyToolkit::unloadObject(fixture, address);
        } else {
            // NOTE: expired objects are unloaded as MemoExpiredRef (placeholders)
            return PyToolkit::unloadExpiredRef(fixture, address.getAddress(), fixture->getUUID(), address);
        }
    }
    
    // OBJECT_LONG_WEAK_REF
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::OBJECT_LONG_WEAK_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        LongWeakRef weak_ref(fixture, value.asAddress());
        auto other_fixture = fixture->getWorkspace().getFixture(weak_ref->m_fixture_uuid);
        auto address = weak_ref->m_address;
        if (PyToolkit::isExistingObject(other_fixture, address.getAddress())) {
            // unload object from a foreign prefix
            return PyToolkit::unloadObject(other_fixture, address);
        } else {
            // NOTE: expired objects are unloaded as MemoExpiredRef (placeholders)
            return PyToolkit::unloadExpiredRef(fixture, weak_ref);
        }
    }
    
    // CLASS specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_CLASS, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int, AccessFlags)
    {
        auto &class_factory = fixture->get<ClassFactory>();
        auto class_item = class_factory.getTypeByAddr(value.asUniqueAddress().getAddress());
        auto lang_type = class_factory.getLangType(class_item);
        return PyToolkit::getTypeManager().getLangObject(lang_type.get());
    }
    
    // PACK-2 specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::PACK_2, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int offset, AccessFlags)
    {
        auto val_code = lofi_store<2>::fromValue(value).get(offset);
        return PyToolkit::getTypeManager().getLangConstant(val_code);
    }
    
    template <> void registerUnloadMemberFunctions<PyToolkit>(
        std::vector<typename PyToolkit::ObjectSharedPtr (*)(db0::swine_ptr<Fixture> &, Value, unsigned int, AccessFlags)> &functions)
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
        functions[static_cast<int>(StorageClass::DB0_CLASS)] = unloadMember<StorageClass::DB0_CLASS, PyToolkit>;
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
        functions[static_cast<int>(StorageClass::PACK_2)] = unloadMember<StorageClass::PACK_2, PyToolkit>;
    }
    
    template <typename T, typename MemoImplT, typename LangToolkit>
    void unrefMemoObject(db0::swine_ptr<Fixture> &fixture, Address address)
    {
        auto obj_ptr = fixture->getLangCache().get(address);
        if (obj_ptr.get()) {
            db0::FixtureLock lock(fixture);
            // decref cached instance via language specific wrapper type
            auto lang_wrapper = reinterpret_cast<MemoImplT*>(obj_ptr.get());
            auto &object = lang_wrapper->modifyExt();
            object.decRef(false);
            if (!object.hasRefs()) {
                // NOTE: we'll drop the object immediately on condition it has no language references
                if (!LangToolkit::hasLangRefs(*obj_ptr)) {
                    auto unique_addr = object.getUniqueAddress();                    
                    // drop dbzero instance, replacing it with a "null" placeholder
                    object.dropInstance(lock);
                    // might also be removed from lang cache                    
                    fixture->getLangCache().erase(unique_addr);                    
                }
            }
        } else {
            T object(fixture, address);
            object.decRef(false);
            // member will be deleted by GC0 if its ref-count = 0
        }
    }

    // Unreference any ObjectBase-derived type (except Memo types)
    template <typename T, typename LangToolkit>
    void unrefObjectBase(db0::swine_ptr<Fixture> &fixture, Address address)
    {
        auto obj_ptr = fixture->getLangCache().get(address);
        if (obj_ptr.get()) {
            db0::FixtureLock lock(fixture);
            // decref cached instance via language specific wrapper type
            auto lang_wrapper = LangToolkit::template getWrapperTypeOf<T>(obj_ptr.get());
            auto &object = lang_wrapper->modifyExt();
            object.decRef(false);
            if (!object.hasRefs()) {
                // NOTE: we'll drop the object immediately on condition it has no language references
                if (!LangToolkit::hasLangRefs(*obj_ptr)) {
                    auto unique_addr = object.getUniqueAddress();                    
                    // drop dbzero instance, replacing it with a "null" placeholder
                    object.dropInstance(lock);
                    // might also be removed from lang cache                    
                    fixture->getLangCache().erase(unique_addr);                    
                }
            }
        } else {
            T object(fixture, address);
            object.decRef(false);
            // member will be deleted by GC0 if its ref-count = 0
        }
    }
    
    // OBJECT_REF specialization (MemoAnyImpl)
    template <> void unrefMember<StorageClass::OBJECT_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        using MemoObject = PyToolkit::TypeManager::MemoObject;
        unrefMemoObject<Object, MemoObject, PyToolkit>(fixture, value.asAddress());
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
    
    // CLASS specialization
    template <> void unrefMember<StorageClass::DB0_CLASS, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        auto &class_factory = fixture->get<ClassFactory>();
        auto class_item = class_factory.getTypeByAddr(value.asUniqueAddress().getAddress());
        class_item.m_class->decRef(false);
    }

    // DELETED specialization does nothing
    template <> void unrefMember<StorageClass::DELETED, PyToolkit>(db0::swine_ptr<Fixture> &, Value)
    {
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
        functions[static_cast<int>(StorageClass::DB0_CLASS)] = unrefMember<StorageClass::DB0_CLASS, PyToolkit>;
        functions[static_cast<int>(StorageClass::DELETED)] = unrefMember<StorageClass::DELETED, PyToolkit>;
        // FIXME: uncomment and refactor when handling of BYTES is fixed (same storage)
        // functions[static_cast<int>(StorageClass::DB0_SERIALIZED)] = unrefMember<StorageClass::DB0_SERIALIZED, PyToolkit>;
    }
    
    bool isMaterialized(PyObjectPtr obj_ptr)
    {
        auto object_ptr = PyToolkit::getTypeManager().tryExtractObject(obj_ptr);
        return !object_ptr || object_ptr->hasInstance();
    }
    
    template <typename MemoImplT>
    void materializeImpl(FixtureLock &fixture, PyObjectPtr obj_ptr)
    {
        auto object_ptr = PyToolkit::getTypeManager().tryExtractMutableObject<MemoImplT>(obj_ptr);
        if (object_ptr && !object_ptr->hasInstance()) {            
            object_ptr->postInit(fixture);
        }
    }
    
    void materialize(FixtureLock &fixture, PyObjectPtr obj_ptr)
    {
        using MemoObject = PyToolkit::TypeManager::MemoObject;
        using MemoImmutableObject = PyToolkit::TypeManager::MemoImmutableObject;

        if (PyToolkit::isMemoObject(obj_ptr)) {
            materializeImpl<MemoObject>(fixture, obj_ptr);
        } else if (PyToolkit::isMemoImmutableObject(obj_ptr)) {
            materializeImpl<MemoImmutableObject>(fixture, obj_ptr);            
        } else {
            assert(false && "Unsupported memo object type");
            THROWF(db0::InputException) << "Unable to materialize non-memo object" << THROWF_END;
        }
    }
    
}