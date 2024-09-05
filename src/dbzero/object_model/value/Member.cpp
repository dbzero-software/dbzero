#include "Member.hpp"
#include <dbzero/core//serialization/Serializable.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/object_model/enum/EnumValue.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>

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
    template <> Value createMember<TypeId::INTEGER, PyToolkit>(db0::swine_ptr<Fixture> &fixture, PyObjectPtr obj_ptr)
    {
        auto int_value = PyLong_AsLong(obj_ptr);
        return db0::binary_cast<std::uint64_t, std::int64_t>()(int_value);
    }
    
    // FLOAT specialization
    template <>  Value createMember<TypeId::FLOAT, PyToolkit>(db0::swine_ptr<Fixture> &fixture, PyObjectPtr obj_ptr)
    {
        auto fp_value = PyFloat_AsDouble(obj_ptr);
        return db0::binary_cast<std::uint64_t, double>()(fp_value);
    }

    // STRING specialization
    template <> Value createMember<TypeId::STRING, PyToolkit>(db0::swine_ptr<Fixture> &fixture, PyObjectPtr obj_ptr) {
        // create string-ref member and take its address
        return db0::v_object<db0::o_string>(*fixture, PyUnicode_AsUTF8(obj_ptr)).getAddress();
    }
    
    // OBJECT specialization
    template <> Value createMember<TypeId::MEMO_OBJECT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {        
        auto &obj = PyToolkit::getTypeManager().extractMutableObject(obj_ptr);
        assert(obj.hasInstance());
        assureSameFixture(fixture, obj);
        obj.modify().incRef();
        return obj.getAddress();
    }

    // BLOCK specialization
    template <> Value createMember<TypeId::DB0_BLOCK, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto &block = PyToolkit::getTypeManager().extractMutableBlock(obj_ptr);
        assureSameFixture(fixture, block);
        block.modify().incRef();
        return block.getAddress();
    }

    // LIST specialization
    template <> Value createMember<TypeId::DB0_LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto &list = PyToolkit::getTypeManager().extractMutableList(obj_ptr);
        assureSameFixture(fixture, list);
        list.modify().incRef();
        return list.getAddress();
    }
    
    // INDEX specialization
    template <> Value createMember<TypeId::DB0_INDEX, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto &index = PyToolkit::getTypeManager().extractMutableIndex(obj_ptr);
        assureSameFixture(fixture, index);
        index.incRef();
        return index.getAddress();
    }

    // SET specialization
    template <> Value createMember<TypeId::DB0_SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto &set = PyToolkit::getTypeManager().extractMutableSet(obj_ptr);
        assureSameFixture(fixture, set);
        set.incRef();
        return set.getAddress();
    }

    // DB0 DICT specialization
    template <> Value createMember<TypeId::DB0_DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto &dict = PyToolkit::getTypeManager().extractMutableDict(obj_ptr);
        assureSameFixture(fixture, dict);
        dict.incRef();
        return dict.getAddress();
    }

    // TUPLE specialization
    template <> Value createMember<TypeId::DB0_TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto &tuple = PyToolkit::getTypeManager().extractMutableTuple(obj_ptr);
        assureSameFixture(fixture, tuple);
        tuple.incRef();
        return tuple.getAddress();
    }
    
    // LIST specialization
    template <> Value createMember<TypeId::LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto list_ptr = db0::python::makeDB0List(fixture, &obj_ptr, 1);
        list_ptr.get()->modifyExt().modify().incRef();
        return list_ptr.get()->ext().getAddress();
    }
    
    // SET specialization
    template <> Value createMember<TypeId::SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto set = db0::python::makeDB0Set(fixture, &obj_ptr, 1);
        set.get()->modifyExt().incRef();
        return set.get()->ext().getAddress();
    }
    
    // DICT specialization
    template <> Value createMember<TypeId::DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        PyObject* args = PyTuple_New(1);
        Py_INCREF(obj_ptr);
        PyTuple_SetItem(args, 0, obj_ptr);
        auto dict = db0::python::makeDB0Dict(fixture, args, nullptr);
        dict.get()->modifyExt().incRef();
        return dict.get()->ext().getAddress();
    }
    
    // TUPLE specialization
    template <> Value createMember<TypeId::TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto tuple = db0::python::makeDB0TupleInternal(fixture, &obj_ptr, 1);
        tuple.get()->modifyExt().incRef();
        return tuple.get()->ext().getAddress();
    }

    // DATETIME specialization
    template <> Value createMember<TypeId::DATETIME, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        return db0::python::pyDateTimeToToUint64(obj_ptr);
    }

    // BYTES specialization
    Value createBytesMember(db0::swine_ptr<Fixture> &fixture, const std::byte *bytes, std::size_t size) {
        // FIXME: impement as ObjectBase and incRef
        return db0::v_object<db0::o_binary>(*fixture, bytes, size).getAddress();
    }

    // BYTES specialization
    template <> Value createMember<TypeId::BYTES, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto size = PyBytes_GET_SIZE(obj_ptr);
        auto *str = PyBytes_AsString(obj_ptr);
        return createBytesMember(fixture, reinterpret_cast<std::byte *>(str), size);
    }

    // NONE specialization
    template <> Value createMember<TypeId::NONE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        return 0;
    }

    // OBJECT_ITERATOR specialization (serialized member)
    template <> Value createMember<TypeId::OBJECT_ITERATOR, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto &obj_iter = PyToolkit::getTypeManager().extractObjectIterator(obj_ptr);
        std::vector<std::byte> bytes;
        // put TypeId as a header
        db0::serial::write(bytes, TypeId::OBJECT_ITERATOR);
        obj_iter.serialize(bytes);
        return createBytesMember(fixture, bytes.data(), bytes.size());
    }

    // ENUM value specialization (serialized member)
    template <> Value createMember<TypeId::DB0_ENUM_VALUE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr obj_ptr)
    {
        auto enum_value = PyToolkit::getTypeManager().extractEnumValue(obj_ptr);
        // make sure value from the same Fixture is assigned
        if (enum_value.m_fixture_uuid != fixture->getUUID()) {
            // migrate enum value to the destination fixture
            enum_value = fixture->get<EnumFactory>().translateEnumValue(enum_value);
        }
        return enum_value.getUID().asULong();
    }
    
    template <> void registerCreateMemberFunctions<PyToolkit>(
        std::vector<Value (*)(db0::swine_ptr<Fixture> &, PyObjectPtr)> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::NONE)] = createMember<TypeId::NONE, PyToolkit>;
        functions[static_cast<int>(TypeId::INTEGER)] = createMember<TypeId::INTEGER, PyToolkit>;
        functions[static_cast<int>(TypeId::FLOAT)] = createMember<TypeId::FLOAT, PyToolkit>;
        functions[static_cast<int>(TypeId::STRING)] = createMember<TypeId::STRING, PyToolkit>;
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = createMember<TypeId::MEMO_OBJECT, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_BLOCK)] = createMember<TypeId::DB0_BLOCK, PyToolkit>;
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
        functions[static_cast<int>(TypeId::BYTES)] = createMember<TypeId::BYTES, PyToolkit>;   
        functions[static_cast<int>(TypeId::OBJECT_ITERATOR)] = createMember<TypeId::OBJECT_ITERATOR, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_ENUM_VALUE)] = createMember<TypeId::DB0_ENUM_VALUE, PyToolkit>;
    }
    
    // STRING_REF specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::STRING_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        db0::v_object<db0::o_string> string_ref(fixture->myPtr(value.cast<std::uint64_t>()));
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
        return PyToolkit::unloadObject(fixture, value.cast<std::uint64_t>(), class_factory);
    }

    // DB0_LIST specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_LIST, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadList(fixture, value.cast<std::uint64_t>());
    }

    // DB0_BLOCK specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BLOCK, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadBlock(fixture, value.cast<std::uint64_t>());
    }
    
    // DB0_INDEX specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_INDEX, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadIndex(fixture, value.cast<std::uint64_t>());
    }

    // DB0_SET specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SET, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadSet(fixture, value.cast<std::uint64_t>());
    }

    // DB0_DICT specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_DICT, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return PyToolkit::unloadDict(fixture, value.cast<std::uint64_t>());
    }

    // BYTES specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BYTES, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        db0::v_object<db0::o_binary> bytes = fixture->myPtr(value.cast<std::uint64_t>());
        auto bytes_ptr = bytes->getBuffer();
        return PyBytes_FromStringAndSize(reinterpret_cast<const char *>(bytes_ptr), bytes->size());
    }
    
    // DATETIME specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        return db0::python::uint64ToPyDatetime(value.cast<std::uint64_t>());
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
        return PyToolkit::unloadTuple(fixture, value.cast<std::uint64_t>());
    }
    
    // DB0_SERIALIZED specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SERIALIZED, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *)
    {
        db0::v_object<db0::o_binary> bytes = fixture->myPtr(value.cast<std::uint64_t>());
        std::vector<std::byte> buffer;
        buffer.resize(bytes->size());
        std::copy(bytes->getBuffer(), bytes->getBuffer() + bytes->size(), buffer.begin());
        auto iter = buffer.cbegin(), end = buffer.cend();
        auto type_id = db0::serial::read<TypeId>(iter, end);
        if (type_id == TypeId::OBJECT_ITERATOR) {
            return PyToolkit::unloadObjectIterator(fixture, iter, end);            
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
        functions[static_cast<int>(StorageClass::DB0_BLOCK)] = unloadMember<StorageClass::DB0_BLOCK, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_LIST)] = unloadMember<StorageClass::DB0_LIST, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_INDEX)] = unloadMember<StorageClass::DB0_INDEX, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_SET)] = unloadMember<StorageClass::DB0_SET, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_DICT)] = unloadMember<StorageClass::DB0_DICT, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_TUPLE)] = unloadMember<StorageClass::DB0_TUPLE, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_BYTES)] = unloadMember<StorageClass::DB0_BYTES, PyToolkit>;
        functions[static_cast<int>(StorageClass::DATE)] = unloadMember<StorageClass::DATE, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_SERIALIZED)] = unloadMember<StorageClass::DB0_SERIALIZED, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_ENUM_VALUE)] = unloadMember<StorageClass::DB0_ENUM_VALUE, PyToolkit>;
    }

    template <typename T, typename LangToolkit>
    void unrefObjectBase(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
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
        unrefObjectBase<Object, PyToolkit>(fixture, value.cast<std::uint64_t>());
    }

    template <> void unrefMember<StorageClass::DB0_LIST, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value) 
    {
        unrefObjectBase<List, PyToolkit>(fixture, value.cast<std::uint64_t>());
    }

    template <> void unrefMember<StorageClass::DB0_BLOCK, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value) 
    {
        unrefObjectBase<db0::object_model::pandas::Block, PyToolkit>(fixture, value.cast<std::uint64_t>());
    }

    template <> void unrefMember<StorageClass::DB0_INDEX, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value) 
    {
        unrefObjectBase<Index, PyToolkit>(fixture, value.cast<std::uint64_t>());
    }

    template <> void unrefMember<StorageClass::DB0_SET, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value) 
    {
        unrefObjectBase<Set, PyToolkit>(fixture, value.cast<std::uint64_t>());
    }

    template <> void unrefMember<StorageClass::DB0_DICT, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        unrefObjectBase<Dict, PyToolkit>(fixture, value.cast<std::uint64_t>());
    }

    template <> void unrefMember<StorageClass::DB0_TUPLE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        unrefObjectBase<Tuple, PyToolkit>(fixture, value.cast<std::uint64_t>());
    }

    template <> void unrefMember<StorageClass::DB0_BYTES, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <> void unrefMember<StorageClass::DB0_SERIALIZED, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value)
    {
        throw std::runtime_error("Not implemented");
    }
    
    template <> void registerUnrefMemberFunctions<PyToolkit>(
        std::vector<void (*)(db0::swine_ptr<Fixture> &, Value)> &functions)
    {
        functions.resize(static_cast<int>(StorageClass::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(StorageClass::OBJECT_REF)] = unrefMember<StorageClass::OBJECT_REF, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_BLOCK)] = unrefMember<StorageClass::DB0_BLOCK, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_LIST)] = unrefMember<StorageClass::DB0_LIST, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_INDEX)] = unrefMember<StorageClass::DB0_INDEX, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_SET)] = unrefMember<StorageClass::DB0_SET, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_DICT)] = unrefMember<StorageClass::DB0_DICT, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_TUPLE)] = unrefMember<StorageClass::DB0_TUPLE, PyToolkit>;
        functions[static_cast<int>(StorageClass::DB0_BYTES)] = unrefMember<StorageClass::DB0_BYTES, PyToolkit>;
        // FIXME: uncomment and refactor when handling of BYTES if fixed (same storage)
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