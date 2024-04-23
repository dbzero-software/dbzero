#include "Member.hpp"
#include <dbzero/core//serialization/Serializable.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>

namespace db0::object_model

{

    // INTEGER specialization
    template <> Value createMember<TypeId::INTEGER, PyToolkit>(db0::swine_ptr<Fixture> &fixture, PyObjectPtr lang_value)
    {
        auto int_value = PyLong_AsLong(lang_value);
        return db0::binary_cast<std::uint64_t, std::int64_t>()(int_value);
    }
    
    // FLOAT specialization
    template <>  Value createMember<TypeId::FLOAT, PyToolkit>(db0::swine_ptr<Fixture> &fixture, PyObjectPtr lang_value)
    {
        auto fp_value = PyFloat_AsDouble(lang_value);
        return db0::binary_cast<std::uint64_t, double>()(fp_value);
    }

    // STRING specialization
    template <> Value createMember<TypeId::STRING, PyToolkit>(db0::swine_ptr<Fixture> &fixture, PyObjectPtr lang_value) {
        // create string-ref member and take its address                
        return db0::v_object<db0::o_string>(*fixture, PyUnicode_AsUTF8(lang_value)).getAddress();
    }

    // OBJECT specialization
    template <> Value createMember<TypeId::MEMO_OBJECT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto &obj = PyToolkit::getTypeManager().extractObject(lang_value);
        assert(obj.hasInstance());
        obj.incRef();
        return obj.getAddress();
    }

    // BLOCK specialization
    template <> Value createMember<TypeId::DB0_BLOCK, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        return PyToolkit::getTypeManager().extractBlock(lang_value).getAddress();
    }

    // LIST specialization
    template <> Value createMember<TypeId::DB0_LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto &list = PyToolkit::getTypeManager().extractList(lang_value);
        list.modify().incRef();
        return list.getAddress();
    }

    // INDEX specialization
    template <> Value createMember<TypeId::DB0_INDEX, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto &index = PyToolkit::getTypeManager().extractIndex(lang_value);
        index.incRef();
        return index.getAddress();
    }

    // SET specialization
    template <> Value createMember<TypeId::DB0_SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto &set = PyToolkit::getTypeManager().extractSet(lang_value);
        set.incRef();
        return set.getAddress();
    }

    // DICT specialization
    template <> Value createMember<TypeId::DB0_DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto &dict = PyToolkit::getTypeManager().extractDict(lang_value);
        dict.incRef();
        return dict.getAddress();
    }

    // TUPLE specialization
    template <> Value createMember<TypeId::DB0_TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto &tuple = PyToolkit::getTypeManager().extractTuple(lang_value);
        tuple.incRef();
        return tuple.getAddress();
    }

    // LIST specialization
    template <> Value createMember<TypeId::LIST, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto list = db0::python::makeList(nullptr, &lang_value, 1);
        list->ext().modify().incRef();
        return list->ext().getAddress();
    }

    // SET specialization
    template <> Value createMember<TypeId::SET, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto set = db0::python::makeSet(nullptr, &lang_value, 1);
        set->ext().incRef();                
        return set->ext().getAddress();
    }

    // DICT specialization
    template <> Value createMember<TypeId::DICT, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        PyObject* args = PyTuple_New(1);
        PyTuple_SetItem(args, 0, lang_value);
        auto dict = db0::python::makeDict(nullptr, args, nullptr);
        dict->ext().incRef();
        return dict->ext().getAddress();
    }

    // TUPLE specialization
    template <> Value createMember<TypeId::TUPLE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto tuple = db0::python::makeTuple(nullptr, &lang_value, 1);
        tuple->ext().incRef();
        return tuple->ext().getAddress();
    }

    // DATETIME specialization
    template <> Value createMember<TypeId::DATETIME, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        return db0::python::pyDateTimeToToUint64(lang_value);
    }

    // BYTES specialization
    Value createBytesMember(db0::swine_ptr<Fixture> &fixture, const std::byte *bytes, std::size_t size) {
        // FIXME: impement as ObjectBase and incRef
        return db0::v_object<db0::o_binary>(*fixture, bytes, size).getAddress();
    }

    // BYTES specialization
    template <> Value createMember<TypeId::BYTES, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto size = PyBytes_GET_SIZE(lang_value);
        auto *str = PyBytes_AsString(lang_value);        
        return createBytesMember(fixture, reinterpret_cast<std::byte *>(str), size);
    }

    // NONE specialization
    template <> Value createMember<TypeId::NONE, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        return 0;
    }

    // OBJECT_ITERATOR specialization (serialized member)
    template <> Value createMember<TypeId::OBJECT_ITERATOR, PyToolkit>(db0::swine_ptr<Fixture> &fixture,
        PyObjectPtr lang_value)
    {
        auto &obj_iter = PyToolkit::getTypeManager().extractObjectIterator(lang_value);
        std::vector<std::byte> bytes;
        // put TypeId as a header
        db0::serial::write(bytes, TypeId::OBJECT_ITERATOR);
        obj_iter.serialize(bytes);
        return createBytesMember(fixture, bytes.data(), bytes.size());
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
    }
    
    // STRING_REF specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::STRING_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        db0::v_object<db0::o_string> string_ref(fixture->myPtr(value.cast<std::uint64_t>()));
        auto str_ptr = string_ref->get();
        return PyUnicode_FromStringAndSize(str_ptr.get_raw(), str_ptr.size());
    }

    // INT64 specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::INT64, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyLong_FromLong(value.cast<std::int64_t>());
    }

    // FLOAT specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::FP_NUMERIC64, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyFloat_FromDouble(value.cast<double>());
    }

    // OBJECT_REF specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::OBJECT_REF, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        auto &class_factory = fixture->template get<ClassFactory>();
        return PyToolkit::unloadObject(fixture, value.cast<std::uint64_t>(), class_factory);
    }

    // DB0_LIST specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_LIST, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyToolkit::unloadList(fixture, value.cast<std::uint64_t>());
    }

    // DB0_BLOCK specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BLOCK, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyToolkit::unloadBlock(fixture, value.cast<std::uint64_t>());
    }

    // DB0_INDEX specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_INDEX, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyToolkit::unloadIndex(fixture, value.cast<std::uint64_t>());
    }

    // DB0_SET specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SET, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyToolkit::unloadSet(fixture, value.cast<std::uint64_t>());
    }

    // DB0_DICT specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_DICT, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyToolkit::unloadDict(fixture, value.cast<std::uint64_t>());
    }

    // BYTES specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_BYTES, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        db0::v_object<db0::o_binary> bytes = fixture->myPtr(value.cast<std::uint64_t>());
        auto bytes_ptr = bytes->getBuffer();
        return PyBytes_FromStringAndSize(reinterpret_cast<const char *>(bytes_ptr), bytes->size());
    }
    
    // DATETIME specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DATE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return db0::python::uint64ToPyDatetime(value.cast<std::uint64_t>());
    }

    // NONE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::NONE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        Py_RETURN_NONE;
    }

    // DB0_TUPLE specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_TUPLE, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
    {
        return PyToolkit::unloadTuple(fixture, value.cast<std::uint64_t>());
    }
    
    // DB0_SERIALIZED specialization
    template <> typename PyToolkit::ObjectSharedPtr unloadMember<StorageClass::DB0_SERIALIZED, PyToolkit>(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name)
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
    }
    
}