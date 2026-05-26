// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "InternContent.hpp"

#include <cstring>
#include <memory>

#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/embedded/EmbeddedObject.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model
{
    namespace
    {
        // These tags describe the normalized intern-content stream, including structural
        // boundaries that are not represented by StorageClass values. The numeric values
        // are part of the intern hash format and must remain stable.
        enum class Token: std::uint8_t
        {
            Object = 1,
            Field = 2,
            Tuple = 3,
            Set = 4,
            Dict = 5,
            Pair = 6,
            None = 7,
            Bool = 8,
            Int = 9,
            Double = 10,
            String = 11,
            Bytes = 12,
            UInt = 13
        };

        struct FieldValue
        {
            std::uint32_t m_index = 0;
            StorageClass m_kind = StorageClass::UNDEFINED;
            Value m_value;
            const o_tuple_item *m_item = nullptr;
            PyObject *m_object = nullptr;
            bool m_valid = false;
        };

        // Interned objects are expected to be small value objects. The 1MB stream
        // budget is intentionally generous and mainly guards accidental cycles or
        // unexpectedly large content from unbounded hash/compare traversal.
        constexpr std::size_t INTERN_CONTENT_STREAM_LIMIT = 1024 * 1024;
        constexpr std::size_t INTERN_REFERENCE_TRAVERSAL_CHARGE = 1024;

        struct StreamBudget
        {
            void add(std::size_t size)
            {
                if (size > INTERN_CONTENT_STREAM_LIMIT || m_consumed > INTERN_CONTENT_STREAM_LIMIT - size) {
                    THROWF(db0::InputException)
                        << "Intern content stream size threshold exceeded; possible reference cycle";
                }
                m_consumed += size;
            }

            std::size_t m_consumed = 0;
        };

        class HashSink
        {
        public:
            HashSink()
                : m_budget(std::make_shared<StreamBudget>())
            {
            }

            void writeByte(std::uint8_t value)
            {
                write(&value, sizeof(value));
            }

            void writeBytes(const void *data, std::size_t size)
            {
                write(data, size);
            }

            template <typename T> void writeScalar(T value)
            {
                write(&value, sizeof(value));
            }

            std::uint64_t getValue() const
            {
                return m_hash;
            }

            void charge(std::size_t size)
            {
                m_budget->add(size);
            }

        private:
            void write(const void *data, std::size_t size)
            {
                charge(size);
                const auto *cursor = static_cast<const unsigned char *>(data);
                for (std::size_t i = 0; i < size; ++i) {
                    m_hash ^= cursor[i];
                    m_hash *= 1099511628211ULL;
                }
            }

            std::shared_ptr<StreamBudget> m_budget;
            std::uint64_t m_hash = 1469598103934665603ULL;
        };

        template <typename SinkT> void writeToken(SinkT &sink, Token value)
        {
            sink.writeByte(static_cast<std::uint8_t>(value));
        }

        StorageClass getNormalizedKind(StorageClass kind)
        {
            switch (kind) {
                case StorageClass::PACKED_INT32:
                    return StorageClass::INT64;
                case StorageClass::STRING_REF:
                case StorageClass::POOLED_STRING:
                case StorageClass::STR64:
                case StorageClass::EMBEDDED_STRING:
                    return StorageClass::EMBEDDED_STRING;
                case StorageClass::DB0_BYTES:
                case StorageClass::DB0_BYTES_ARRAY:
                case StorageClass::EMBEDDED_BYTES:
                    return StorageClass::EMBEDDED_BYTES;
                case StorageClass::DB0_TUPLE:
                case StorageClass::DB0_LIST:
                    return StorageClass::EMBEDDED_TUPLE;
                case StorageClass::DB0_SET:
                    return StorageClass::EMBEDDED_SET;
                case StorageClass::DB0_DICT:
                    return StorageClass::EMBEDDED_DICT;
                case StorageClass::OBJECT_REF:
                case StorageClass::EMBEDDED_OBJECT_REF:
                case StorageClass::EMBEDDED_OBJECT:
                    return StorageClass::EMBEDDED_OBJECT;
                default:
                    return kind;
            }
        }

        int compareWithFixture(
            db0::swine_ptr<db0::Fixture> *fixture, const o_tuple_item &lhs, const o_tuple_item &rhs
        );
        int compareWithFixture(
            db0::swine_ptr<db0::Fixture> *fixture, const o_dict_pair &lhs, const o_dict_pair &rhs
        );
        int compareWithFixture(db0::swine_ptr<db0::Fixture> *fixture, PyObject *lhs, PyObject *rhs);
        std::uint64_t hashWithFixture(db0::swine_ptr<db0::Fixture> *fixture, const o_tuple_item &item);
        std::uint64_t hashWithFixture(db0::swine_ptr<db0::Fixture> *fixture, PyObject *object);
        std::uint64_t hashPairWithFixture(db0::swine_ptr<db0::Fixture> *fixture, const o_dict_pair &pair);
        std::uint64_t hashPythonDictPairWithFixture(
            db0::swine_ptr<db0::Fixture> *fixture, PyObject *dict, PyObject *key
        );
        std::uint64_t hashFieldWithFixture(db0::swine_ptr<db0::Fixture> *fixture, const FieldValue &field);

        db0::python::PyToolkit::ObjectSharedPtr resolveObjectRef(
            db0::swine_ptr<db0::Fixture> *fixture, db0::UniqueAddress address
        )
        {
            assert(fixture && *fixture);

            auto &classFactory = (*fixture)->template get<ClassFactory>();
            return db0::python::PyToolkit::unloadAnyObject(
                *fixture, address.getAddress(), classFactory, nullptr, address.getInstanceId()
            );
        }

        db0::python::PyToolkit::ObjectSharedPtr resolveObjectRef(
            db0::swine_ptr<db0::Fixture> *fixture, StorageClass kind, Value value
        )
        {
            assert(fixture && *fixture);

            if (kind == StorageClass::OBJECT_REF) {
                auto &classFactory = (*fixture)->template get<ClassFactory>();
                return db0::python::PyToolkit::unloadObject(*fixture, value.asAddress(), classFactory);
            }

            auto uniqueAddress = value.asUniqueAddress();
            if (!uniqueAddress.hasInstanceId()) {
                THROWF(db0::InputException) << "Invalid intern object reference";
            }
            return resolveObjectRef(fixture, uniqueAddress);
        }

        class InternStreamer
        {
        public:
            InternStreamer(HashSink &sink, db0::swine_ptr<db0::Fixture> *fixture)
                : m_sink(sink)
                , m_fixture(fixture)
            {
            }

            void writeObject(const o_embedded_object &object)
            {
                writeToken(m_sink, Token::Object);
                m_sink.writeScalar<std::uint32_t>(object.getClassRef());
                writeFields(object);
            }

            void writeInitializer(const ImmutableObjectInitializer &initializer)
            {
                writeToken(m_sink, Token::Object);
                m_sink.writeScalar<std::uint32_t>(initializer.getClassPtr()->getClassRef());
                writeFields(initializer);
            }

            void writeTupleItemForHash(const o_tuple_item &item)
            {
                writeTupleItem(item);
            }

            void writePythonItemForHash(PyObject *object)
            {
                writePythonItem(object);
            }

            void writeDictPairForHash(const o_dict_pair &pair)
            {
                writeToken(m_sink, Token::Pair);
                writeTupleItem(pair.key());
                writeTupleItem(pair.value());
            }

            void writePythonDictPairForHash(PyObject *dict, PyObject *key)
            {
                auto *value = PyDict_GetItemWithError(dict, key);
                if (!value) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python dict value";
                }
                writeToken(m_sink, Token::Pair);
                writePythonItem(key);
                writePythonItem(value);
            }

            void writeFieldForHash(const FieldValue &field)
            {
                writeToken(m_sink, Token::Field);
                m_sink.writeScalar<std::uint32_t>(field.m_index);
                writeFieldValue(field);
            }

        private:
            void writeFields(const o_embedded_object &object)
            {
                writeUnorderedFields(countObjectFields(object), [&](auto &&emit) {
                    forEachObjectField(object, emit);
                });
            }

            void writeFields(const ImmutableObjectInitializer &initializer)
            {
                writeUnorderedFields(countInitializerFields(initializer), [&](auto &&emit) {
                    forEachInitializerField(initializer, emit);
                });
            }

            template <typename FieldIteratorT> void writeUnorderedFields(
                std::uint64_t count, FieldIteratorT iterateFields
            )
            {
                writeToken(m_sink, Token::Field);
                m_sink.writeScalar<std::uint64_t>(count);
                std::uint64_t fieldHashSum = 0;
                std::uint64_t emitted = 0;
                iterateFields([&](const FieldValue &field) {
                    fieldHashSum += hashFieldWithFixture(m_fixture, field);
                    ++emitted;
                });
                if (emitted != count) {
                    THROWF(db0::InternalException) << "Unable to normalize intern fields";
                }
                m_sink.writeScalar<std::uint64_t>(fieldHashSum);
            }

            std::uint64_t countObjectFields(const o_embedded_object &object) const
            {
                std::uint64_t count = 0;
                const auto &types = object.pos_vt().types();
                for (std::size_t i = 0; i < object.pos_vt().size(); ++i) {
                    count += isFieldValue(types[i]) ? 1 : 0;
                }
                for (const auto &value: object.index_vt().xvalues()) {
                    count += isFieldValue(value.m_type) ? 1 : 0;
                }
                for (const auto &entry: object.field_map()) {
                    (void)entry;
                    ++count;
                }
                return count;
            }

            std::uint64_t countInitializerFields(const ImmutableObjectInitializer &initializer) const
            {
                std::uint64_t count = 0;
                PosVT::Data posVtData;
                unsigned int posVtOffset = 0;
                auto indexVtData = initializer.getData(posVtData, posVtOffset);

                for (std::size_t i = 0; i < posVtData.size(); ++i) {
                    count += isFieldValue(posVtData.m_types[i]) ? 1 : 0;
                }
                for (auto value = indexVtData.first; value != indexVtData.second; ++value) {
                    count += isFieldValue(value->m_type) ? 1 : 0;
                }
                for (const auto &value: initializer.objects()) {
                    count += !!value.m_object && value.m_storage_class != StorageClass::DELETED ? 1 : 0;
                }
                return count;
            }

            static bool isFieldValue(StorageClass kind)
            {
                return kind != StorageClass::UNDEFINED && kind != StorageClass::DELETED;
            }

            static FieldValue makeField(std::uint32_t index, StorageClass kind, Value value)
            {
                FieldValue field;
                field.m_index = index;
                field.m_kind = kind;
                field.m_value = value;
                field.m_valid = true;
                return field;
            }

            static FieldValue makeField(std::uint32_t index, const o_tuple_item &item)
            {
                FieldValue field;
                field.m_index = index;
                field.m_item = &item;
                field.m_valid = true;
                return field;
            }

            static FieldValue makeField(std::uint32_t index, StorageClass kind, PyObject *object)
            {
                FieldValue field;
                field.m_index = index;
                field.m_kind = kind;
                field.m_object = object;
                field.m_valid = true;
                return field;
            }

            template <typename EmitT> void forEachObjectField(const o_embedded_object &object, EmitT emit)
            {
                const auto &types = object.pos_vt().types();
                const auto &values = object.pos_vt().values();
                for (std::size_t i = 0; i < object.pos_vt().size(); ++i) {
                    if (isFieldValue(types[i])) {
                        emit(makeField(
                            static_cast<std::uint32_t>(object.pos_vt().offset() + i), types[i], values[i]
                        ));
                    }
                }
                for (const auto &value: object.index_vt().xvalues()) {
                    if (isFieldValue(value.m_type)) {
                        emit(makeField(value.getIndex(), value.m_type, value.m_value));
                    }
                }
                for (const auto &entry: object.field_map()) {
                    emit(makeField(getFieldIndex(entry.key()), entry.value()));
                }
            }

            template <typename EmitT> void forEachInitializerField(
                const ImmutableObjectInitializer &initializer, EmitT emit
            )
            {
                PosVT::Data posVtData;
                unsigned int posVtOffset = 0;
                auto indexVtData = initializer.getData(posVtData, posVtOffset);

                for (std::size_t i = 0; i < posVtData.size(); ++i) {
                    if (isFieldValue(posVtData.m_types[i])) {
                        emit(makeField(
                            static_cast<std::uint32_t>(posVtOffset + i),
                            posVtData.m_types[i], posVtData.m_values[i]
                        ));
                    }
                }
                for (auto value = indexVtData.first; value != indexVtData.second; ++value) {
                    if (isFieldValue(value->m_type)) {
                        emit(makeField(value->getIndex(), value->m_type, value->m_value));
                    }
                }
                for (const auto &value: initializer.objects()) {
                    if (!!value.m_object && value.m_storage_class != StorageClass::DELETED) {
                        emit(makeField(value.m_loc.first, value.m_storage_class, value.m_object.get()));
                    }
                }
            }

            void writeFieldValue(const FieldValue &field)
            {
                if (field.m_item) {
                    writeTupleItem(*field.m_item);
                    return;
                }
                if (field.m_object) {
                    writePythonObject(field.m_kind, field.m_object);
                    return;
                }
                writeFixedValue(field.m_kind, field.m_value);
            }

            std::uint32_t getFieldIndex(const o_tuple_item &key) const
            {
                if (key.itemKind() == StorageClass::PACKED_INT32) {
                    return key.packedIntPayload().value();
                }
                if (key.itemKind() == StorageClass::INT64) {
                    return static_cast<std::uint32_t>(key.intPayload().value());
                }
                THROWF(db0::InternalException) << "Embedded object field map key is not an integer";
                return 0;
            }

            void writeFixedValue(StorageClass kind, Value value)
            {
                switch (kind) {
                    case StorageClass::NONE:
                        writeToken(m_sink, Token::None);
                        return;
                    case StorageClass::BOOLEAN:
                        writeToken(m_sink, Token::Bool);
                        m_sink.writeScalar<std::uint8_t>(value.m_store != 0 ? 1 : 0);
                        return;
                    case StorageClass::INT64:
                    case StorageClass::PACKED_INT32:
                        writeToken(m_sink, Token::Int);
                        m_sink.writeScalar<std::int64_t>(static_cast<std::int64_t>(value.m_store));
                        return;
                    case StorageClass::FP_NUMERIC64:
                        writeToken(m_sink, Token::Double);
                        m_sink.writeScalar<std::uint64_t>(value.m_store);
                        return;
                    case StorageClass::PTIME64:
                    case StorageClass::DATE:
                    case StorageClass::DATETIME:
                    case StorageClass::DATETIME_TZ:
                    case StorageClass::TIME:
                    case StorageClass::TIME_TZ:
                    case StorageClass::DECIMAL:
                        writeToken(m_sink, Token::UInt);
                        m_sink.writeByte(static_cast<std::uint8_t>(kind));
                        m_sink.writeScalar<std::uint64_t>(value.m_store);
                        return;
                    case StorageClass::OBJECT_REF:
                    case StorageClass::EMBEDDED_OBJECT_REF:
                        writeObjectRef(kind, value);
                        return;
                    default:
                        THROWF(db0::InternalException) << "Unsupported fixed intern content kind: " << kind;
                }
            }

            void writeTupleItem(const o_tuple_item &item)
            {
                switch (item.itemKind()) {
                    case StorageClass::NONE:
                        writeToken(m_sink, Token::None);
                        return;
                    case StorageClass::BOOLEAN:
                        writeToken(m_sink, Token::Bool);
                        m_sink.writeScalar<std::uint8_t>(item.boolPayload().value() ? 1 : 0);
                        return;
                    case StorageClass::PACKED_INT32:
                        writeToken(m_sink, Token::Int);
                        m_sink.writeScalar<std::int64_t>(static_cast<std::int64_t>(item.packedIntPayload().value()));
                        return;
                    case StorageClass::INT64:
                        writeToken(m_sink, Token::Int);
                        m_sink.writeScalar<std::int64_t>(item.intPayload().value());
                        return;
                    case StorageClass::FP_NUMERIC64:
                        writeToken(m_sink, Token::Double);
                        m_sink.writeScalar<double>(item.doublePayload().value());
                        return;
                    case StorageClass::STRING_REF:
                    case StorageClass::EMBEDDED_STRING: {
                        auto value = item.stringPayload().get();
                        writeBytes(Token::String, value.get_raw(), value.size());
                        return;
                    }
                    case StorageClass::DB0_BYTES:
                    case StorageClass::EMBEDDED_BYTES:
                        writeBytes(Token::Bytes, item.bytesPayload().begin(), item.bytesPayload().size());
                        return;
                    case StorageClass::PTIME64:
                    case StorageClass::DATE:
                    case StorageClass::DATETIME:
                    case StorageClass::DATETIME_TZ:
                    case StorageClass::TIME:
                    case StorageClass::TIME_TZ:
                    case StorageClass::DECIMAL:
                        writeToken(m_sink, Token::UInt);
                        m_sink.writeByte(static_cast<std::uint8_t>(item.itemKind()));
                        m_sink.writeScalar<std::uint64_t>(item.uint64Payload().value());
                        return;
                    case StorageClass::EMBEDDED_TUPLE:
                        writeTuple(o_py_tuple::__const_ref(item.embeddedPayload().begin()));
                        return;
                    case StorageClass::EMBEDDED_SET:
                        writeSet(o_py_set::__const_ref(item.embeddedPayload().begin()));
                        return;
                    case StorageClass::EMBEDDED_DICT:
                        writeDict(o_py_dict::__const_ref(item.embeddedPayload().begin()));
                        return;
                    case StorageClass::EMBEDDED_OBJECT:
                        writeObject(o_embedded_object::__const_ref(item.embeddedPayload().begin()));
                        return;
                    default:
                        THROWF(db0::InternalException) << "Unsupported tuple intern content kind: " << item.itemKind();
                }
            }

            void writeTuple(const o_py_tuple &value)
            {
                writeToken(m_sink, Token::Tuple);
                m_sink.writeScalar<std::uint64_t>(value.size());
                for (const auto &item: value) {
                    writeTupleItem(item);
                }
            }

            void writeSet(const o_py_set &value)
            {
                writeToken(m_sink, Token::Set);
                m_sink.writeScalar<std::uint64_t>(value.size());
                std::uint64_t itemHashSum = 0;
                for (const auto &item: value) {
                    itemHashSum += hashWithFixture(m_fixture, item);
                }
                m_sink.writeScalar<std::uint64_t>(itemHashSum);
            }

            void writeDict(const o_py_dict &value)
            {
                writeToken(m_sink, Token::Dict);
                m_sink.writeScalar<std::uint64_t>(value.size());
                std::uint64_t pairHashSum = 0;
                for (const auto &pair: value) {
                    pairHashSum += hashPairWithFixture(m_fixture, pair);
                }
                m_sink.writeScalar<std::uint64_t>(pairHashSum);
            }

            int compare(const o_tuple_item &lhs, const o_tuple_item &rhs)
            {
                return compareWithFixture(m_fixture, lhs, rhs);
            }

            int compare(const o_dict_pair &lhs, const o_dict_pair &rhs)
            {
                return compareWithFixture(m_fixture, lhs, rhs);
            }

            int compare(PyObject *lhs, PyObject *rhs)
            {
                return compareWithFixture(m_fixture, lhs, rhs);
            }

            void writeBytes(Token kind, const void *data, std::size_t size)
            {
                writeToken(m_sink, kind);
                m_sink.writeScalar<std::uint64_t>(size);
                m_sink.writeBytes(data, size);
            }

            void writeObjectRef(StorageClass kind, Value value)
            {
                writeObjectRefObject(resolveObjectRef(m_fixture, kind, value));
            }

            void writeObjectRef(db0::UniqueAddress address)
            {
                writeObjectRefObject(resolveObjectRef(m_fixture, address));
            }

            void writeObjectRefObject(const db0::python::PyToolkit::ObjectSharedPtr &pyObject)
            {
                if (!m_fixture || !*m_fixture) {
                    THROWF(db0::InputException) << "Fixture is required for intern object references";
                }
                m_sink.charge(INTERN_REFERENCE_TRAVERSAL_CHARGE);
                if (db0::python::PyEmbeddedMemo_Check(pyObject.get())) {
                    writeObject(db0::python::getEmbeddedMemoRef(
                        reinterpret_cast<db0::python::MemoImmutableObject *>(pyObject.get())
                    ).embeddedObject());
                    return;
                }
                if (!db0::python::PyToolkit::isMemoImmutableObject(pyObject.get())) {
                    THROWF(db0::InputException) << "intern object reference does not resolve to an immutable object";
                }
                const auto &memo = db0::python::PyToolkit::getTypeManager()
                    .template extractObject<db0::python::MemoImmutableObject>(pyObject.get());
                writeObject(memo.getData()->getObject());
            }

            void writePythonObject(StorageClass storageClass, PyObject *pyObject)
            {
                switch (getNormalizedKind(storageClass)) {
                    case StorageClass::EMBEDDED_STRING: {
                        auto value = db0::python::PyToolkit::getTypeManager().extractString(pyObject);
                        writeBytes(Token::String, value, std::strlen(value));
                        return;
                    }
                    case StorageClass::EMBEDDED_BYTES: {
                        auto value = db0::python::PyToolkit::getTypeManager().extractBytes(pyObject);
                        writeBytes(Token::Bytes, value.m_data, value.m_size);
                        return;
                    }
                    case StorageClass::EMBEDDED_TUPLE:
                        writePythonTuple(pyObject);
                        return;
                    case StorageClass::EMBEDDED_SET:
                        writePythonSet(pyObject);
                        return;
                    case StorageClass::EMBEDDED_DICT:
                        writePythonDict(pyObject);
                        return;
                    case StorageClass::EMBEDDED_OBJECT:
                        writePythonMemoObject(pyObject);
                        return;
                    default:
                        THROWF(db0::InternalException) << "Unsupported initializer intern object kind: " << storageClass;
                }
            }

            void writePythonTuple(PyObject *sequence)
            {
                writeToken(m_sink, Token::Tuple);
                auto count = getPythonSequenceSize(sequence);
                m_sink.writeScalar<std::uint64_t>(count);
                for (std::size_t i = 0; i < count; ++i) {
                    writePythonItem(getPythonSequenceItem(sequence, i));
                }
            }

            void writePythonSet(PyObject *set)
            {
                if (!PySet_Check(set)) {
                    THROWF(db0::InputException) << "Intern set content expects a Python set";
                }
                auto size = PySet_GET_SIZE(set);
                if (size < 0) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python set size";
                }
                auto count = static_cast<std::uint64_t>(size);
                writeToken(m_sink, Token::Set);
                m_sink.writeScalar<std::uint64_t>(count);
                std::uint64_t itemHashSum = 0;
                auto iterator = Py_OWN(PyObject_GetIter(set));
                if (!iterator) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Intern set content expects a Python set";
                }
                Py_FOR(item, iterator) {
                    itemHashSum += hashWithFixture(m_fixture, *item);
                }
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to iterate Python set";
                }
                m_sink.writeScalar<std::uint64_t>(itemHashSum);
            }

            void writePythonDict(PyObject *dict)
            {
                if (!PyDict_Check(dict)) {
                    THROWF(db0::InputException) << "Intern dict content expects a Python dict";
                }
                auto size = PyDict_Size(dict);
                if (size < 0) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python dict size";
                }
                auto count = static_cast<std::uint64_t>(size);
                writeToken(m_sink, Token::Dict);
                m_sink.writeScalar<std::uint64_t>(count);
                std::uint64_t pairHashSum = 0;
                auto iterator = Py_OWN(PyObject_GetIter(dict));
                if (!iterator) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Intern dict content expects a Python dict";
                }
                Py_FOR(key, iterator) {
                    pairHashSum += hashPythonDictPairWithFixture(m_fixture, dict, *key);
                }
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to iterate Python dict";
                }
                m_sink.writeScalar<std::uint64_t>(pairHashSum);
            }

            std::size_t getPythonSequenceSize(PyObject *sequence) const
            {
                if (PyTuple_Check(sequence)) {
                    return static_cast<std::size_t>(PyTuple_GET_SIZE(sequence));
                }
                if (PyList_Check(sequence)) {
                    return static_cast<std::size_t>(PyList_GET_SIZE(sequence));
                }
                THROWF(db0::InputException) << "Intern tuple content expects a Python tuple or list";
                return 0;
            }

            PyObject *getPythonSequenceItem(PyObject *sequence, std::size_t index) const
            {
                if (PyTuple_Check(sequence)) {
                    return PyTuple_GET_ITEM(sequence, static_cast<Py_ssize_t>(index));
                }
                return PyList_GET_ITEM(sequence, static_cast<Py_ssize_t>(index));
            }

            void writePythonItem(PyObject *object)
            {
                auto &typeManager = db0::python::PyToolkit::getTypeManager();
                auto typeId = typeManager.getTypeId(object);
                switch (typeId) {
                    case db0::bindings::TypeId::NONE:
                        writeToken(m_sink, Token::None);
                        return;
                    case db0::bindings::TypeId::BOOLEAN:
                        writeToken(m_sink, Token::Bool);
                        m_sink.writeScalar<std::uint8_t>(object == Py_True ? 1 : 0);
                        return;
                    case db0::bindings::TypeId::INTEGER: {
                        auto value = PyLong_AsLongLong(object);
                        if (PyErr_Occurred()) {
                            PyErr_Clear();
                            THROWF(db0::InputException) << "Python integer is out of int64 range";
                        }
                        writeToken(m_sink, Token::Int);
                        m_sink.writeScalar<std::int64_t>(value);
                        return;
                    }
                    case db0::bindings::TypeId::FLOAT:
                        writeToken(m_sink, Token::Double);
                        m_sink.writeScalar<double>(PyFloat_AsDouble(object));
                        return;
                    case db0::bindings::TypeId::DATETIME:
                        writePythonUIntItem(StorageClass::DATETIME, typeManager.extractUInt64(typeId, object));
                        return;
                    case db0::bindings::TypeId::DATETIME_TZ:
                        writePythonUIntItem(StorageClass::DATETIME_TZ, typeManager.extractUInt64(typeId, object));
                        return;
                    case db0::bindings::TypeId::DATE:
                        writePythonUIntItem(StorageClass::DATE, typeManager.extractUInt64(typeId, object));
                        return;
                    case db0::bindings::TypeId::TIME:
                        writePythonUIntItem(StorageClass::TIME, typeManager.extractUInt64(typeId, object));
                        return;
                    case db0::bindings::TypeId::TIME_TZ:
                        writePythonUIntItem(StorageClass::TIME_TZ, typeManager.extractUInt64(typeId, object));
                        return;
                    case db0::bindings::TypeId::DECIMAL:
                        writePythonUIntItem(StorageClass::DECIMAL, typeManager.extractUInt64(typeId, object));
                        return;
                    case db0::bindings::TypeId::STRING: {
                        auto value = typeManager.extractString(object);
                        writeBytes(Token::String, value, std::strlen(value));
                        return;
                    }
                    case db0::bindings::TypeId::BYTES: {
                        auto value = typeManager.extractBytes(object);
                        writeBytes(Token::Bytes, value.m_data, value.m_size);
                        return;
                    }
                    case db0::bindings::TypeId::LIST:
                    case db0::bindings::TypeId::TUPLE:
                        writePythonTuple(object);
                        return;
                    case db0::bindings::TypeId::SET:
                        writePythonSet(object);
                        return;
                    case db0::bindings::TypeId::DICT:
                        writePythonDict(object);
                        return;
                    case db0::bindings::TypeId::MEMO_IMMUTABLE_OBJECT:
                        writePythonMemoObject(object);
                        return;
                    default:
                        break;
                }
                THROWF(db0::InputException) << "Unsupported intern content Python type: " << Py_TYPE(object)->tp_name;
            }

            void writePythonUIntItem(StorageClass kind, std::uint64_t value)
            {
                writeToken(m_sink, Token::UInt);
                m_sink.writeByte(static_cast<std::uint8_t>(kind));
                m_sink.writeScalar<std::uint64_t>(value);
            }

            void writePythonMemoObject(PyObject *pyObject)
            {
                using MemoImmutableObject = db0::python::PyToolkit::TypeManager::MemoImmutableObject;

                if (db0::python::PyEmbeddedMemo_Check(pyObject)) {
                    auto &embeddedObject = db0::python::getEmbeddedMemoRef(
                        reinterpret_cast<MemoImmutableObject *>(pyObject)
                    ).embeddedObject();
                    writeObject(embeddedObject);
                    return;
                }

                const auto &memo = db0::python::PyToolkit::getTypeManager()
                    .template extractObject<MemoImmutableObject>(pyObject);
                if (!memo.hasInstance()) {
                    auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
                        InitManager::instance.findInitializer(memo)
                    );
                    if (!initializer) {
                        THROWF(db0::InputException) << "Non-materialized intern memo object has no initializer";
                    }
                    this->writeInitializer(*initializer);
                    return;
                }

                writeObjectRef(memo.getUniqueAddress());
            }

            HashSink &m_sink;
            db0::swine_ptr<db0::Fixture> *m_fixture = nullptr;
        };

        std::uint64_t hashWithFixture(db0::swine_ptr<db0::Fixture> *fixture, const o_tuple_item &item)
        {
            HashSink sink;
            InternStreamer(sink, fixture).writeTupleItemForHash(item);
            return sink.getValue();
        }

        std::uint64_t hashWithFixture(db0::swine_ptr<db0::Fixture> *fixture, PyObject *object)
        {
            HashSink sink;
            InternStreamer(sink, fixture).writePythonItemForHash(object);
            return sink.getValue();
        }

        std::uint64_t hashPairWithFixture(db0::swine_ptr<db0::Fixture> *fixture, const o_dict_pair &pair)
        {
            HashSink sink;
            InternStreamer(sink, fixture).writeDictPairForHash(pair);
            return sink.getValue();
        }

        std::uint64_t hashPythonDictPairWithFixture(
            db0::swine_ptr<db0::Fixture> *fixture, PyObject *dict, PyObject *key
        )
        {
            HashSink sink;
            InternStreamer(sink, fixture).writePythonDictPairForHash(dict, key);
            return sink.getValue();
        }

        std::uint64_t hashFieldWithFixture(db0::swine_ptr<db0::Fixture> *fixture, const FieldValue &field)
        {
            HashSink sink;
            InternStreamer(sink, fixture).writeFieldForHash(field);
            return sink.getValue();
        }

        class InternComparator
        {
        public:
            explicit InternComparator(db0::swine_ptr<db0::Fixture> *fixture)
                : m_fixture(fixture)
            {
            }

            int compare(const o_embedded_object &lhs, const o_embedded_object &rhs)
            {
                if (auto result = compareToken(Token::Object, Token::Object)) {
                    return result;
                }
                if (auto result = compareScalar(lhs.getClassRef(), rhs.getClassRef())) {
                    return result;
                }
                return compareObjectFields(lhs, rhs);
            }

            int compare(const ImmutableObjectInitializer &lhs, const o_embedded_object &rhs)
            {
                if (auto result = compareToken(Token::Object, Token::Object)) {
                    return result;
                }
                if (auto result = compareScalar(lhs.getClassPtr()->getClassRef(), rhs.getClassRef())) {
                    return result;
                }
                return compareInitializerFields(lhs, rhs);
            }

            int compare(const o_embedded_object &lhs, const ImmutableObjectInitializer &rhs)
            {
                return -compare(rhs, lhs);
            }

            int compare(const ImmutableObjectInitializer &lhs, const ImmutableObjectInitializer &rhs)
            {
                if (auto result = compareToken(Token::Object, Token::Object)) {
                    return result;
                }
                if (auto result = compareScalar(lhs.getClassPtr()->getClassRef(), rhs.getClassPtr()->getClassRef())) {
                    return result;
                }
                return compareInitializerFields(lhs, rhs);
            }

            int compare(const o_tuple_item &lhs, const o_tuple_item &rhs)
            {
                switch (lhs.itemKind()) {
                    case StorageClass::NONE:
                        return compareNoneToItem(rhs);
                    case StorageClass::BOOLEAN:
                        return compare(lhs.boolPayload().value(), rhs);
                    case StorageClass::PACKED_INT32:
                        return compare(static_cast<std::int64_t>(lhs.packedIntPayload().value()), rhs);
                    case StorageClass::INT64:
                        return compare(lhs.intPayload().value(), rhs);
                    case StorageClass::FP_NUMERIC64:
                        return compare(lhs.doublePayload().value(), rhs);
                    case StorageClass::STRING_REF:
                    case StorageClass::EMBEDDED_STRING: {
                        auto value = lhs.stringPayload().get();
                        return compareBytesWithToken(Token::String, value.get_raw(), value.size(), rhs);
                    }
                    case StorageClass::DB0_BYTES:
                    case StorageClass::EMBEDDED_BYTES:
                        return compareBytesWithToken(Token::Bytes, lhs.bytesPayload().begin(), lhs.bytesPayload().size(), rhs);
                    case StorageClass::PTIME64:
                    case StorageClass::DATE:
                    case StorageClass::DATETIME:
                    case StorageClass::DATETIME_TZ:
                    case StorageClass::TIME:
                    case StorageClass::TIME_TZ:
                    case StorageClass::DECIMAL:
                        return compare(lhs.itemKind(), lhs.uint64Payload().value(), rhs);
                    case StorageClass::EMBEDDED_TUPLE:
                        return compare(o_py_tuple::__const_ref(lhs.embeddedPayload().begin()), rhs);
                    case StorageClass::EMBEDDED_SET:
                        return compare(o_py_set::__const_ref(lhs.embeddedPayload().begin()), rhs);
                    case StorageClass::EMBEDDED_DICT:
                        return compare(o_py_dict::__const_ref(lhs.embeddedPayload().begin()), rhs);
                    case StorageClass::EMBEDDED_OBJECT:
                        return compare(o_embedded_object::__const_ref(lhs.embeddedPayload().begin()), rhs);
                    default:
                        THROWF(db0::InternalException) << "Unsupported tuple intern content kind: " << lhs.itemKind();
                        return 0;
                }
            }

            int compare(const o_dict_pair &lhs, const o_dict_pair &rhs)
            {
                if (auto result = compareToken(Token::Pair, Token::Pair)) {
                    return result;
                }
                if (auto result = compare(lhs.key(), rhs.key())) {
                    return result;
                }
                return compare(lhs.value(), rhs.value());
            }

            int compare(PyObject *lhs, PyObject *rhs)
            {
                auto &typeManager = db0::python::PyToolkit::getTypeManager();
                auto lhsType = typeManager.getTypeId(lhs);
                auto rhsType = typeManager.getTypeId(rhs);

                switch (lhsType) {
                    case db0::bindings::TypeId::NONE:
                        return compareToken(Token::None, getToken(rhs));
                    case db0::bindings::TypeId::BOOLEAN:
                        return compare(lhs == Py_True, rhs);
                    case db0::bindings::TypeId::INTEGER: {
                        auto value = PyLong_AsLongLong(lhs);
                        if (PyErr_Occurred()) {
                            PyErr_Clear();
                            THROWF(db0::InputException) << "Python integer is out of int64 range";
                        }
                        return compare(static_cast<std::int64_t>(value), rhs);
                    }
                    case db0::bindings::TypeId::FLOAT:
                        return compare(PyFloat_AsDouble(lhs), rhs);
                    case db0::bindings::TypeId::DATETIME:
                        return compare(StorageClass::DATETIME, typeManager.extractUInt64(lhsType, lhs), rhs);
                    case db0::bindings::TypeId::DATETIME_TZ:
                        return compare(StorageClass::DATETIME_TZ, typeManager.extractUInt64(lhsType, lhs), rhs);
                    case db0::bindings::TypeId::DATE:
                        return compare(StorageClass::DATE, typeManager.extractUInt64(lhsType, lhs), rhs);
                    case db0::bindings::TypeId::TIME:
                        return compare(StorageClass::TIME, typeManager.extractUInt64(lhsType, lhs), rhs);
                    case db0::bindings::TypeId::TIME_TZ:
                        return compare(StorageClass::TIME_TZ, typeManager.extractUInt64(lhsType, lhs), rhs);
                    case db0::bindings::TypeId::DECIMAL:
                        return compare(StorageClass::DECIMAL, typeManager.extractUInt64(lhsType, lhs), rhs);
                    case db0::bindings::TypeId::STRING: {
                        auto value = typeManager.extractString(lhs);
                        return compareBytesToPythonItem(Token::String, value, std::strlen(value), rhs);
                    }
                    case db0::bindings::TypeId::BYTES: {
                        auto value = typeManager.extractBytes(lhs);
                        return compareBytesToPythonItem(Token::Bytes, value.m_data, value.m_size, rhs);
                    }
                    case db0::bindings::TypeId::LIST:
                    case db0::bindings::TypeId::TUPLE:
                        return comparePythonTupleToPythonItem(lhs, rhs);
                    case db0::bindings::TypeId::SET:
                        return comparePythonSetToPythonItem(lhs, rhs);
                    case db0::bindings::TypeId::DICT:
                        return comparePythonDictToPythonItem(lhs, rhs);
                    case db0::bindings::TypeId::MEMO_IMMUTABLE_OBJECT:
                        return comparePythonMemoToPythonItem(lhs, rhs);
                    default:
                        break;
                }
                (void)rhsType;
                THROWF(db0::InputException) << "Unsupported intern content Python type: " << Py_TYPE(lhs)->tp_name;
                return 0;
            }

        private:
            template <typename T> int compareScalar(T lhs, T rhs)
            {
                return compareBytes(&lhs, &rhs, sizeof(T));
            }

            int compareToken(Token lhs, Token rhs)
            {
                return compareScalar(static_cast<std::uint8_t>(lhs), static_cast<std::uint8_t>(rhs));
            }

            int compareBytes(const void *lhs, const void *rhs, std::size_t size)
            {
                m_lhsBudget.add(size);
                m_rhsBudget.add(size);
                auto result = std::memcmp(lhs, rhs, size);
                return result < 0 ? -1 : result > 0 ? 1 : 0;
            }

            int compareBytesPayload(const void *lhs, std::size_t lhsSize, const void *rhs, std::size_t rhsSize)
            {
                if (auto result = compareScalar<std::uint64_t>(lhsSize, rhsSize)) {
                    return result;
                }
                auto prefix = lhsSize < rhsSize ? lhsSize : rhsSize;
                if (auto result = compareBytes(lhs, rhs, prefix)) {
                    return result;
                }
                return 0;
            }

            static bool isFieldValue(StorageClass kind)
            {
                return kind != StorageClass::UNDEFINED && kind != StorageClass::DELETED;
            }

            std::uint64_t countObjectFields(const o_embedded_object &object) const
            {
                std::uint64_t count = 0;
                const auto &types = object.pos_vt().types();
                for (std::size_t i = 0; i < object.pos_vt().size(); ++i) {
                    count += isFieldValue(types[i]) ? 1 : 0;
                }
                for (const auto &value: object.index_vt().xvalues()) {
                    count += isFieldValue(value.m_type) ? 1 : 0;
                }
                for (const auto &entry: object.field_map()) {
                    (void)entry;
                    ++count;
                }
                return count;
            }

            std::uint64_t countInitializerFields(const ImmutableObjectInitializer &initializer) const
            {
                std::uint64_t count = 0;
                PosVT::Data posVtData;
                unsigned int posVtOffset = 0;
                auto indexVtData = initializer.getData(posVtData, posVtOffset);
                for (std::size_t i = 0; i < posVtData.size(); ++i) {
                    count += isFieldValue(posVtData.m_types[i]) ? 1 : 0;
                }
                for (auto value = indexVtData.first; value != indexVtData.second; ++value) {
                    count += isFieldValue(value->m_type) ? 1 : 0;
                }
                for (const auto &value: initializer.objects()) {
                    count += !!value.m_object && value.m_storage_class != StorageClass::DELETED ? 1 : 0;
                }
                return count;
            }

            template <typename LhsT, typename RhsT> int compareFields(const LhsT &lhs, const RhsT &rhs)
            {
                auto lhsCount = countFields(lhs);
                auto rhsCount = countFields(rhs);
                if (auto result = compareToken(Token::Field, Token::Field)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhsCount, rhsCount)) {
                    return result;
                }
                auto rhsLookup = makeFieldLookup(rhs);
                int compareResult = 0;
                std::uint64_t emitted = 0;
                forEachField(lhs, [&](const FieldValue &lhsField) {
                    ++emitted;
                    if (compareResult) {
                        return;
                    }
                    auto rhsField = rhsLookup.find(lhsField.m_index);
                    if (!rhsField.m_valid) {
                        compareResult = -1;
                        return;
                    }
                    compareResult = compareFieldValue(lhsField, rhsField);
                });
                if (emitted != lhsCount) {
                    THROWF(db0::InternalException) << "Unable to compare intern fields";
                }
                return compareResult;
            }

            std::uint64_t countFields(const o_embedded_object &object) const
            {
                return countObjectFields(object);
            }

            std::uint64_t countFields(const ImmutableObjectInitializer &initializer) const
            {
                return countInitializerFields(initializer);
            }

            int compareObjectFields(const o_embedded_object &lhs, const o_embedded_object &rhs)
            {
                return compareFields(lhs, rhs);
            }

            int compareInitializerFields(const ImmutableObjectInitializer &lhs, const o_embedded_object &rhs)
            {
                return compareFields(lhs, rhs);
            }

            int compareInitializerFields(const ImmutableObjectInitializer &lhs, const ImmutableObjectInitializer &rhs)
            {
                return compareSequentialInitializerFields(lhs, rhs);
            }

            static FieldValue makeField(std::uint32_t index, StorageClass kind, Value value)
            {
                FieldValue field;
                field.m_index = index;
                field.m_kind = kind;
                field.m_value = value;
                field.m_valid = true;
                return field;
            }

            static FieldValue makeField(std::uint32_t index, const o_tuple_item &item)
            {
                FieldValue field;
                field.m_index = index;
                field.m_item = &item;
                field.m_valid = true;
                return field;
            }

            static FieldValue makeField(std::uint32_t index, StorageClass kind, PyObject *object)
            {
                FieldValue field;
                field.m_index = index;
                field.m_kind = kind;
                field.m_object = object;
                field.m_valid = true;
                return field;
            }

            struct ObjectFieldLookup
            {
                explicit ObjectFieldLookup(const o_embedded_object &object)
                    : m_object(object)
                {
                }

                FieldValue find(std::uint32_t index) const
                {
                    std::pair<StorageClass, Value> value;
                    if (m_object.pos_vt().find(index, value) && InternComparator::isFieldValue(value.first)) {
                        return InternComparator::makeField(index, value.first, value.second);
                    }
                    if (m_object.index_vt().find(index, value) && InternComparator::isFieldValue(value.first)) {
                        return InternComparator::makeField(index, value.first, value.second);
                    }
                    if (auto *item = m_object.variableValue(index)) {
                        return InternComparator::makeField(index, *item);
                    }
                    return {};
                }

                const o_embedded_object &m_object;
            };

            ObjectFieldLookup makeFieldLookup(const o_embedded_object &object) const
            {
                return ObjectFieldLookup(object);
            }

            struct InitializerFieldCursor
            {
                explicit InitializerFieldCursor(const ImmutableObjectInitializer &initializer)
                    : m_objects(&initializer.objects())
                {
                    m_indexVtData = initializer.getData(m_posVtData, m_posVtOffset);
                    m_indexValue = m_indexVtData.first;
                }

                FieldValue next()
                {
                    while (m_pos < m_posVtData.size()) {
                        auto pos = m_pos++;
                        if (InternComparator::isFieldValue(m_posVtData.m_types[pos])) {
                            return InternComparator::makeField(
                                static_cast<std::uint32_t>(m_posVtOffset + pos),
                                m_posVtData.m_types[pos], m_posVtData.m_values[pos]
                            );
                        }
                    }
                    while (m_indexValue != m_indexVtData.second) {
                        auto *value = m_indexValue++;
                        if (InternComparator::isFieldValue(value->m_type)) {
                            return InternComparator::makeField(
                                value->getIndex(), value->m_type, value->m_value
                            );
                        }
                    }
                    while (m_objectPos < m_objects->size()) {
                        const auto &value = (*m_objects)[m_objectPos++];
                        if (!!value.m_object && value.m_storage_class != StorageClass::DELETED) {
                            return InternComparator::makeField(
                                value.m_loc.first, value.m_storage_class, value.m_object.get()
                            );
                        }
                    }
                    return {};
                }

                PosVT::Data m_posVtData;
                unsigned int m_posVtOffset = 0;
                std::size_t m_pos = 0;
                std::pair<const XValue *, const XValue *> m_indexVtData = { nullptr, nullptr };
                const XValue *m_indexValue = nullptr;
                const std::vector<ImmutableObjectInitializer::ObjectValue> *m_objects = nullptr;
                std::size_t m_objectPos = 0;
            };

            int compareSequentialInitializerFields(
                const ImmutableObjectInitializer &lhs, const ImmutableObjectInitializer &rhs
            )
            {
                auto lhsCount = countInitializerFields(lhs);
                auto rhsCount = countInitializerFields(rhs);
                if (auto result = compareToken(Token::Field, Token::Field)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhsCount, rhsCount)) {
                    return result;
                }
                InitializerFieldCursor lhsCursor(lhs);
                InitializerFieldCursor rhsCursor(rhs);
                for (std::uint64_t i = 0; i < lhsCount; ++i) {
                    auto lhsField = lhsCursor.next();
                    auto rhsField = rhsCursor.next();
                    if (!lhsField.m_valid || !rhsField.m_valid) {
                        THROWF(db0::InternalException) << "Unable to compare intern fields";
                    }
                    if (auto result = compareScalar<std::uint32_t>(lhsField.m_index, rhsField.m_index)) {
                        return result;
                    }
                    if (auto result = compareFieldValue(lhsField, rhsField)) {
                        return result;
                    }
                }
                return 0;
            }

            template <typename EmitT> void forEachField(const o_embedded_object &object, EmitT emit)
            {
                const auto &types = object.pos_vt().types();
                const auto &values = object.pos_vt().values();
                for (std::size_t i = 0; i < object.pos_vt().size(); ++i) {
                    if (isFieldValue(types[i])) {
                        emit(makeField(
                            static_cast<std::uint32_t>(object.pos_vt().offset() + i), types[i], values[i]
                        ));
                    }
                }
                for (const auto &value: object.index_vt().xvalues()) {
                    if (isFieldValue(value.m_type)) {
                        emit(makeField(value.getIndex(), value.m_type, value.m_value));
                    }
                }
                for (const auto &entry: object.field_map()) {
                    emit(makeField(getFieldIndex(entry.key()), entry.value()));
                }
            }

            template <typename EmitT> void forEachField(const ImmutableObjectInitializer &initializer, EmitT emit)
            {
                PosVT::Data posVtData;
                unsigned int posVtOffset = 0;
                auto indexVtData = initializer.getData(posVtData, posVtOffset);
                for (std::size_t i = 0; i < posVtData.size(); ++i) {
                    if (isFieldValue(posVtData.m_types[i])) {
                        emit(makeField(
                            static_cast<std::uint32_t>(posVtOffset + i),
                            posVtData.m_types[i], posVtData.m_values[i]
                        ));
                    }
                }
                for (auto value = indexVtData.first; value != indexVtData.second; ++value) {
                    if (isFieldValue(value->m_type)) {
                        emit(makeField(value->getIndex(), value->m_type, value->m_value));
                    }
                }
                for (const auto &value: initializer.objects()) {
                    if (!!value.m_object && value.m_storage_class != StorageClass::DELETED) {
                        emit(makeField(value.m_loc.first, value.m_storage_class, value.m_object.get()));
                    }
                }
            }

            std::uint32_t getFieldIndex(const o_tuple_item &key) const
            {
                if (key.itemKind() == StorageClass::PACKED_INT32) {
                    return key.packedIntPayload().value();
                }
                if (key.itemKind() == StorageClass::INT64) {
                    return static_cast<std::uint32_t>(key.intPayload().value());
                }
                THROWF(db0::InternalException) << "Embedded object field map key is not an integer";
                return 0;
            }

            int compareFieldValue(const FieldValue &lhs, const FieldValue &rhs)
            {
                if (lhs.m_item) {
                    return compareItemToField(*lhs.m_item, rhs);
                }
                if (lhs.m_object) {
                    return comparePythonObjectToField(lhs.m_kind, lhs.m_object, rhs);
                }
                return compareFixedToField(lhs.m_kind, lhs.m_value, rhs);
            }

            int compareItemToField(const o_tuple_item &lhs, const FieldValue &rhs)
            {
                if (rhs.m_item) {
                    return compare(lhs, *rhs.m_item);
                }
                if (rhs.m_object) {
                    return compareItemToPythonObject(lhs, rhs.m_kind, rhs.m_object);
                }
                return compare(lhs, rhs.m_kind, rhs.m_value);
            }

            int compareFixedToField(StorageClass kind, Value value, const FieldValue &rhs)
            {
                if (rhs.m_item) {
                    return compare(kind, value, *rhs.m_item);
                }
                if (rhs.m_object) {
                    return compareFixedToPythonObject(kind, value, rhs.m_kind, rhs.m_object);
                }
                return compareFixed(kind, value, rhs.m_kind, rhs.m_value);
            }

            int comparePythonObjectToField(StorageClass kind, PyObject *object, const FieldValue &rhs)
            {
                if (rhs.m_item) {
                    return comparePythonObjectToItem(kind, object, *rhs.m_item);
                }
                if (rhs.m_object) {
                    return comparePythonObject(kind, object, rhs.m_kind, rhs.m_object);
                }
                return -compareFixedToPythonObject(rhs.m_kind, rhs.m_value, kind, object);
            }

            Token getToken(StorageClass kind) const
            {
                switch (kind) {
                    case StorageClass::NONE:
                        return Token::None;
                    case StorageClass::BOOLEAN:
                        return Token::Bool;
                    case StorageClass::INT64:
                    case StorageClass::PACKED_INT32:
                        return Token::Int;
                    case StorageClass::FP_NUMERIC64:
                        return Token::Double;
                    case StorageClass::PTIME64:
                    case StorageClass::DATE:
                    case StorageClass::DATETIME:
                    case StorageClass::DATETIME_TZ:
                    case StorageClass::TIME:
                    case StorageClass::TIME_TZ:
                    case StorageClass::DECIMAL:
                        return Token::UInt;
                    case StorageClass::OBJECT_REF:
                    case StorageClass::EMBEDDED_OBJECT_REF:
                        return Token::Object;
                    default:
                        THROWF(db0::InternalException) << "Unsupported fixed intern content kind: " << kind;
                        return Token::None;
                }
            }

            Token getToken(const o_tuple_item &item) const
            {
                switch (item.itemKind()) {
                    case StorageClass::NONE:
                        return Token::None;
                    case StorageClass::BOOLEAN:
                        return Token::Bool;
                    case StorageClass::PACKED_INT32:
                    case StorageClass::INT64:
                        return Token::Int;
                    case StorageClass::FP_NUMERIC64:
                        return Token::Double;
                    case StorageClass::STRING_REF:
                    case StorageClass::EMBEDDED_STRING:
                        return Token::String;
                    case StorageClass::DB0_BYTES:
                    case StorageClass::EMBEDDED_BYTES:
                        return Token::Bytes;
                    case StorageClass::PTIME64:
                    case StorageClass::DATE:
                    case StorageClass::DATETIME:
                    case StorageClass::DATETIME_TZ:
                    case StorageClass::TIME:
                    case StorageClass::TIME_TZ:
                    case StorageClass::DECIMAL:
                        return Token::UInt;
                    case StorageClass::EMBEDDED_TUPLE:
                        return Token::Tuple;
                    case StorageClass::EMBEDDED_SET:
                        return Token::Set;
                    case StorageClass::EMBEDDED_DICT:
                        return Token::Dict;
                    case StorageClass::EMBEDDED_OBJECT:
                        return Token::Object;
                    default:
                        THROWF(db0::InternalException) << "Unsupported tuple intern content kind: " << item.itemKind();
                        return Token::None;
                }
            }

            Token getToken(PyObject *object) const
            {
                auto &typeManager = db0::python::PyToolkit::getTypeManager();
                switch (typeManager.getTypeId(object)) {
                    case db0::bindings::TypeId::NONE:
                        return Token::None;
                    case db0::bindings::TypeId::BOOLEAN:
                        return Token::Bool;
                    case db0::bindings::TypeId::INTEGER:
                        return Token::Int;
                    case db0::bindings::TypeId::FLOAT:
                        return Token::Double;
                    case db0::bindings::TypeId::DATETIME:
                    case db0::bindings::TypeId::DATETIME_TZ:
                    case db0::bindings::TypeId::DATE:
                    case db0::bindings::TypeId::TIME:
                    case db0::bindings::TypeId::TIME_TZ:
                    case db0::bindings::TypeId::DECIMAL:
                        return Token::UInt;
                    case db0::bindings::TypeId::STRING:
                        return Token::String;
                    case db0::bindings::TypeId::BYTES:
                        return Token::Bytes;
                    case db0::bindings::TypeId::LIST:
                    case db0::bindings::TypeId::TUPLE:
                        return Token::Tuple;
                    case db0::bindings::TypeId::SET:
                        return Token::Set;
                    case db0::bindings::TypeId::DICT:
                        return Token::Dict;
                    case db0::bindings::TypeId::MEMO_IMMUTABLE_OBJECT:
                        return Token::Object;
                    default:
                        break;
                }
                THROWF(db0::InputException) << "Unsupported intern content Python type: " << Py_TYPE(object)->tp_name;
                return Token::None;
            }

            int compareFixed(StorageClass lhsKind, Value lhs, StorageClass rhsKind, Value rhs)
            {
                if (lhsKind == StorageClass::OBJECT_REF || lhsKind == StorageClass::EMBEDDED_OBJECT_REF) {
                    return compareObjectRefToFixed(lhsKind, lhs, rhsKind, rhs);
                }
                if (rhsKind == StorageClass::OBJECT_REF || rhsKind == StorageClass::EMBEDDED_OBJECT_REF) {
                    return -compareObjectRefToFixed(rhsKind, rhs, lhsKind, lhs);
                }
                if (auto result = compareToken(getToken(lhsKind), getToken(rhsKind))) {
                    return result;
                }
                switch (lhsKind) {
                    case StorageClass::NONE:
                        return 0;
                    case StorageClass::BOOLEAN: {
                        auto lhsBool = static_cast<std::uint8_t>(lhs.m_store != 0 ? 1 : 0);
                        auto rhsBool = static_cast<std::uint8_t>(rhs.m_store != 0 ? 1 : 0);
                        return compareScalar(lhsBool, rhsBool);
                    }
                    case StorageClass::INT64:
                    case StorageClass::PACKED_INT32:
                        return compareScalar(static_cast<std::int64_t>(lhs.m_store), static_cast<std::int64_t>(rhs.m_store));
                    case StorageClass::FP_NUMERIC64:
                        return compareScalar(lhs.m_store, rhs.m_store);
                    case StorageClass::PTIME64:
                    case StorageClass::DATE:
                    case StorageClass::DATETIME:
                    case StorageClass::DATETIME_TZ:
                    case StorageClass::TIME:
                    case StorageClass::TIME_TZ:
                    case StorageClass::DECIMAL: {
                        auto result = compareScalar(static_cast<std::uint8_t>(lhsKind), static_cast<std::uint8_t>(rhsKind));
                        return result ? result : compareScalar(lhs.m_store, rhs.m_store);
                    }
                    default:
                        THROWF(db0::InternalException) << "Unsupported fixed intern content kind: " << lhsKind;
                        return 0;
                }
            }

            int compare(StorageClass kind, Value value, const o_tuple_item &item)
            {
                if (kind == StorageClass::OBJECT_REF || kind == StorageClass::EMBEDDED_OBJECT_REF) {
                    return compareObjectRefToItem(kind, value, item);
                }
                if (auto result = compareToken(getToken(kind), getToken(item))) {
                    return result;
                }
                switch (kind) {
                    case StorageClass::NONE:
                        return 0;
                    case StorageClass::BOOLEAN:
                        return compare(value.m_store != 0, item);
                    case StorageClass::INT64:
                    case StorageClass::PACKED_INT32:
                        return compare(static_cast<std::int64_t>(value.m_store), item);
                    case StorageClass::FP_NUMERIC64: {
                        auto rhsValue = item.doublePayload().value();
                        return compareBytes(&value.m_store, &rhsValue, sizeof(value.m_store));
                    }
                    case StorageClass::PTIME64:
                    case StorageClass::DATE:
                    case StorageClass::DATETIME:
                    case StorageClass::DATETIME_TZ:
                    case StorageClass::TIME:
                    case StorageClass::TIME_TZ:
                    case StorageClass::DECIMAL:
                        return compare(kind, value.m_store, item);
                    default:
                        THROWF(db0::InternalException) << "Unsupported fixed intern content kind: " << kind;
                        return 0;
                }
            }

            int compare(const o_tuple_item &item, StorageClass kind, Value value)
            {
                return -compare(kind, value, item);
            }

            int compareNoneToItem(const o_tuple_item &rhs)
            {
                return compareToken(Token::None, getToken(rhs));
            }

            int compare(bool lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Bool, getToken(rhs))) {
                    return result;
                }
                auto lhsValue = static_cast<std::uint8_t>(lhs ? 1 : 0);
                auto rhsValue = static_cast<std::uint8_t>(rhs.boolPayload().value() ? 1 : 0);
                return compareScalar(lhsValue, rhsValue);
            }

            int compare(std::int64_t lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Int, getToken(rhs))) {
                    return result;
                }
                auto rhsValue = rhs.itemKind() == StorageClass::PACKED_INT32
                    ? static_cast<std::int64_t>(rhs.packedIntPayload().value())
                    : rhs.intPayload().value();
                return compareScalar<std::int64_t>(lhs, static_cast<std::int64_t>(rhsValue));
            }

            int compare(double lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Double, getToken(rhs))) {
                    return result;
                }
                return compareScalar(lhs, rhs.doublePayload().value());
            }

            int compare(StorageClass kind, std::uint64_t lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::UInt, getToken(rhs))) {
                    return result;
                }
                if (auto result = compareScalar(static_cast<std::uint8_t>(kind), static_cast<std::uint8_t>(rhs.itemKind()))) {
                    return result;
                }
                return compareScalar(lhs, rhs.uint64Payload().value());
            }

            int compareBytesWithToken(Token tokenValue, const void *lhs, std::size_t lhsSize, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(tokenValue, getToken(rhs))) {
                    return result;
                }
                if (tokenValue == Token::String) {
                    auto value = rhs.stringPayload().get();
                    return compareBytesPayload(lhs, lhsSize, value.get_raw(), value.size());
                }
                return compareBytesPayload(lhs, lhsSize, rhs.bytesPayload().begin(), rhs.bytesPayload().size());
            }

            int compare(const o_py_tuple &lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Tuple, getToken(rhs))) {
                    return result;
                }
                return compare(lhs, o_py_tuple::__const_ref(rhs.embeddedPayload().begin()));
            }

            int compare(const o_py_set &lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Set, getToken(rhs))) {
                    return result;
                }
                return compare(lhs, o_py_set::__const_ref(rhs.embeddedPayload().begin()));
            }

            int compare(const o_py_dict &lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Dict, getToken(rhs))) {
                    return result;
                }
                return compare(lhs, o_py_dict::__const_ref(rhs.embeddedPayload().begin()));
            }

            int compare(const o_embedded_object &lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Object, getToken(rhs))) {
                    return result;
                }
                return compare(lhs, o_embedded_object::__const_ref(rhs.embeddedPayload().begin()));
            }

            int compare(const o_py_tuple &lhs, const o_py_tuple &rhs)
            {
                if (auto result = compareToken(Token::Tuple, Token::Tuple)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhs.size(), rhs.size())) {
                    return result;
                }
                auto lhsIt = lhs.begin();
                auto rhsIt = rhs.begin();
                for (; lhsIt != lhs.end(); ++lhsIt, ++rhsIt) {
                    if (auto result = compare(*lhsIt, *rhsIt)) {
                        return result;
                    }
                }
                return 0;
            }

            int compare(const o_py_set &lhs, const o_py_set &rhs)
            {
                if (auto result = compareToken(Token::Set, Token::Set)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhs.size(), rhs.size())) {
                    return result;
                }
                for (const auto &lhsItem: lhs) {
                    if (!rhs.contains(lhsItem)) {
                        return -1;
                    }
                }
                return 0;
            }

            int compare(const o_py_dict &lhs, const o_py_dict &rhs)
            {
                if (auto result = compareToken(Token::Dict, Token::Dict)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhs.size(), rhs.size())) {
                    return result;
                }
                for (const auto &lhsPair: lhs) {
                    auto *rhsValue = rhs.get(lhsPair.key());
                    if (!rhsValue) {
                        return -1;
                    }
                    if (auto result = compare(lhsPair.value(), *rhsValue)) {
                        return result;
                    }
                }
                return 0;
            }

            int compareObjectRefToFixed(StorageClass lhsKind, Value lhs, StorageClass rhsKind, Value rhs)
            {
                return withObjectRef(lhsKind, lhs, [&](const auto &lhsObject) {
                    return compareObjectToFixed(lhsObject, rhsKind, rhs);
                });
            }

            int compareObjectRefToItem(StorageClass kind, Value value, const o_tuple_item &item)
            {
                return withObjectRef(kind, value, [&](const auto &lhs) {
                    return compareObjectToItem(lhs, item);
                });
            }

            template <typename ObjectT> int compareObjectToFixed(const ObjectT &lhs, StorageClass kind, Value value)
            {
                if (kind == StorageClass::OBJECT_REF || kind == StorageClass::EMBEDDED_OBJECT_REF) {
                    return withObjectRef(kind, value, [&](const auto &rhs) {
                        return compare(lhs, rhs);
                    });
                }
                return compareToken(Token::Object, getToken(kind));
            }

            template <typename ObjectT> int compareObjectToItem(const ObjectT &lhs, const o_tuple_item &item)
            {
                if (auto result = compareToken(Token::Object, getToken(item))) {
                    return result;
                }
                return compare(lhs, o_embedded_object::__const_ref(item.embeddedPayload().begin()));
            }

            template <typename FnT> int withObjectRef(StorageClass kind, Value value, FnT fn)
            {
                return withObjectRefObject(resolveObjectRef(m_fixture, kind, value), fn);
            }

            template <typename FnT> int withObjectRef(db0::UniqueAddress address, FnT fn)
            {
                return withObjectRefObject(resolveObjectRef(m_fixture, address), fn);
            }

            template <typename FnT>
            int withObjectRefObject(const db0::python::PyToolkit::ObjectSharedPtr &pyObject, FnT fn)
            {
                if (!m_fixture || !*m_fixture) {
                    THROWF(db0::InputException) << "Fixture is required for intern object references";
                }
                m_lhsBudget.add(INTERN_REFERENCE_TRAVERSAL_CHARGE);
                m_rhsBudget.add(INTERN_REFERENCE_TRAVERSAL_CHARGE);
                if (db0::python::PyEmbeddedMemo_Check(pyObject.get())) {
                    return fn(db0::python::getEmbeddedMemoRef(
                        reinterpret_cast<db0::python::MemoImmutableObject *>(pyObject.get())
                    ).embeddedObject());
                }
                if (!db0::python::PyToolkit::isMemoImmutableObject(pyObject.get())) {
                    THROWF(db0::InputException) << "Intern object reference does not resolve to an immutable object";
                }
                const auto &memo = db0::python::PyToolkit::getTypeManager()
                    .template extractObject<db0::python::MemoImmutableObject>(pyObject.get());
                return fn(memo.getData()->getObject());
            }

            int compareItemToPythonObject(const o_tuple_item &item, StorageClass kind, PyObject *object)
            {
                return -comparePythonObjectToItem(kind, object, item);
            }

            int comparePythonObjectToItem(StorageClass storageClass, PyObject *object, const o_tuple_item &item)
            {
                switch (getNormalizedKind(storageClass)) {
                    case StorageClass::EMBEDDED_STRING: {
                        auto value = db0::python::PyToolkit::getTypeManager().extractString(object);
                        return compareBytesWithToken(Token::String, value, std::strlen(value), item);
                    }
                    case StorageClass::EMBEDDED_BYTES: {
                        auto value = db0::python::PyToolkit::getTypeManager().extractBytes(object);
                        return compareBytesWithToken(Token::Bytes, value.m_data, value.m_size, item);
                    }
                    case StorageClass::EMBEDDED_TUPLE:
                        return comparePythonTuple(object, item);
                    case StorageClass::EMBEDDED_SET:
                        return comparePythonSet(object, item);
                    case StorageClass::EMBEDDED_DICT:
                        return comparePythonDict(object, item);
                    case StorageClass::EMBEDDED_OBJECT:
                        return comparePythonMemo(object, item);
                    default:
                        THROWF(db0::InternalException) << "Unsupported initializer intern object kind: " << storageClass;
                        return 0;
                }
            }

            int compareFixedToPythonObject(StorageClass lhsKind, Value lhs, StorageClass rhsKind, PyObject *rhs)
            {
                if (lhsKind == StorageClass::OBJECT_REF || lhsKind == StorageClass::EMBEDDED_OBJECT_REF) {
                    return withObjectRef(lhsKind, lhs, [&](const auto &lhsObject) {
                        return compareObjectToPythonObject(lhsObject, rhsKind, rhs);
                    });
                }
                return -comparePythonObjectToFixed(rhsKind, rhs, lhsKind, lhs);
            }

            int comparePythonObjectToFixed(StorageClass lhsKind, PyObject *lhs, StorageClass rhsKind, Value rhs)
            {
                switch (getNormalizedKind(lhsKind)) {
                    case StorageClass::EMBEDDED_OBJECT:
                        return comparePythonMemoToFixed(lhs, rhsKind, rhs);
                    default:
                        return compareToken(getPythonObjectToken(lhsKind), getToken(rhsKind));
                }
            }

            int comparePythonObject(StorageClass lhsKind, PyObject *lhs, StorageClass rhsKind, PyObject *rhs)
            {
                switch (getNormalizedKind(lhsKind)) {
                    case StorageClass::EMBEDDED_STRING: {
                        auto lhsValue = db0::python::PyToolkit::getTypeManager().extractString(lhs);
                        auto rhsValue = db0::python::PyToolkit::getTypeManager().extractString(rhs);
                        if (auto result = compareToken(Token::String, Token::String)) {
                            return result;
                        }
                        return compareBytesPayload(lhsValue, std::strlen(lhsValue), rhsValue, std::strlen(rhsValue));
                    }
                    case StorageClass::EMBEDDED_BYTES: {
                        auto lhsValue = db0::python::PyToolkit::getTypeManager().extractBytes(lhs);
                        auto rhsValue = db0::python::PyToolkit::getTypeManager().extractBytes(rhs);
                        if (auto result = compareToken(Token::Bytes, Token::Bytes)) {
                            return result;
                        }
                        return compareBytesPayload(lhsValue.m_data, lhsValue.m_size, rhsValue.m_data, rhsValue.m_size);
                    }
                    case StorageClass::EMBEDDED_TUPLE:
                        return comparePythonTuple(lhs, rhs);
                    case StorageClass::EMBEDDED_SET:
                        return comparePythonSet(lhs, rhs);
                    case StorageClass::EMBEDDED_DICT:
                        return comparePythonDict(lhs, rhs);
                    case StorageClass::EMBEDDED_OBJECT:
                        return comparePythonMemo(lhs, rhs);
                    default:
                        THROWF(db0::InternalException) << "Unsupported initializer intern object kind: " << lhsKind;
                        return 0;
                }
            }

            Token getPythonObjectToken(StorageClass kind) const
            {
                switch (getNormalizedKind(kind)) {
                    case StorageClass::EMBEDDED_STRING:
                        return Token::String;
                    case StorageClass::EMBEDDED_BYTES:
                        return Token::Bytes;
                    case StorageClass::EMBEDDED_TUPLE:
                        return Token::Tuple;
                    case StorageClass::EMBEDDED_SET:
                        return Token::Set;
                    case StorageClass::EMBEDDED_DICT:
                        return Token::Dict;
                    case StorageClass::EMBEDDED_OBJECT:
                        return Token::Object;
                    default:
                        THROWF(db0::InternalException) << "Unsupported initializer intern object kind: " << kind;
                        return Token::None;
                }
            }

            template <typename ObjectT> int compareObjectToPythonObject(const ObjectT &lhs, StorageClass rhsKind, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Object, getPythonObjectToken(rhsKind))) {
                    return result;
                }
                return compareObjectToPythonMemo(lhs, rhs);
            }

            int comparePythonTuple(PyObject *lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Tuple, getToken(rhs))) {
                    return result;
                }
                return comparePythonTupleToTuple(lhs, o_py_tuple::__const_ref(rhs.embeddedPayload().begin()));
            }

            int comparePythonSet(PyObject *lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Set, getToken(rhs))) {
                    return result;
                }
                return comparePythonSetToSet(lhs, o_py_set::__const_ref(rhs.embeddedPayload().begin()));
            }

            int comparePythonDict(PyObject *lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Dict, getToken(rhs))) {
                    return result;
                }
                return comparePythonDictToDict(lhs, o_py_dict::__const_ref(rhs.embeddedPayload().begin()));
            }

            int comparePythonMemo(PyObject *lhs, const o_tuple_item &rhs)
            {
                if (auto result = compareToken(Token::Object, getToken(rhs))) {
                    return result;
                }
                return comparePythonMemoToObject(lhs, o_embedded_object::__const_ref(rhs.embeddedPayload().begin()));
            }

            int compare(bool lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Bool, getToken(rhs))) {
                    return result;
                }
                auto lhsValue = static_cast<std::uint8_t>(lhs ? 1 : 0);
                auto rhsValue = static_cast<std::uint8_t>(rhs == Py_True ? 1 : 0);
                return compareScalar(lhsValue, rhsValue);
            }

            int compare(std::int64_t lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Int, getToken(rhs))) {
                    return result;
                }
                auto rhsValue = PyLong_AsLongLong(rhs);
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Python integer is out of int64 range";
                }
                return compareScalar<std::int64_t>(lhs, static_cast<std::int64_t>(rhsValue));
            }

            int compare(double lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Double, getToken(rhs))) {
                    return result;
                }
                return compareScalar(lhs, PyFloat_AsDouble(rhs));
            }

            int compare(StorageClass kind, std::uint64_t lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::UInt, getToken(rhs))) {
                    return result;
                }
                auto &typeManager = db0::python::PyToolkit::getTypeManager();
                auto rhsType = typeManager.getTypeId(rhs);
                auto rhsKind = getStorageClassForPythonUInt(rhsType);
                if (auto result = compareScalar(static_cast<std::uint8_t>(kind), static_cast<std::uint8_t>(rhsKind))) {
                    return result;
                }
                return compareScalar(lhs, typeManager.extractUInt64(rhsType, rhs));
            }

            StorageClass getStorageClassForPythonUInt(db0::bindings::TypeId typeId) const
            {
                switch (typeId) {
                    case db0::bindings::TypeId::DATETIME:
                        return StorageClass::DATETIME;
                    case db0::bindings::TypeId::DATETIME_TZ:
                        return StorageClass::DATETIME_TZ;
                    case db0::bindings::TypeId::DATE:
                        return StorageClass::DATE;
                    case db0::bindings::TypeId::TIME:
                        return StorageClass::TIME;
                    case db0::bindings::TypeId::TIME_TZ:
                        return StorageClass::TIME_TZ;
                    case db0::bindings::TypeId::DECIMAL:
                        return StorageClass::DECIMAL;
                    default:
                        THROWF(db0::InputException) << "Unsupported intern content Python uint type";
                        return StorageClass::UNDEFINED;
                }
            }

            int compareBytesToPythonItem(Token tokenValue, const void *lhs, std::size_t lhsSize, PyObject *rhs)
            {
                if (auto result = compareToken(tokenValue, getToken(rhs))) {
                    return result;
                }
                if (tokenValue == Token::String) {
                    auto value = db0::python::PyToolkit::getTypeManager().extractString(rhs);
                    return compareBytesPayload(lhs, lhsSize, value, std::strlen(value));
                }
                auto value = db0::python::PyToolkit::getTypeManager().extractBytes(rhs);
                return compareBytesPayload(lhs, lhsSize, value.m_data, value.m_size);
            }

            int comparePythonTupleToPythonItem(PyObject *lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Tuple, getToken(rhs))) {
                    return result;
                }
                return comparePythonTuple(lhs, rhs);
            }

            int comparePythonSetToPythonItem(PyObject *lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Set, getToken(rhs))) {
                    return result;
                }
                return comparePythonSet(lhs, rhs);
            }

            int comparePythonDictToPythonItem(PyObject *lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Dict, getToken(rhs))) {
                    return result;
                }
                return comparePythonDict(lhs, rhs);
            }

            int comparePythonMemoToPythonItem(PyObject *lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Object, getToken(rhs))) {
                    return result;
                }
                return comparePythonMemo(lhs, rhs);
            }

            int comparePythonTuple(PyObject *lhs, PyObject *rhs)
            {
                if (auto result = compareToken(Token::Tuple, Token::Tuple)) {
                    return result;
                }
                auto lhsCount = getPythonSequenceSize(lhs);
                auto rhsCount = getPythonSequenceSize(rhs);
                if (auto result = compareScalar<std::uint64_t>(lhsCount, rhsCount)) {
                    return result;
                }
                for (std::size_t i = 0; i < lhsCount; ++i) {
                    if (auto result = compare(getPythonSequenceItem(lhs, i), getPythonSequenceItem(rhs, i))) {
                        return result;
                    }
                }
                return 0;
            }

            int comparePythonTupleToTuple(PyObject *lhs, const o_py_tuple &rhs)
            {
                if (auto result = compareToken(Token::Tuple, Token::Tuple)) {
                    return result;
                }
                auto lhsCount = getPythonSequenceSize(lhs);
                if (auto result = compareScalar<std::uint64_t>(lhsCount, rhs.size())) {
                    return result;
                }
                auto rhsIt = rhs.begin();
                for (std::size_t i = 0; i < lhsCount; ++i, ++rhsIt) {
                    if (auto result = comparePythonItemToTupleItem(getPythonSequenceItem(lhs, i), *rhsIt)) {
                        return result;
                    }
                }
                return 0;
            }

            int comparePythonItemToTupleItem(PyObject *lhs, const o_tuple_item &rhs)
            {
                switch (getToken(lhs)) {
                    case Token::None:
                        return compareToken(Token::None, getToken(rhs));
                    case Token::Bool:
                        return compare(lhs == Py_True, rhs);
                    case Token::Int: {
                        auto value = PyLong_AsLongLong(lhs);
                        if (PyErr_Occurred()) {
                            PyErr_Clear();
                            THROWF(db0::InputException) << "Python integer is out of int64 range";
                        }
                        return compare(static_cast<std::int64_t>(value), rhs);
                    }
                    case Token::Double:
                        return compare(PyFloat_AsDouble(lhs), rhs);
                    case Token::String: {
                        auto value = db0::python::PyToolkit::getTypeManager().extractString(lhs);
                        return compareBytesWithToken(Token::String, value, std::strlen(value), rhs);
                    }
                    case Token::Bytes: {
                        auto value = db0::python::PyToolkit::getTypeManager().extractBytes(lhs);
                        return compareBytesWithToken(Token::Bytes, value.m_data, value.m_size, rhs);
                    }
                    case Token::Tuple:
                        return comparePythonTuple(lhs, rhs);
                    case Token::Set:
                        return comparePythonSet(lhs, rhs);
                    case Token::Dict:
                        return comparePythonDict(lhs, rhs);
                    case Token::Object:
                        return comparePythonMemo(lhs, rhs);
                    case Token::UInt: {
                        auto &typeManager = db0::python::PyToolkit::getTypeManager();
                        auto typeId = typeManager.getTypeId(lhs);
                        return compare(getStorageClassForPythonUInt(typeId), typeManager.extractUInt64(typeId, lhs), rhs);
                    }
                    default:
                        break;
                }
                THROWF(db0::InputException) << "Unsupported intern content Python type: " << Py_TYPE(lhs)->tp_name;
                return 0;
            }

            int comparePythonSet(PyObject *lhs, PyObject *rhs)
            {
                if (!PySet_Check(lhs) || !PySet_Check(rhs)) {
                    THROWF(db0::InputException) << "Intern set content expects a Python set";
                }
                auto lhsSize = PySet_GET_SIZE(lhs);
                auto rhsSize = PySet_GET_SIZE(rhs);
                if (lhsSize < 0 || rhsSize < 0) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python set size";
                }
                if (auto result = compareToken(Token::Set, Token::Set)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhsSize, rhsSize)) {
                    return result;
                }
                auto iterator = Py_OWN(PyObject_GetIter(lhs));
                if (!iterator) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Intern set content expects a Python set";
                }
                Py_FOR(item, iterator) {
                    auto contains = PySet_Contains(rhs, *item);
                    if (contains < 0) {
                        PyErr_Clear();
                        THROWF(db0::InputException) << "Unable to lookup Python set item";
                    }
                    if (!contains) {
                        return -1;
                    }
                }
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to iterate Python set";
                }
                return 0;
            }

            int comparePythonSetToSet(PyObject *lhs, const o_py_set &rhs)
            {
                if (!PySet_Check(lhs)) {
                    THROWF(db0::InputException) << "Intern set content expects a Python set";
                }
                auto lhsSize = PySet_GET_SIZE(lhs);
                if (lhsSize < 0) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python set size";
                }
                if (auto result = compareToken(Token::Set, Token::Set)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhsSize, rhs.size())) {
                    return result;
                }
                auto iterator = Py_OWN(PyObject_GetIter(lhs));
                if (!iterator) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Intern set content expects a Python set";
                }
                Py_FOR(item, iterator) {
                    auto element = o_py_set::elementFromPythonObject(*item);
                    if (!rhs.contains(element)) {
                        return -1;
                    }
                }
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to iterate Python set";
                }
                return 0;
            }

            int comparePythonDict(PyObject *lhs, PyObject *rhs)
            {
                if (!PyDict_Check(lhs) || !PyDict_Check(rhs)) {
                    THROWF(db0::InputException) << "Intern dict content expects a Python dict";
                }
                auto lhsSize = PyDict_Size(lhs);
                auto rhsSize = PyDict_Size(rhs);
                if (lhsSize < 0 || rhsSize < 0) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python dict size";
                }
                if (auto result = compareToken(Token::Dict, Token::Dict)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhsSize, rhsSize)) {
                    return result;
                }
                auto iterator = Py_OWN(PyObject_GetIter(lhs));
                if (!iterator) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Intern dict content expects a Python dict";
                }
                Py_FOR(key, iterator) {
                    auto *lhsValue = getPythonDictValue(lhs, *key);
                    auto *rhsValue = PyDict_GetItemWithError(rhs, *key);
                    if (!rhsValue) {
                        if (PyErr_Occurred()) {
                            PyErr_Clear();
                            THROWF(db0::InputException) << "Unable to lookup Python dict key";
                        }
                        return -1;
                    }
                    if (auto result = compare(lhsValue, rhsValue)) {
                        return result;
                    }
                }
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to iterate Python dict";
                }
                return 0;
            }

            int comparePythonDictToDict(PyObject *lhs, const o_py_dict &rhs)
            {
                if (!PyDict_Check(lhs)) {
                    THROWF(db0::InputException) << "Intern dict content expects a Python dict";
                }
                auto lhsSize = PyDict_Size(lhs);
                if (lhsSize < 0) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python dict size";
                }
                if (auto result = compareToken(Token::Dict, Token::Dict)) {
                    return result;
                }
                if (auto result = compareScalar<std::uint64_t>(lhsSize, rhs.size())) {
                    return result;
                }
                auto iterator = Py_OWN(PyObject_GetIter(lhs));
                if (!iterator) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Intern dict content expects a Python dict";
                }
                Py_FOR(key, iterator) {
                    auto *lhsValue = getPythonDictValue(lhs, *key);
                    auto element = o_py_dict::elementFromPythonObject(*key);
                    auto *rhsValue = rhs.get(element);
                    if (!rhsValue) {
                        return -1;
                    }
                    if (auto result = comparePythonItemToTupleItem(lhsValue, *rhsValue)) {
                        return result;
                    }
                }
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to iterate Python dict";
                }
                return 0;
            }

            PyObject *getPythonDictValue(PyObject *dict, PyObject *key)
            {
                auto *value = PyDict_GetItemWithError(dict, key);
                if (!value) {
                    PyErr_Clear();
                    THROWF(db0::InputException) << "Unable to read Python dict value";
                }
                return value;
            }

            int comparePythonMemo(PyObject *lhs, PyObject *rhs)
            {
                return withPythonMemo(lhs, [&](const auto &lhsObject) {
                    return compareObjectToPythonMemo(lhsObject, rhs);
                });
            }

            template <typename ObjectT> int compareObjectToPythonMemo(const ObjectT &lhs, PyObject *rhs)
            {
                return withPythonMemo(rhs, [&](const auto &rhsObject) {
                    return compare(lhs, rhsObject);
                });
            }

            int comparePythonMemoToObject(PyObject *lhs, const o_embedded_object &rhs)
            {
                return withPythonMemo(lhs, [&](const auto &lhsObject) {
                    return compare(lhsObject, rhs);
                });
            }

            int comparePythonMemoToFixed(PyObject *lhs, StorageClass rhsKind, Value rhs)
            {
                return withPythonMemo(lhs, [&](const auto &lhsObject) {
                    return compareObjectToFixed(lhsObject, rhsKind, rhs);
                });
            }

            template <typename FnT> int withPythonMemo(PyObject *pyObject, FnT fn)
            {
                using MemoImmutableObject = db0::python::PyToolkit::TypeManager::MemoImmutableObject;
                if (db0::python::PyEmbeddedMemo_Check(pyObject)) {
                    return fn(db0::python::getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(pyObject)).embeddedObject());
                }
                if (!db0::python::PyToolkit::isMemoImmutableObject(pyObject)) {
                    THROWF(db0::InputException) << "Interned object content can only reference immutable memo objects";
                }
                const auto &memo = db0::python::PyToolkit::getTypeManager()
                    .template extractObject<MemoImmutableObject>(pyObject);
                if (!memo.hasInstance()) {
                    auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
                        InitManager::instance.findInitializer(memo)
                    );
                    if (!initializer) {
                        THROWF(db0::InputException) << "Non-materialized intern memo object has no initializer";
                    }
                    return fn(*initializer);
                }
                return withObjectRef(memo.getUniqueAddress(), fn);
            }

            std::size_t getPythonSequenceSize(PyObject *sequence) const
            {
                if (PyTuple_Check(sequence)) {
                    return static_cast<std::size_t>(PyTuple_GET_SIZE(sequence));
                }
                if (PyList_Check(sequence)) {
                    return static_cast<std::size_t>(PyList_GET_SIZE(sequence));
                }
                THROWF(db0::InputException) << "Intern tuple content expects a Python tuple or list";
                return 0;
            }

            PyObject *getPythonSequenceItem(PyObject *sequence, std::size_t index) const
            {
                if (PyTuple_Check(sequence)) {
                    return PyTuple_GET_ITEM(sequence, static_cast<Py_ssize_t>(index));
                }
                return PyList_GET_ITEM(sequence, static_cast<Py_ssize_t>(index));
            }

            StreamBudget m_lhsBudget;
            StreamBudget m_rhsBudget;
            db0::swine_ptr<db0::Fixture> *m_fixture = nullptr;
        };

        template <typename LhsT, typename RhsT>
        int compareStreams(db0::swine_ptr<db0::Fixture> &fixture, const LhsT &lhs, const RhsT &rhs)
        {
            return InternComparator(&fixture).compare(lhs, rhs);
        }

        int compareWithFixture(
            db0::swine_ptr<db0::Fixture> *fixture, const o_tuple_item &lhs, const o_tuple_item &rhs
        )
        {
            return InternComparator(fixture).compare(lhs, rhs);
        }

        int compareWithFixture(
            db0::swine_ptr<db0::Fixture> *fixture, const o_dict_pair &lhs, const o_dict_pair &rhs
        )
        {
            return InternComparator(fixture).compare(lhs, rhs);
        }

        int compareWithFixture(db0::swine_ptr<db0::Fixture> *fixture, PyObject *lhs, PyObject *rhs)
        {
            return InternComparator(fixture).compare(lhs, rhs);
        }

    }

    std::uint64_t intern_hash(db0::swine_ptr<db0::Fixture> &fixture, const o_embedded_object &object)
    {
        HashSink sink;
        InternStreamer(sink, &fixture).writeObject(object);
        return sink.getValue();
    }

    std::uint64_t intern_hash(
        db0::swine_ptr<db0::Fixture> &fixture, const ImmutableObjectInitializer &initializer
    )
    {
        HashSink sink;
        InternStreamer(sink, &fixture).writeInitializer(initializer);
        return sink.getValue();
    }

    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const o_embedded_object &lhs, const o_embedded_object &rhs
    )
    {
        return compareStreams(fixture, lhs, rhs);
    }

    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const ImmutableObjectInitializer &lhs,
        const o_embedded_object &rhs
    )
    {
        return compareStreams(fixture, lhs, rhs);
    }

    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const o_embedded_object &lhs,
        const ImmutableObjectInitializer &rhs
    )
    {
        return compareStreams(fixture, lhs, rhs);
    }

    int intern_compare(
        db0::swine_ptr<db0::Fixture> &fixture, const ImmutableObjectInitializer &lhs,
        const ImmutableObjectInitializer &rhs
    )
    {
        return compareStreams(fixture, lhs, rhs);
    }
}
