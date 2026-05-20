// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <Python.h>
#include <utils/TestBase.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/core/serialization/bounded_buf_t.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_immutable_object.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/set/o_set.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>

#include <stdexcept>

namespace tests
{

    using namespace db0;
    using namespace db0::object_model;

    class EmbeddedObjectTest: public MemspaceTestBase
    {
    };

    static void throwDecodeError()
    {
        throw std::runtime_error("decode error");
    }

    static ImmutableObjectInitializer &makeInitializer(ObjectInitializerManager &manager, int &object)
    {
        manager.addInitializerFor<ObjectImmutableImpl>(object, std::shared_ptr<Class>());
        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(manager.findInitializer(object));
        if (!initializer) {
            throw std::runtime_error("immutable initializer not found");
        }
        return *initializer;
    }

    TEST_F( EmbeddedObjectTest , testEmbeddedObjectStoresInitializerPlannedFixedTables )
    {
        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        initializer.set({0, 0}, StorageClass::INT64, Value(42));
        initializer.set({1, 0}, StorageClass::PACK_2, Value((Value::FALSE << 0) | (Value::TRUE << 2)));
        initializer.set({100, 0}, StorageClass::DATE, Value(20260519));
        initializer.set({200, 0}, StorageClass::DECIMAL, Value(123456789));

        v_object<o_embedded_object> object(memspace, 77u, initializer);

        ASSERT_EQ(object->getClassRef(), 77u);
        ASSERT_EQ(object->pos_vt().offset(), 0u);
        ASSERT_EQ(object->pos_vt().size(), 2u);
        auto intValue = object->fixedValue(0);
        ASSERT_TRUE(intValue.has_value());
        ASSERT_EQ(intValue->m_kind, StorageClass::INT64);
        ASSERT_EQ(intValue->m_value, 42u);

        auto falseValue = object->fixedValue(1, 0);
        ASSERT_TRUE(falseValue.has_value());
        ASSERT_EQ(falseValue->m_kind, StorageClass::BOOLEAN);
        ASSERT_EQ(falseValue->m_value, 0u);
        auto trueValue = object->fixedValue(1, 1);
        ASSERT_TRUE(trueValue.has_value());
        ASSERT_EQ(trueValue->m_kind, StorageClass::BOOLEAN);
        ASSERT_EQ(trueValue->m_value, 1u);

        auto dateValue = object->fixedValue(100);
        ASSERT_TRUE(dateValue.has_value());
        ASSERT_EQ(dateValue->m_kind, StorageClass::DATE);
        ASSERT_EQ(dateValue->m_value, 20260519u);
        auto decimalValue = object->fixedValue(200);
        ASSERT_TRUE(decimalValue.has_value());
        ASSERT_EQ(decimalValue->m_kind, StorageClass::DECIMAL);
        ASSERT_EQ(decimalValue->m_value, 123456789u);

        std::pair<StorageClass, Value> indexedValue;
        ASSERT_TRUE(object->index_vt().find(100, indexedValue));
        ASSERT_EQ(indexedValue.first, StorageClass::DATE);
        ASSERT_EQ(indexedValue.second, Value(20260519));
        ASSERT_TRUE(object->index_vt().find(200, indexedValue));
        ASSERT_EQ(indexedValue.first, StorageClass::DECIMAL);
        ASSERT_EQ(indexedValue.second, Value(123456789));

        ASSERT_FALSE(object->fixedValue(999).has_value());
    }

    TEST_F( EmbeddedObjectTest , testImmutableRootEncapsulatesEmbeddedObjectStorage )
    {
        Py_Initialize();

        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        initializer.set({0, 0}, StorageClass::INT64, Value(42));

        auto pyString = Py_OWN(PyUnicode_FromString("root variable string"));
        initializer.setObject(
            {300, 0}, StorageClass::STRING_REF, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyString.get())
        );

        v_object<o_immutable_object> object(memspace, 88u, std::make_pair(1u, 2u), 1u, initializer);

        ASSERT_EQ(object->getClassRef(), 88u);
        ASSERT_TRUE(object->hasAnyRefs());
        auto fixedValue = object->fixedValue(0);
        ASSERT_TRUE(fixedValue.has_value());
        ASSERT_EQ(fixedValue->m_kind, StorageClass::INT64);
        ASSERT_EQ(fixedValue->m_value, 42u);

        auto *variableValue = object->variableValue(300);
        ASSERT_NE(variableValue, nullptr);
        ASSERT_EQ(variableValue->itemKind(), StorageClass::STRING_REF);
        ASSERT_EQ(variableValue->stringPayload().toString(), "root variable string");
        ASSERT_EQ(&object->embeddedObject().pos_vt(), &object->pos_vt());
        ASSERT_EQ(&object->embeddedObject().index_vt(), &object->index_vt());
        ASSERT_EQ(&object->embeddedObject().field_map(), &object->field_map());
    }

    TEST_F( EmbeddedObjectTest , testEmbeddedObjectStoresVariableFieldsInDictMap )
    {
        Py_Initialize();

        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        auto pyString = Py_OWN(PyUnicode_FromString("variable string"));
        const char rawBytes[] = { 0x01, 0x02, 0x03 };
        auto pyBytes = Py_OWN(PyBytes_FromStringAndSize(rawBytes, sizeof(rawBytes)));
        initializer.setObject(
            {300, 0}, StorageClass::STRING_REF, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyString.get())
        );
        initializer.setObject(
            {301, 0}, StorageClass::DB0_BYTES, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyBytes.get())
        );

        v_object<o_embedded_object> object(memspace, 88u, initializer);

        ASSERT_EQ(object->getClassRef(), 88u);
        auto *stringValue = object->variableValue(300);
        ASSERT_NE(stringValue, nullptr);
        ASSERT_EQ(stringValue->itemKind(), StorageClass::STRING_REF);
        ASSERT_EQ(stringValue->stringPayload().toString(), "variable string");
        auto *bytesValue = object->variableValue(301);
        ASSERT_NE(bytesValue, nullptr);
        ASSERT_EQ(bytesValue->itemKind(), StorageClass::DB0_BYTES);
        ASSERT_EQ(bytesValue->bytesPayload().size(), 3u);
        ASSERT_EQ(bytesValue->bytesPayload().begin()[0], std::byte{0x01});
        ASSERT_EQ(bytesValue->bytesPayload().begin()[1], std::byte{0x02});
        ASSERT_EQ(bytesValue->bytesPayload().begin()[2], std::byte{0x03});
        ASSERT_EQ(object->variableValue(999), nullptr);
    }

    TEST_F( EmbeddedObjectTest , testEmbeddedObjectUsesLatestVariableFieldMapValue )
    {
        Py_Initialize();

        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        auto pyString1 = Py_OWN(PyUnicode_FromString("old value"));
        auto pyString2 = Py_OWN(PyUnicode_FromString("new value"));
        initializer.setObject(
            {300, 0}, StorageClass::STRING_REF, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyString1.get())
        );
        initializer.setObject(
            {300, 0}, StorageClass::STRING_REF, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyString2.get())
        );

        ImmutableObjectInitializer::ObjectSharedPtr storedObject;
        ASSERT_TRUE(initializer.tryGetObjectAt({300, 0}, storedObject));
        ASSERT_EQ(storedObject.get(), pyString2.get());

        v_object<o_embedded_object> object(memspace, 88u, initializer);

        auto *stringValue = object->variableValue(300);
        ASSERT_NE(stringValue, nullptr);
        ASSERT_EQ(stringValue->itemKind(), StorageClass::STRING_REF);
        ASSERT_EQ(stringValue->stringPayload().toString(), "new value");
    }

    TEST_F( EmbeddedObjectTest , testEmbeddedObjectTombstoneRemovesVariableFieldMapValue )
    {
        Py_Initialize();

        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        auto pyString = Py_OWN(PyUnicode_FromString("old variable value"));
        initializer.setObject(
            {300, 0}, StorageClass::STRING_REF, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyString.get())
        );
        initializer.set({300, 0}, StorageClass::INT64, Value(91));

        ImmutableObjectInitializer::ObjectSharedPtr storedObject;
        ASSERT_FALSE(initializer.tryGetObjectAt({300, 0}, storedObject));

        v_object<o_embedded_object> object(memspace, 88u, initializer);

        ASSERT_EQ(object->variableValue(300), nullptr);
        auto fixedValue = object->fixedValue(300);
        ASSERT_TRUE(fixedValue.has_value());
        ASSERT_EQ(fixedValue->m_kind, StorageClass::INT64);
        ASSERT_EQ(fixedValue->m_value, 91u);
    }

    TEST_F( EmbeddedObjectTest , testEmbeddedObjectStoresNestedTuplePayload )
    {
        Py_Initialize();

        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        auto pyList = Py_OWN(PyList_New(2));
        db0::python::PySafeList_SetItem(pyList.get(), 0, Py_OWN(PyLong_FromLong(700)));
        db0::python::PySafeList_SetItem(pyList.get(), 1, Py_OWN(PyUnicode_FromString("value")));
        initializer.setObject(
            {400, 0}, StorageClass::DB0_LIST, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyList.get())
        );

        v_object<o_embedded_object> object(memspace, 12u, initializer);

        auto *tupleValue = object->variableValue(400);
        ASSERT_NE(tupleValue, nullptr);
        ASSERT_EQ(tupleValue->itemKind(), StorageClass::DB0_TUPLE);
        const auto &payload = tupleValue->embeddedPayload();
        const auto &tuple = o_tuple<>::__const_ref(payload.begin());
        ASSERT_EQ(tuple.size(), 2u);
        ASSERT_EQ(tuple.item(0).packedIntPayload().value(), 700u);
        ASSERT_EQ(tuple.item(1).stringPayload().toString(), "value");
    }

    TEST_F( EmbeddedObjectTest , testComplexEmbeddedObjectStoresMultipleNestedCollections )
    {
        Py_Initialize();

        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        initializer.set({0, 0}, StorageClass::INT64, Value(123));
        initializer.set({20, 0}, StorageClass::DATE, Value(20260519));

        auto pyList = Py_OWN(PyList_New(3));
        db0::python::PySafeList_SetItem(pyList.get(), 0, Py_OWN(PyLong_FromLong(7)));
        db0::python::PySafeList_SetItem(pyList.get(), 1, Py_OWN(PyUnicode_FromString("seven")));
        db0::python::PySafeList_SetItem(pyList.get(), 2, Py_OWN(PyBool_FromLong(1)));
        initializer.setObject(
            {100, 0}, StorageClass::DB0_LIST, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyList.get())
        );

        auto pySet = Py_OWN(PySet_New(nullptr));
        db0::python::PySafeSet_Add(pySet.get(), Py_OWN(PyLong_FromLong(10)));
        db0::python::PySafeSet_Add(pySet.get(), Py_OWN(PyUnicode_FromString("ten")));
        initializer.setObject(
            {101, 0}, StorageClass::DB0_SET, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pySet.get())
        );

        auto pyDict = Py_OWN(PyDict_New());
        db0::python::PySafeDict_SetItem(
            pyDict.get(), Py_OWN(PyUnicode_FromString("name")), Py_OWN(PyUnicode_FromString("dbzero"))
        );
        db0::python::PySafeDict_SetItem(
            pyDict.get(), Py_OWN(PyUnicode_FromString("count")), Py_OWN(PyLong_FromLong(3))
        );
        initializer.setObject(
            {102, 0}, StorageClass::DB0_DICT, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyDict.get())
        );

        auto measured = o_embedded_object::measure(144u, initializer);
        v_object<o_embedded_object> object(memspace, 144u, initializer);

        ASSERT_EQ(object->sizeOf(), measured);
        ASSERT_EQ(object->getClassRef(), 144u);
        auto fixedValue = object->fixedValue(0);
        ASSERT_TRUE(fixedValue.has_value());
        ASSERT_EQ(fixedValue->m_kind, StorageClass::INT64);
        ASSERT_EQ(fixedValue->m_value, 123u);
        auto dateValue = object->fixedValue(20);
        ASSERT_TRUE(dateValue.has_value());
        ASSERT_EQ(dateValue->m_kind, StorageClass::DATE);
        ASSERT_EQ(dateValue->m_value, 20260519u);

        auto *listValue = object->variableValue(100);
        ASSERT_NE(listValue, nullptr);
        ASSERT_EQ(listValue->itemKind(), StorageClass::DB0_TUPLE);
        const auto &embeddedTuple = o_tuple<>::__const_ref(listValue->embeddedPayload().begin());
        ASSERT_EQ(embeddedTuple.size(), 3u);
        ASSERT_EQ(embeddedTuple.item(0).packedIntPayload().value(), 7u);
        ASSERT_EQ(embeddedTuple.item(1).stringPayload().toString(), "seven");
        ASSERT_EQ(embeddedTuple.item(2).boolPayload().value(), true);

        auto *setValue = object->variableValue(101);
        ASSERT_NE(setValue, nullptr);
        ASSERT_EQ(setValue->itemKind(), StorageClass::DB0_SET);
        const auto &embeddedSet = o_set::__const_ref(setValue->embeddedPayload().begin());
        ASSERT_EQ(embeddedSet.size(), 2u);
        ASSERT_TRUE(embeddedSet.contains(o_set::Element::integer(10)));
        ASSERT_TRUE(embeddedSet.contains(o_set::Element::string("ten")));

        auto *dictValue = object->variableValue(102);
        ASSERT_NE(dictValue, nullptr);
        ASSERT_EQ(dictValue->itemKind(), StorageClass::DB0_DICT);
        const auto &embeddedDict = o_dict::__const_ref(dictValue->embeddedPayload().begin());
        ASSERT_EQ(embeddedDict.size(), 2u);
        auto *nameValue = embeddedDict.get(o_dict::Element::string("name"));
        ASSERT_NE(nameValue, nullptr);
        ASSERT_EQ(nameValue->stringPayload().toString(), "dbzero");
        auto *countValue = embeddedDict.get(o_dict::Element::string("count"));
        ASSERT_NE(countValue, nullptr);
        ASSERT_EQ(countValue->packedIntPayload().value(), 3u);
    }

    TEST_F( EmbeddedObjectTest , testEmbeddedObjectMeasureSizeOfAndSafeSizeOf )
    {
        Py_Initialize();

        auto memspace = getMemspace();
        int sourceObject = 0;
        ObjectInitializerManager manager;
        auto &initializer = makeInitializer(manager, sourceObject);
        initializer.set({4, 0}, StorageClass::INT64, Value(5));
        initializer.set({50, 0}, StorageClass::TIME, Value(600));
        auto pyString = Py_OWN(PyUnicode_FromString("payload"));
        initializer.setObject(
            {100, 0}, StorageClass::STRING_REF, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(pyString.get())
        );

        v_object<o_embedded_object> object(memspace, 99u, initializer);
        auto *begin = reinterpret_cast<const std::byte *>(object.getData());
        auto measured = o_embedded_object::measure(99u, initializer);

        ASSERT_EQ(object->sizeOf(), measured);
        ASSERT_EQ(o_embedded_object::safeSizeOf(begin), measured);
        ASSERT_EQ(o_embedded_object::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + measured)), measured);
        for (std::size_t truncatedSize = 0; truncatedSize < measured; ++truncatedSize) {
            ASSERT_THROW(
                o_embedded_object::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + truncatedSize)),
                std::runtime_error
            ) << "truncated size: " << truncatedSize;
        }
    }

    TEST_F( EmbeddedObjectTest , testEmbeddedObjectStorageClassValuesAreStable )
    {
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::UNDEFINED), 0u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::NONE), 1u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::STRING_REF), 2u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::INT64), 4u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::PTIME64), 5u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::FP_NUMERIC64), 6u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DATE), 7u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DATETIME), 8u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DATETIME_TZ), 9u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::TIME), 10u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::TIME_TZ), 11u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DECIMAL), 12u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::OBJECT_REF), 13u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DB0_DICT), 15u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DB0_SET), 16u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DB0_TUPLE), 17u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DB0_BYTES), 23u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::BOOLEAN), 28u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::PACK_2), 29u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::PACKED_INT32), 253u);
    }

}
