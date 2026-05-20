// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <Python.h>
#include <utils/utils.hpp>
#include <utils/SubClass.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/PyAPI.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/core/memory/SlabAllocatorConfig.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

#include <cstring>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;

namespace tests

{
    
    class ObjectInitializerTest: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {
            drop(file_name);
        }
    };

    static db0::python::shared_py_object<PyTypeObject *> makeImmutableMemoType()
    {
        static std::uint64_t memoTypeIndex = 0;
        auto className = std::string("ObjectInitializerNestedImmutable") + std::to_string(memoTypeIndex);
        auto typeId = "tests/" + className;
        ++memoTypeIndex;

        if (PyRun_SimpleString(("class " + className + ": pass\n").c_str()) != 0) {
            return {};
        }

        auto mainModule = Py_BORROW(PyImport_AddModule("__main__"));
        auto pyClass = Py_OWN(PyObject_GetAttrString(mainModule.get(), className.c_str()));
        auto args = Py_OWN(PyTuple_Pack(1, pyClass.get()));
        auto kwargs = Py_OWN(PyDict_New());
        auto pyTypeId = Py_OWN(PyUnicode_FromString(typeId.c_str()));
        auto pyImmutable = Py_OWN(PyBool_FromLong(1));
        if (!mainModule.get() || !pyClass.get() || !args.get() || !kwargs.get()
            || !pyTypeId.get() || !pyImmutable.get()) {
            return {};
        }
        db0::python::PySafeDict_SetItemString(kwargs.get(), "id", std::move(pyTypeId));
        db0::python::PySafeDict_SetItemString(kwargs.get(), "immutable", std::move(pyImmutable));

        return db0::python::shared_py_object<PyTypeObject *>(
            reinterpret_cast<PyTypeObject *>(db0::python::PyAPI_wrapPyClass(nullptr, args.get(), kwargs.get())),
            false
        );
    }

    TEST_F( ObjectInitializerTest, testIncompletePosVT )
    {   
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(mock_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, mock_class);
        auto &cut = *manager.findInitializer(*object_1);
        // fill rate = 3 / 4
        cut.set({0, 0}, StorageClass::INT64, Value(0));
        cut.set({1, 0}, StorageClass::POOLED_STRING, Value(0));
        cut.set({3, 0}, StorageClass::INT64, Value(0));
        
        PosVT::Data pos_vt_data;
        unsigned int pos_vt_offset = 0;
        cut.getData(pos_vt_data, pos_vt_offset);
        ASSERT_EQ(pos_vt_offset, 0);
        // NOTE: there should be 4 elements in pos-vt, but only 3 are filled
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        ASSERT_EQ(pos_vt_data.m_types[0], StorageClass::INT64);
        ASSERT_EQ(pos_vt_data.m_types[1], StorageClass::POOLED_STRING);
        ASSERT_EQ(pos_vt_data.m_types[2], StorageClass::UNDEFINED);
        ASSERT_EQ(pos_vt_data.m_types[3], StorageClass::INT64);
        
        object_1->~Object();
        workspace.close();
    }
    
    TEST_F( ObjectInitializerTest, testReducedPosVT )
    {    
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(mock_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, mock_class);
        auto &cut = *manager.findInitializer(*object_1);
        // NOTE: only the first 4 elements should be selected to pos-vt
        cut.set({0, 0}, StorageClass::INT64, Value(0));
        cut.set({13, 0}, StorageClass::INT64, Value(0));
        cut.set({1, 0}, StorageClass::POOLED_STRING, Value(0));
        cut.set({3, 0}, StorageClass::INT64, Value(0));
        cut.set({12, 0}, StorageClass::INT64, Value(0));
        
        PosVT::Data pos_vt_data;
        unsigned int pos_vt_offset = 0;
        cut.getData(pos_vt_data, pos_vt_offset);
        // NOTE: there should be 4 elements in pos-vt, but only 3 are filled
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        object_1->~Object();
        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testReducedPosVTWithOffset )
    {    
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(mock_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, mock_class);
        auto &cut = *manager.findInitializer(*object_1);
        // NOTE: only the first 4 elements should be selected to pos-vt
        cut.set({7, 0}, StorageClass::INT64, Value(0));
        cut.set({15, 0}, StorageClass::INT64, Value(0));
        cut.set({4, 0}, StorageClass::POOLED_STRING, Value(0));
        cut.set({6, 0}, StorageClass::INT64, Value(0));
        cut.set({13, 0}, StorageClass::INT64, Value(0));
        
        PosVT::Data pos_vt_data;
        unsigned int pos_vt_offset = 0;
        cut.getData(pos_vt_data, pos_vt_offset);
        ASSERT_EQ(pos_vt_offset, 4);
        // elements: 4, 6, 7 should only be picked for pos-vt
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        object_1->~Object();
        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testManagerCreatesImmutableInitializerForImmutableObjects )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        int object = 0;
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        ObjectInitializerManager manager;
        manager.addInitializerFor<ObjectImmutableImpl>(object, mock_class);

        auto *initializer = manager.findInitializer(object);
        ASSERT_NE(initializer, nullptr);
        ASSERT_NE(dynamic_cast<ImmutableObjectInitializer *>(initializer), nullptr);
        ASSERT_EQ(dynamic_cast<ImmutableObjectInitializer *>(initializer)->getClassPtr(), mock_class);

        manager.closeInitializer(object);
        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutableInitializerStoresObjectForNonFixedValues )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        int object = 0;
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        ObjectInitializerManager manager;
        manager.addInitializerFor<ObjectImmutableImpl>(object, mock_class);
        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(manager.findInitializer(object));
        ASSERT_NE(initializer, nullptr);

        auto py_value = Py_OWN(PyLong_FromLong(42));
        ImmutableObjectInitializer::ObjectSharedPtr object_value(py_value.get());
        initializer->setObject({9, 0}, StorageClass::STRING_REF, Value(123), object_value);
        ASSERT_FALSE(initializer->empty());

        std::pair<StorageClass, Value> stored_value;
        ASSERT_FALSE(initializer->tryGetAt({9, 0}, stored_value));

        ImmutableObjectInitializer::ObjectSharedPtr stored_object;
        ASSERT_TRUE(initializer->tryGetObjectAt({9, 0}, stored_object));
        ASSERT_EQ(stored_object.get(), py_value.get());

        ASSERT_TRUE(initializer->remove({9, 0}));
        ASSERT_FALSE(initializer->tryGetObjectAt({9, 0}, stored_object));

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutableInitializerDoesNotStoreObjectForFixedValues )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        int object = 0;
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        ObjectInitializerManager manager;
        manager.addInitializerFor<ObjectImmutableImpl>(object, mock_class);
        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(manager.findInitializer(object));
        ASSERT_NE(initializer, nullptr);

        auto py_value = Py_OWN(PyLong_FromLong(42));
        initializer->setObject({3, 0}, StorageClass::INT64, Value(42), ImmutableObjectInitializer::ObjectSharedPtr(py_value.get()));

        ImmutableObjectInitializer::ObjectSharedPtr stored_object;
        ASSERT_FALSE(initializer->tryGetObjectAt({3, 0}, stored_object));

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutableInitializerCollectsVariableValuesIntoFieldMap )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        int object = 0;
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        ObjectInitializerManager manager;
        manager.addInitializerFor<ObjectImmutableImpl>(object, mock_class);
        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(manager.findInitializer(object));
        ASSERT_NE(initializer, nullptr);

        initializer->set({0, 0}, StorageClass::INT64, Value(17));

        auto py_string = Py_OWN(PyUnicode_FromString("variable-value"));
        initializer->setObject(
            {4, 0}, StorageClass::STRING_REF, Value(123),
            ImmutableObjectInitializer::ObjectSharedPtr(py_string.get())
        );

        auto measured = o_embedded_object::measure(33u, *initializer);
        std::vector<std::byte> buffer(measured);
        auto &embedded_object = o_embedded_object::__new(buffer.data(), 33u, *initializer);

        ASSERT_EQ(embedded_object.sizeOf(), measured);
        auto fixed_value = embedded_object.fixedValue(0);
        ASSERT_TRUE(fixed_value.has_value());
        ASSERT_EQ(fixed_value->m_kind, StorageClass::INT64);
        ASSERT_EQ(fixed_value->m_value, 17u);

        auto *variable_value = embedded_object.variableValue(4);
        ASSERT_NE(variable_value, nullptr);
        ASSERT_EQ(variable_value->itemKind(), StorageClass::EMBEDDED_STRING);
        ASSERT_EQ(variable_value->stringPayload().toString(), "variable-value");

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutableInitializerEmbedsPythonListFieldMapValue )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        int object = 0;
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        ObjectInitializerManager manager;
        manager.addInitializerFor<ObjectImmutableImpl>(object, mock_class);
        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(manager.findInitializer(object));
        ASSERT_NE(initializer, nullptr);

        auto py_list = Py_OWN(PyList_New(2));
        db0::python::PySafeList_SetItem(py_list.get(), 0, Py_OWN(PyLong_FromLong(7)));
        db0::python::PySafeList_SetItem(py_list.get(), 1, Py_OWN(PyUnicode_FromString("seven")));
        initializer->setObject(
            {8, 0}, StorageClass::DB0_LIST, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(py_list.get())
        );

        auto measured = o_embedded_object::measure(33u, *initializer);
        std::vector<std::byte> buffer(measured);
        auto &embedded_object = o_embedded_object::__new(buffer.data(), 33u, *initializer);

        ASSERT_EQ(embedded_object.sizeOf(), measured);
        ASSERT_FALSE(embedded_object.fixedValue(8).has_value());
        auto *variable_value = embedded_object.variableValue(8);
        ASSERT_NE(variable_value, nullptr);
        ASSERT_EQ(variable_value->itemKind(), StorageClass::EMBEDDED_TUPLE);

        const auto &tuple = o_tuple<>::__const_ref(variable_value->embeddedPayload().begin());
        ASSERT_EQ(tuple.size(), 2u);
        ASSERT_EQ(tuple.item(0).itemKind(), StorageClass::PACKED_INT32);
        ASSERT_EQ(tuple.item(0).packedIntPayload().value(), 7u);
        ASSERT_EQ(tuple.item(1).itemKind(), StorageClass::EMBEDDED_STRING);
        ASSERT_EQ(tuple.item(1).stringPayload().toString(), "seven");

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testEmbeddedObjectMeasureDoesNotConsumeImmutableVariableValues )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        int object = 0;
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        ObjectInitializerManager manager;
        manager.addInitializerFor<ObjectImmutableImpl>(object, mock_class);
        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(manager.findInitializer(object));
        ASSERT_NE(initializer, nullptr);

        initializer->setObject({0, 0}, StorageClass::INT64, Value(17), {});

        auto py_list = Py_OWN(PyList_New(2));
        db0::python::PySafeList_SetItem(py_list.get(), 0, Py_OWN(PyLong_FromLong(7)));
        db0::python::PySafeList_SetItem(py_list.get(), 1, Py_OWN(PyUnicode_FromString("seven")));
        initializer->setObject(
            {8, 0}, StorageClass::DB0_LIST, Value(0),
            ImmutableObjectInitializer::ObjectSharedPtr(py_list.get())
        );

        auto measured = o_embedded_object::measure(33u, *initializer);

        ImmutableObjectInitializer::ObjectSharedPtr stored_object;
        ASSERT_TRUE(initializer->tryGetObjectAt({8, 0}, stored_object));
        ASSERT_EQ(stored_object.get(), py_list.get());

        std::vector<std::byte> buffer(measured);
        auto &embedded_object = o_embedded_object::__new(buffer.data(), 33u, *initializer);

        ASSERT_EQ(embedded_object.sizeOf(), measured);
        auto fixed_value = embedded_object.fixedValue(0);
        ASSERT_TRUE(fixed_value.has_value());
        ASSERT_EQ(fixed_value->m_kind, StorageClass::INT64);
        ASSERT_EQ(fixed_value->m_value, 17u);

        auto *list_value = embedded_object.variableValue(8);
        ASSERT_NE(list_value, nullptr);
        ASSERT_EQ(list_value->itemKind(), StorageClass::EMBEDDED_TUPLE);
        const auto &tuple = o_tuple<>::__const_ref(list_value->embeddedPayload().begin());
        ASSERT_EQ(tuple.size(), 2u);
        ASSERT_EQ(tuple.item(0).packedIntPayload().value(), 7u);
        ASSERT_EQ(tuple.item(1).stringPayload().toString(), "seven");

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testObjectImmutableImplPostInitUsesEmbeddedStorageAndNoKVIndex )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            auto py_int = Py_OWN(PyLong_FromLong(42));
            object.setPreInit("value", db0::bindings::TypeId::INTEGER, py_int.get());
            auto py_string = Py_OWN(PyUnicode_FromString("immutable payload"));
            object.setPreInit("name", db0::bindings::TypeId::STRING, py_string.get());

            {
                db0::FixtureLock lock(fixture);
                object.postInit(lock);
            }

            auto layout = object.getFieldLayout();
            ASSERT_TRUE(layout.m_kv_index_fields.empty());
            ASSERT_FALSE(layout.m_pos_vt_fields.empty());

            std::optional<FixedValue> fixedValue;
            for (std::uint32_t index = 0; index < 32 && !fixedValue.has_value(); ++index) {
                auto candidate = object->fixedValue(index);
                if (candidate && candidate->m_value == 42u) {
                    fixedValue = candidate;
                }
            }
            ASSERT_TRUE(fixedValue.has_value());

            const o_tuple_item *variableValue = nullptr;
            for (std::uint32_t index = 0; index < 32 && !variableValue; ++index) {
                auto *candidate = object->variableValue(index);
                if (candidate && candidate->itemKind() == StorageClass::EMBEDDED_STRING) {
                    variableValue = candidate;
                }
            }
            ASSERT_NE(variableValue, nullptr);
            ASSERT_EQ(variableValue->stringPayload().toString(), "immutable payload");
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutableGetRetrievesEmbeddedStringAndBytes )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            auto pyString = Py_OWN(PyUnicode_FromString("embedded read string"));
            const char rawBytes[] = { 'a', '\0', 'z' };
            auto pyBytes = Py_OWN(PyBytes_FromStringAndSize(rawBytes, sizeof(rawBytes)));
            object.setPreInit("name", db0::bindings::TypeId::STRING, pyString.get());
            object.setPreInit("payload", db0::bindings::TypeId::BYTES, pyBytes.get());

            {
                db0::FixtureLock lock(fixture);
                object.postInit(lock);
            }

            auto stringResult = object.get("name");
            ASSERT_STREQ(PyUnicode_AsUTF8(stringResult.get()), "embedded read string");

            auto bytesResult = object.get("payload");
            ASSERT_TRUE(PyBytes_Check(bytesResult.get()));
            ASSERT_EQ(PyBytes_GET_SIZE(bytesResult.get()), static_cast<Py_ssize_t>(sizeof(rawBytes)));
            ASSERT_EQ(std::memcmp(PyBytes_AsString(bytesResult.get()), rawBytes, sizeof(rawBytes)), 0);
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutablePreInitGetRetrievesEmbeddedStringAndBytes )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            auto pyString = Py_OWN(PyUnicode_FromString("pre-init embedded string"));
            const char rawBytes[] = { 'p', '\0', 'i' };
            auto pyBytes = Py_OWN(PyBytes_FromStringAndSize(rawBytes, sizeof(rawBytes)));
            object.setPreInit("name", db0::bindings::TypeId::STRING, pyString.get());
            object.setPreInit("payload", db0::bindings::TypeId::BYTES, pyBytes.get());

            auto stringResult = object.get("name");
            ASSERT_EQ(stringResult.get(), pyString.get());

            auto bytesResult = object.get("payload");
            ASSERT_EQ(bytesResult.get(), pyBytes.get());
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutableLargeStringAndBytesUseDurableFallback )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            std::string largeString(3 * SlabAllocatorConfig::DEFAULT_PAGE_SIZE, 'x');
            std::string largeBytes(3 * SlabAllocatorConfig::DEFAULT_PAGE_SIZE, 'y');
            auto pyString = Py_OWN(PyUnicode_FromStringAndSize(largeString.data(), largeString.size()));
            auto pyBytes = Py_OWN(PyBytes_FromStringAndSize(largeBytes.data(), largeBytes.size()));
            object.setPreInit("name", db0::bindings::TypeId::STRING, pyString.get());
            object.setPreInit("payload", db0::bindings::TypeId::BYTES, pyBytes.get());

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(object));
            ASSERT_NE(initializer, nullptr);

            auto [nameMemberId, nameIsInitVar] = mock_class->findField("name");
            (void)nameIsInitVar;
            ASSERT_TRUE(nameMemberId);
            auto nameLoc = nameMemberId.get(0).getIndexAndOffset();
            ImmutableObjectInitializer::ObjectSharedPtr storedObject;
            ASSERT_FALSE(initializer->tryGetObjectAt(nameLoc, storedObject));
            std::pair<StorageClass, Value> storedValue;
            ASSERT_TRUE(initializer->tryGetAt(nameLoc, storedValue));
            ASSERT_EQ(storedValue.first, StorageClass::STRING_REF);

            auto [payloadMemberId, payloadIsInitVar] = mock_class->findField("payload");
            (void)payloadIsInitVar;
            ASSERT_TRUE(payloadMemberId);
            auto payloadLoc = payloadMemberId.get(0).getIndexAndOffset();
            ASSERT_FALSE(initializer->tryGetObjectAt(payloadLoc, storedObject));
            ASSERT_TRUE(initializer->tryGetAt(payloadLoc, storedValue));
            ASSERT_EQ(storedValue.first, StorageClass::DB0_BYTES);

            {
                db0::FixtureLock lock(fixture);
                object.postInit(lock);
            }

            ASSERT_EQ(object->variableValue(nameLoc.first), nullptr);
            ASSERT_EQ(object->variableValue(payloadLoc.first), nullptr);
            auto stringResult = object.get("name");
            ASSERT_EQ(std::string(PyUnicode_AsUTF8(stringResult.get())), largeString);
            auto bytesResult = object.get("payload");
            ASSERT_EQ(PyBytes_GET_SIZE(bytesResult.get()), static_cast<Py_ssize_t>(largeBytes.size()));
            ASSERT_EQ(std::memcmp(PyBytes_AsString(bytesResult.get()), largeBytes.data(), largeBytes.size()), 0);
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutablePreInitEmbeddableValueDoesNotCreateDurableMemberValue )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            auto py_string = Py_OWN(PyUnicode_FromString("embedded without durable side object"));
            object.setPreInit("name", db0::bindings::TypeId::STRING, py_string.get());

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(object));
            ASSERT_NE(initializer, nullptr);
            ASSERT_FALSE(initializer->objects().empty());
            auto loc = initializer->objects().back().m_loc;
            ASSERT_EQ(initializer->objects().back().m_storage_class, StorageClass::STRING_REF);

            std::pair<StorageClass, Value> storedValue;
            ASSERT_FALSE(initializer->tryGetAt(loc, storedValue));

            ImmutableObjectInitializer::ObjectSharedPtr storedObject;
            ASSERT_TRUE(initializer->tryGetObjectAt(loc, storedObject));
            ASSERT_EQ(storedObject.get(), py_string.get());
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutablePreInitEmbedsPythonList )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            auto pyList = Py_OWN(PyList_New(2));
            db0::python::PySafeList_SetItem(pyList.get(), 0, Py_OWN(PyLong_FromLong(7)));
            db0::python::PySafeList_SetItem(pyList.get(), 1, Py_OWN(PyUnicode_FromString("seven")));
            object.setPreInit("items", db0::bindings::TypeId::LIST, pyList.get());

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(object));
            ASSERT_NE(initializer, nullptr);

            auto [memberId, isInitVar] = mock_class->findField("items");
            (void)isInitVar;
            ASSERT_TRUE(memberId);
            auto loc = memberId.get(0).getIndexAndOffset();

            std::pair<StorageClass, Value> storedValue;
            ASSERT_FALSE(initializer->tryGetAt(loc, storedValue));

            ImmutableObjectInitializer::ObjectSharedPtr storedObject;
            ASSERT_TRUE(initializer->tryGetObjectAt(loc, storedObject));
            ASSERT_EQ(storedObject.get(), pyList.get());

            {
                db0::FixtureLock lock(fixture);
                object.postInit(lock);
            }

            auto *embeddedValue = object->variableValue(loc.first);
            ASSERT_NE(embeddedValue, nullptr);
            ASSERT_EQ(embeddedValue->itemKind(), StorageClass::EMBEDDED_TUPLE);

            const auto &tuple = o_tuple<>::__const_ref(embeddedValue->embeddedPayload().begin());
            ASSERT_EQ(tuple.size(), 2u);
            ASSERT_EQ(tuple.item(0).itemKind(), StorageClass::PACKED_INT32);
            ASSERT_EQ(tuple.item(0).packedIntPayload().value(), 7u);
            ASSERT_EQ(tuple.item(1).itemKind(), StorageClass::EMBEDDED_STRING);
            ASSERT_EQ(tuple.item(1).stringPayload().toString(), "seven");
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutablePreInitEmbedsNonMaterializedNestedMemo )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto mockClass = getTestClass(fixture);
        auto pyMemoType = makeImmutableMemoType();
        ASSERT_TRUE(pyMemoType.get());
        auto nestedClass = fixture->get<ClassFactory>().getOrCreateType(pyMemoType.get());

        {
            ObjectImmutableImpl object(mockClass);
            auto pyMemo = Py_OWN(reinterpret_cast<db0::python::MemoImmutableObject *>(
                db0::python::MemoObjectStub_new(pyMemoType.get())
            ));
            pyMemo->makeNew(nestedClass);
            auto *nestedInitializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(pyMemo->ext())
            );
            ASSERT_NE(nestedInitializer, nullptr);
            nestedInitializer->set({0, 0}, StorageClass::INT64, Value(17));

            object.setPreInit(
                "inner", db0::bindings::TypeId::MEMO_IMMUTABLE_OBJECT, reinterpret_cast<PyObject *>(pyMemo.get())
            );

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(object));
            ASSERT_NE(initializer, nullptr);

            auto [memberId, isInitVar] = mockClass->findField("inner");
            (void)isInitVar;
            ASSERT_TRUE(memberId);
            auto loc = memberId.get(0).getIndexAndOffset();

            std::pair<StorageClass, Value> storedValue;
            ASSERT_FALSE(initializer->tryGetAt(loc, storedValue));

            ImmutableObjectInitializer::ObjectSharedPtr storedObject;
            ASSERT_TRUE(initializer->tryGetObjectAt(loc, storedObject));
            ASSERT_EQ(storedObject.get(), reinterpret_cast<PyObject *>(pyMemo.get()));
        }

        mockClass.reset();
        nestedClass.reset();
        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testDestroyImmutableRootUnrefsEmbeddedNestedObjectMembers )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto rootClass = getTestClass(fixture);
        auto referencedClass = getTestClass(fixture);
        auto pyMemoType = makeImmutableMemoType();
        ASSERT_TRUE(pyMemoType.get());
        auto nestedClass = fixture->get<ClassFactory>().getOrCreateType(pyMemoType.get());
        auto rootLoc = rootClass->addField("inner", 0).get(0).getIndexAndOffset();
        auto nestedLoc = nestedClass->addField("held", 0).get(0).getIndexAndOffset();

        {
            Object referenced(referencedClass);
            {
                db0::FixtureLock lock(fixture);
                referenced.postInit(lock);
            }
            referenced.incRef(false);
            referenced.incRef(false);
            ASSERT_EQ(referenced.getRefCounts().second, 2u);

            ObjectImmutableImpl root(rootClass);
            auto pyMemo = Py_OWN(reinterpret_cast<db0::python::MemoImmutableObject *>(
                db0::python::MemoObjectStub_new(pyMemoType.get())
            ));
            pyMemo->makeNew(nestedClass);
            auto *nestedInitializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(pyMemo->ext())
            );
            ASSERT_NE(nestedInitializer, nullptr);
            nestedInitializer->set(nestedLoc, StorageClass::OBJECT_REF, Value(referenced.getAddress()));

            auto *rootInitializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(root)
            );
            ASSERT_NE(rootInitializer, nullptr);
            rootInitializer->setObject(
                rootLoc, StorageClass::OBJECT_REF, Value(0),
                ImmutableObjectInitializer::ObjectSharedPtr(reinterpret_cast<PyObject *>(pyMemo.get()))
            );

            {
                db0::FixtureLock lock(fixture);
                root.postInit(lock);
            }

            ASSERT_TRUE(fixture->isAddressValid(root.getAddress(), ObjectImmutableImpl::REALM_ID));
            root.destroy();
            ASSERT_EQ(referenced.getRefCounts().second, 1u);
        }

        rootClass.reset();
        referencedClass.reset();
        nestedClass.reset();
        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testDestroyImmutableRootUnrefsRecursivelyEmbeddedNestedObjectMembers )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto rootClass = getTestClass(fixture);
        auto referencedClass = getTestClass(fixture);
        auto pyMemoType = makeImmutableMemoType();
        ASSERT_TRUE(pyMemoType.get());
        auto nestedClass = fixture->get<ClassFactory>().getOrCreateType(pyMemoType.get());
        auto rootLoc = rootClass->addField("outer", 0).get(0).getIndexAndOffset();
        auto outerLoc = nestedClass->addField("inner", 0).get(0).getIndexAndOffset();
        auto innerLoc = nestedClass->addField("held", 0).get(0).getIndexAndOffset();

        {
            Object referenced(referencedClass);
            {
                db0::FixtureLock lock(fixture);
                referenced.postInit(lock);
            }
            referenced.incRef(false);
            referenced.incRef(false);
            ASSERT_EQ(referenced.getRefCounts().second, 2u);

            auto pyInnerMemo = Py_OWN(reinterpret_cast<db0::python::MemoImmutableObject *>(
                db0::python::MemoObjectStub_new(pyMemoType.get())
            ));
            pyInnerMemo->makeNew(nestedClass);
            auto *innerInitializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(pyInnerMemo->ext())
            );
            ASSERT_NE(innerInitializer, nullptr);
            innerInitializer->set(innerLoc, StorageClass::OBJECT_REF, Value(referenced.getAddress()));

            auto pyOuterMemo = Py_OWN(reinterpret_cast<db0::python::MemoImmutableObject *>(
                db0::python::MemoObjectStub_new(pyMemoType.get())
            ));
            pyOuterMemo->makeNew(nestedClass);
            auto *outerInitializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(pyOuterMemo->ext())
            );
            ASSERT_NE(outerInitializer, nullptr);
            outerInitializer->setObject(
                outerLoc, StorageClass::OBJECT_REF, Value(0),
                ImmutableObjectInitializer::ObjectSharedPtr(reinterpret_cast<PyObject *>(pyInnerMemo.get()))
            );

            ObjectImmutableImpl root(rootClass);
            auto *rootInitializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(root)
            );
            ASSERT_NE(rootInitializer, nullptr);
            rootInitializer->setObject(
                rootLoc, StorageClass::OBJECT_REF, Value(0),
                ImmutableObjectInitializer::ObjectSharedPtr(reinterpret_cast<PyObject *>(pyOuterMemo.get()))
            );

            {
                db0::FixtureLock lock(fixture);
                root.postInit(lock);
            }

            root.destroy();
            ASSERT_EQ(referenced.getRefCounts().second, 1u);
        }

        rootClass.reset();
        referencedClass.reset();
        nestedClass.reset();
        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutablePreInitChangingRegularValueToLoFiClearsEmbeddedObject )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            auto pyString = Py_OWN(PyUnicode_FromString("stale embedded object"));
            object.setPreInit("name", db0::bindings::TypeId::STRING, pyString.get());

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(object));
            ASSERT_NE(initializer, nullptr);
            ASSERT_FALSE(initializer->objects().empty());
            auto regularLoc = initializer->objects().back().m_loc;

            object.setPreInit("name", db0::bindings::TypeId::BOOLEAN, Py_True);

            ImmutableObjectInitializer::ObjectSharedPtr storedObject;
            ASSERT_FALSE(initializer->tryGetObjectAt(regularLoc, storedObject));

            std::pair<StorageClass, Value> storedValue;
            ASSERT_FALSE(initializer->tryGetAt(regularLoc, storedValue));

            auto [memberId, isInitVar] = mock_class->findField("name");
            (void)isInitVar;
            ASSERT_TRUE(memberId);
            ASSERT_TRUE(memberId.hasFidelity(2));
            auto lofiLoc = memberId.get(2).getIndexAndOffset();

            ASSERT_TRUE(initializer->tryGetAt(lofiLoc, storedValue));
            ASSERT_EQ(storedValue.first, StorageClass::PACK_2);
            ASSERT_TRUE(lofi_store<2>::fromValue(storedValue.second).isSet(lofiLoc.second));
            ASSERT_EQ(lofi_store<2>::fromValue(storedValue.second).get(lofiLoc.second), Value::TRUE);
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testImmutableRemovePreInitClearsEmbeddedObject )
    {
        Py_Initialize();

        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::shared_ptr<Class> mock_class = getTestClass(fixture);

        {
            ObjectImmutableImpl object(mock_class);
            auto pyString = Py_OWN(PyUnicode_FromString("removed embedded object"));
            object.setPreInit("name", db0::bindings::TypeId::STRING, pyString.get());

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(object));
            ASSERT_NE(initializer, nullptr);
            ASSERT_FALSE(initializer->objects().empty());
            auto regularLoc = initializer->objects().back().m_loc;

            object.removePreInit("name");

            ImmutableObjectInitializer::ObjectSharedPtr storedObject;
            ASSERT_FALSE(initializer->tryGetObjectAt(regularLoc, storedObject));

            std::pair<StorageClass, Value> storedValue;
            ASSERT_TRUE(initializer->tryGetAt(regularLoc, storedValue));
            ASSERT_EQ(storedValue.first, StorageClass::DELETED);
            ASSERT_EQ(storedValue.second, Value());
        }

        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testPosVTLoFiExclusive )
    {    
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(mock_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, mock_class);
        auto &cut = *manager.findInitializer(*object_1);
        cut.set({1, 0}, StorageClass::INT64, Value(0));
        
        // Lo-fi member slot not included
        PosVT::Data pos_vt_data;
        unsigned int pos_vt_offset = 0;
        cut.getData(pos_vt_data, pos_vt_offset);
        ASSERT_EQ(pos_vt_offset, 1);
        ASSERT_EQ(pos_vt_data.m_types.size(), 1u);
        object_1->~Object();
        workspace.close();
    }

    TEST_F( ObjectInitializerTest, testPosVTLoFiInclusive )
    {    
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefix_name);

        std::vector<char> data(sizeof(Object));
        std::shared_ptr<Class> mock_class = getTestClass(fixture);
        auto object_1 = new (data.data()) Object(mock_class);
        ObjectInitializerManager manager;
        manager.addInitializer(*object_1, mock_class);
        auto &cut = *manager.findInitializer(*object_1);
        cut.set({1, 0}, StorageClass::INT64, Value(0));
        cut.set({2, 0}, StorageClass::INT64, Value(0));
        cut.set({3, 0}, StorageClass::INT64, Value(0));

        // NOTE: even though pos-vt starts at 1, member 0 should also be included
        // this is to pre-allocate space for lo-fi types where the footprint is small enough
        PosVT::Data pos_vt_data;
        unsigned int pos_vt_offset = 0;
        cut.getData(pos_vt_data, pos_vt_offset);
        ASSERT_EQ(pos_vt_offset, 0);
        ASSERT_EQ(pos_vt_data.m_types.size(), 4u);
        object_1->~Object();
        workspace.close();
    }
    
}
