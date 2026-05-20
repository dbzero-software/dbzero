// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <Python.h>
#include <utils/utils.hpp>
#include <utils/SubClass.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>

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

        std::pair<StorageClass, Value> stored_value;
        ASSERT_TRUE(initializer->tryGetAt({9, 0}, stored_value));
        ASSERT_EQ(stored_value.first, StorageClass::STRING_REF);
        ASSERT_EQ(stored_value.second, Value(123));

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
        ASSERT_EQ(variable_value->itemKind(), StorageClass::STRING_REF);
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
        ASSERT_EQ(variable_value->itemKind(), StorageClass::DB0_TUPLE);

        const auto &tuple = o_tuple<>::__const_ref(variable_value->embeddedPayload().begin());
        ASSERT_EQ(tuple.size(), 2u);
        ASSERT_EQ(tuple.item(0).itemKind(), StorageClass::PACKED_INT32);
        ASSERT_EQ(tuple.item(0).packedIntPayload().value(), 7u);
        ASSERT_EQ(tuple.item(1).itemKind(), StorageClass::STRING_REF);
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
        ASSERT_EQ(list_value->itemKind(), StorageClass::DB0_TUPLE);
        const auto &tuple = o_tuple<>::__const_ref(list_value->embeddedPayload().begin());
        ASSERT_EQ(tuple.size(), 2u);
        ASSERT_EQ(tuple.item(0).packedIntPayload().value(), 7u);
        ASSERT_EQ(tuple.item(1).stringPayload().toString(), "seven");

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
