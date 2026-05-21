// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <Python.h>

#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/PyAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/SlabAllocatorConfig.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/EmbeddingMeasure.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <utils/SubClass.hpp>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>

using namespace db0;
using namespace db0::bindings;
using namespace db0::object_model;
using namespace db0::tests;

namespace tests
{

    class EmbeddingMeasureTest: public testing::Test
    {
    public:
        static constexpr const char *prefixName = "embedding-measure-prefix";
        static constexpr const char *fileName = "embedding-measure-prefix.db0";

        void SetUp() override
        {
            Py_Initialize();
            drop(fileName);
        }

        void TearDown() override
        {
            drop(fileName);
        }
    };

    db0::python::shared_py_object<PyTypeObject *> makeMemoType(bool immutable)
    {
        static std::uint64_t memoTypeIndex = 0;
        auto className = std::string("EmbeddingMeasure") + (immutable ? "Immutable" : "Regular")
            + "Memo" + std::to_string(memoTypeIndex);
        auto typeId = "tests/" + className;
        ++memoTypeIndex;

        if (PyRun_SimpleString(("class " + className + ": pass\n").c_str()) != 0) {
            return {};
        }

        auto mainModule = Py_BORROW(PyImport_AddModule("__main__"));
        if (!mainModule.get()) {
            return {};
        }

        auto pyClass = Py_OWN(PyObject_GetAttrString(*mainModule, className.c_str()));
        auto args = Py_OWN(PyTuple_Pack(1, pyClass.get()));
        auto kwargs = Py_OWN(PyDict_New());
        auto pyTypeId = Py_OWN(PyUnicode_FromString(typeId.c_str()));
        if (!pyClass.get() || !args.get() || !kwargs.get() || !pyTypeId.get()) {
            return {};
        }
        db0::python::PySafeDict_SetItemString(kwargs.get(), "id", std::move(pyTypeId));
        if (immutable) {
            auto pyImmutable = Py_OWN(PyBool_FromLong(1));
            if (!pyImmutable.get()) {
                return {};
            }
            db0::python::PySafeDict_SetItemString(kwargs.get(), "immutable", std::move(pyImmutable));
        }

        return db0::python::shared_py_object<PyTypeObject *>(
            reinterpret_cast<PyTypeObject *>(db0::python::PyAPI_wrapPyClass(nullptr, args.get(), kwargs.get())),
            false
        );
    }

    db0::python::shared_py_object<PyObject *> makeUnsupportedValue()
    {
        static std::uint64_t unsupportedTypeIndex = 0;
        auto className = std::string("UnsupportedEmbeddingMeasureValue") + std::to_string(unsupportedTypeIndex);
        ++unsupportedTypeIndex;

        if (PyRun_SimpleString(("class " + className + ": pass\n").c_str()) != 0) {
            return {};
        }

        auto mainModule = Py_BORROW(PyImport_AddModule("__main__"));
        if (!mainModule.get()) {
            return {};
        }

        auto pyClass = Py_OWN(PyObject_GetAttrString(mainModule.get(), className.c_str()));
        if (!pyClass.get()) {
            return {};
        }

        return Py_OWN(PyObject_CallNoArgs(pyClass.get()));
    }

    TEST_F( EmbeddingMeasureTest, testMeasuresStringAndBytesValues )
    {
        auto pyString = Py_OWN(PyUnicode_FromString("embedded-string"));
        auto stringMeasure = tryMeasureEmbeddingValue(TypeId::STRING, StorageClass::STRING_REF, pyString.get());
        ASSERT_TRUE(stringMeasure.has_value());
        ASSERT_EQ(stringMeasure->m_storageClass, StorageClass::STRING_REF);
        ASSERT_EQ(
            stringMeasure->m_embeddedBytes,
            o_tuple_item::measure(o_tuple_item::Element::string("embedded-string"))
        );
        ASSERT_EQ(stringMeasure->m_separateStorageBytes, db0::o_string::measure("embedded-string"));
        ASSERT_FALSE(stringMeasure->m_requiresObjectView);
        ASSERT_FALSE(stringMeasure->m_requiresCollectionView);
        ASSERT_EQ(stringMeasure->m_allocationsAvoided, 1u);

        const char bytes[] = { 'a', 'b', 'c' };
        auto pyBytes = Py_OWN(PyBytes_FromStringAndSize(bytes, sizeof(bytes)));
        auto bytesMeasure = tryMeasureEmbeddingValue(TypeId::BYTES, StorageClass::DB0_BYTES, pyBytes.get());
        ASSERT_TRUE(bytesMeasure.has_value());
        ASSERT_EQ(
            bytesMeasure->m_embeddedBytes,
            o_tuple_item::measure(o_tuple_item::Element::bytes(
                reinterpret_cast<const std::byte *>(bytes), sizeof(bytes)
            ))
        );
        ASSERT_EQ(
            bytesMeasure->m_separateStorageBytes,
            db0::o_binary::measure(reinterpret_cast<const std::byte *>(bytes), sizeof(bytes))
        );
        ASSERT_EQ(bytesMeasure->m_allocationsAvoided, 1u);
    }

    TEST_F( EmbeddingMeasureTest, testStringAndBytesEmbeddingDecisionUsesCostRule )
    {
        auto smallString = Py_OWN(PyUnicode_FromString("small embedded string"));
        ASSERT_TRUE(shouldEmbedValue(TypeId::STRING, StorageClass::STRING_REF, smallString.get()));

        std::string largeString(3 * SlabAllocatorConfig::DEFAULT_PAGE_SIZE, 'x');
        auto largePyString = Py_OWN(PyUnicode_FromStringAndSize(largeString.data(), largeString.size()));
        ASSERT_FALSE(shouldEmbedValue(TypeId::STRING, StorageClass::STRING_REF, largePyString.get()));

        const char smallBytes[] = { 'a', '\0', 'b' };
        auto smallPyBytes = Py_OWN(PyBytes_FromStringAndSize(smallBytes, sizeof(smallBytes)));
        ASSERT_TRUE(shouldEmbedValue(TypeId::BYTES, StorageClass::DB0_BYTES, smallPyBytes.get()));

        std::string largeBytes(3 * SlabAllocatorConfig::DEFAULT_PAGE_SIZE, 'y');
        auto largePyBytes = Py_OWN(PyBytes_FromStringAndSize(largeBytes.data(), largeBytes.size()));
        ASSERT_FALSE(shouldEmbedValue(TypeId::BYTES, StorageClass::DB0_BYTES, largePyBytes.get()));
    }

    TEST_F( EmbeddingMeasureTest, testMeasuresPythonCollectionValues )
    {
        auto pyList = Py_OWN(PyList_New(2));
        db0::python::PySafeList_SetItem(pyList.get(), 0, Py_OWN(PyLong_FromLong(7)));
        db0::python::PySafeList_SetItem(pyList.get(), 1, Py_OWN(PyUnicode_FromString("seven")));

        auto listMeasure = tryMeasureEmbeddingValue(TypeId::LIST, StorageClass::DB0_LIST, pyList.get());
        ASSERT_TRUE(listMeasure.has_value());
        ASSERT_GT(listMeasure->m_embeddedBytes, 0u);
        ASSERT_TRUE(listMeasure->m_requiresCollectionView);
        ASSERT_EQ(listMeasure->m_allocationsAvoided, 2u);

        auto pyTuple = Py_OWN(PyTuple_New(1));
        db0::python::PySafeTuple_SetItem(pyTuple.get(), 0, Py_OWN(PyLong_FromLong(42)));
        auto tupleMeasure = tryMeasureEmbeddingValue(TypeId::TUPLE, StorageClass::DB0_TUPLE, pyTuple.get());
        ASSERT_TRUE(tupleMeasure.has_value());
        ASSERT_GT(tupleMeasure->m_embeddedBytes, 0u);
        ASSERT_EQ(tupleMeasure->m_allocationsAvoided, 1u);

        auto pySet = Py_OWN(PySet_New(nullptr));
        db0::python::PySafeSet_Add(pySet.get(), Py_OWN(PyUnicode_FromString("item")));
        auto setMeasure = tryMeasureEmbeddingValue(TypeId::SET, StorageClass::DB0_SET, pySet.get());
        ASSERT_TRUE(setMeasure.has_value());
        ASSERT_GT(setMeasure->m_embeddedBytes, 0u);
        ASSERT_EQ(setMeasure->m_allocationsAvoided, 2u);

        auto pyDict = Py_OWN(PyDict_New());
        db0::python::PySafeDict_SetItemString(pyDict.get(), "key", Py_OWN(PyLong_FromLong(99)));
        auto dictMeasure = tryMeasureEmbeddingValue(TypeId::DICT, StorageClass::DB0_DICT, pyDict.get());
        ASSERT_TRUE(dictMeasure.has_value());
        ASSERT_GT(dictMeasure->m_embeddedBytes, 0u);
        ASSERT_EQ(dictMeasure->m_allocationsAvoided, 2u);
    }

    TEST_F( EmbeddingMeasureTest, testCollectionAllocationHeuristicCountsNestedMemberStorage )
    {
        auto nestedTuple = Py_OWN(PyTuple_New(2));
        db0::python::PySafeTuple_SetItem(nestedTuple.get(), 0, Py_OWN(PyUnicode_FromString("nested")));
        db0::python::PySafeTuple_SetItem(nestedTuple.get(), 1, Py_OWN(PyBytes_FromStringAndSize("b", 1)));

        auto pyList = Py_OWN(PyList_New(2));
        db0::python::PySafeList_SetItem(pyList.get(), 0, Py_OWN(PyUnicode_FromString("root")));
        db0::python::PySafeList_SetItem(pyList.get(), 1, std::move(nestedTuple));

        auto measure = tryMeasureEmbeddingValue(TypeId::LIST, StorageClass::DB0_LIST, pyList.get());
        ASSERT_TRUE(measure.has_value());
        ASSERT_EQ(measure->m_allocationsAvoided, 5u);
    }

    TEST_F( EmbeddingMeasureTest, testCollectionAllocationHeuristicCountsLargeRoots )
    {
        constexpr std::size_t largeCount = 10000;
        auto smallTuple = Py_OWN(PyTuple_New(1));
        db0::python::PySafeTuple_SetItem(smallTuple.get(), 0, Py_OWN(PyLong_FromLong(0)));
        auto largeTuple = Py_OWN(PyTuple_New(static_cast<Py_ssize_t>(largeCount)));
        for (std::size_t i = 0; i < largeCount; ++i) {
            db0::python::PySafeTuple_SetItem(
                largeTuple.get(), static_cast<Py_ssize_t>(i), Py_OWN(PyLong_FromLong(static_cast<long>(i)))
            );
        }

        auto smallTupleMeasure = tryMeasureEmbeddingValue(TypeId::TUPLE, StorageClass::DB0_TUPLE, smallTuple.get());
        auto largeTupleMeasure = tryMeasureEmbeddingValue(TypeId::TUPLE, StorageClass::DB0_TUPLE, largeTuple.get());
        ASSERT_TRUE(smallTupleMeasure.has_value());
        ASSERT_TRUE(largeTupleMeasure.has_value());
        ASSERT_GT(largeTupleMeasure->m_allocationsAvoided, smallTupleMeasure->m_allocationsAvoided);

        auto smallList = Py_OWN(PyList_New(1));
        db0::python::PySafeList_SetItem(smallList.get(), 0, Py_OWN(PyLong_FromLong(0)));
        auto largeList = Py_OWN(PyList_New(static_cast<Py_ssize_t>(largeCount)));
        for (std::size_t i = 0; i < largeCount; ++i) {
            db0::python::PySafeList_SetItem(
                largeList.get(), static_cast<Py_ssize_t>(i), Py_OWN(PyLong_FromLong(static_cast<long>(i)))
            );
        }

        auto smallListMeasure = tryMeasureEmbeddingValue(TypeId::LIST, StorageClass::DB0_LIST, smallList.get());
        auto largeListMeasure = tryMeasureEmbeddingValue(TypeId::LIST, StorageClass::DB0_LIST, largeList.get());
        ASSERT_TRUE(smallListMeasure.has_value());
        ASSERT_TRUE(largeListMeasure.has_value());
        ASSERT_GT(largeListMeasure->m_allocationsAvoided, smallListMeasure->m_allocationsAvoided);
    }

    TEST_F( EmbeddingMeasureTest, testUnsupportedCollectionElementsThrow )
    {
        auto pyList = Py_OWN(PyList_New(1));
        auto unsupportedListValue = makeUnsupportedValue();
        ASSERT_TRUE(unsupportedListValue.get());
        db0::python::PySafeList_SetItem(pyList.get(), 0, std::move(unsupportedListValue));
        ASSERT_THROW(
            tryMeasureEmbeddingValue(TypeId::LIST, StorageClass::DB0_LIST, pyList.get()),
            db0::InputException
        );

        auto pyTuple = Py_OWN(PyTuple_New(1));
        auto unsupportedTupleValue = makeUnsupportedValue();
        ASSERT_TRUE(unsupportedTupleValue.get());
        db0::python::PySafeTuple_SetItem(pyTuple.get(), 0, std::move(unsupportedTupleValue));
        ASSERT_THROW(
            tryMeasureEmbeddingValue(TypeId::TUPLE, StorageClass::DB0_TUPLE, pyTuple.get()),
            db0::InputException
        );

        auto pySet = Py_OWN(PySet_New(nullptr));
        auto unsupportedSetValue = makeUnsupportedValue();
        ASSERT_TRUE(unsupportedSetValue.get());
        db0::python::PySafeSet_Add(pySet.get(), std::move(unsupportedSetValue));
        ASSERT_THROW(
            tryMeasureEmbeddingValue(TypeId::SET, StorageClass::DB0_SET, pySet.get()),
            db0::InputException
        );
    }

    TEST_F( EmbeddingMeasureTest, testUnsupportedDictElementsThrow )
    {
        auto dictWithUnsupportedKey = Py_OWN(PyDict_New());
        auto unsupportedKey = makeUnsupportedValue();
        ASSERT_TRUE(unsupportedKey.get());
        db0::python::PySafeDict_SetItem(
            dictWithUnsupportedKey.get(), std::move(unsupportedKey), Py_OWN(PyLong_FromLong(3))
        );
        ASSERT_THROW(
            tryMeasureEmbeddingValue(TypeId::DICT, StorageClass::DB0_DICT, dictWithUnsupportedKey.get()),
            db0::InputException
        );

        auto dictWithUnsupportedValue = Py_OWN(PyDict_New());
        auto unsupportedValue = makeUnsupportedValue();
        ASSERT_TRUE(unsupportedValue.get());
        db0::python::PySafeDict_SetItemString(
            dictWithUnsupportedValue.get(), "key", std::move(unsupportedValue)
        );
        ASSERT_THROW(
            tryMeasureEmbeddingValue(TypeId::DICT, StorageClass::DB0_DICT, dictWithUnsupportedValue.get()),
            db0::InputException
        );
    }

    TEST_F( EmbeddingMeasureTest, testUnsupportedNestedCollectionElementThrows )
    {
        auto nestedList = Py_OWN(PyList_New(1));
        auto unsupportedValue = makeUnsupportedValue();
        ASSERT_TRUE(unsupportedValue.get());
        db0::python::PySafeList_SetItem(nestedList.get(), 0, std::move(unsupportedValue));

        auto pyList = Py_OWN(PyList_New(2));
        db0::python::PySafeList_SetItem(pyList.get(), 0, Py_OWN(PyUnicode_FromString("ok")));
        db0::python::PySafeList_SetItem(pyList.get(), 1, std::move(nestedList));

        ASSERT_THROW(
            tryMeasureEmbeddingValue(TypeId::LIST, StorageClass::DB0_LIST, pyList.get()),
            db0::InputException
        );
    }

    TEST_F( EmbeddingMeasureTest, testUnsupportedScalarValueReturnsNullopt )
    {
        auto pyInt = Py_OWN(PyLong_FromLong(7));
        ASSERT_FALSE(tryMeasureEmbeddingValue(TypeId::INTEGER, StorageClass::INT64, pyInt.get()).has_value());
    }

    TEST_F( EmbeddingMeasureTest, testDeferredImmutableMemoObjectMeasuresEmbeddedObjectOnly )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefixName);
        auto mockClass = getTestClass(fixture);
        auto pyMemoType = makeMemoType(true);
        ASSERT_TRUE(pyMemoType.get());

        auto pyMemo = Py_OWN(reinterpret_cast<db0::python::MemoImmutableObject *>(
            db0::python::MemoObjectStub_new(pyMemoType.get())
        ));
        pyMemo->makeNew(mockClass);
        auto &object = pyMemo->ext();
        auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(object));
        ASSERT_NE(initializer, nullptr);
        initializer->set({0, 0}, StorageClass::INT64, Value(17));

        auto measure = tryMeasureEmbeddingValue(
            TypeId::MEMO_IMMUTABLE_OBJECT, StorageClass::OBJECT_REF,
            reinterpret_cast<LangConfig::ObjectPtr>(pyMemo.get())
        );
        ASSERT_TRUE(measure.has_value());
        ASSERT_EQ(measure->m_embeddedBytes, o_embedded_object::measure(mockClass->getClassRef(), *initializer));
        ASSERT_TRUE(measure->m_requiresObjectView);
        ASSERT_FALSE(measure->m_requiresCollectionView);

        workspace.close();
    }

    TEST_F( EmbeddingMeasureTest, testMaterializedMemoObjectReturnsNullopt )
    {
        Workspace workspace("", {}, {}, {}, {}, db0::object_model::initializer());
        auto fixture = workspace.getFixture(prefixName);
        auto mockClass = getTestClass(fixture);
        auto pyMemoType = makeMemoType(false);
        ASSERT_TRUE(pyMemoType.get());

        auto pyMemo = Py_OWN(db0::python::MemoObjectStub_new(pyMemoType.get()));
        pyMemo->makeNew(mockClass);
        {
            FixtureLock lock(fixture);
            pyMemo->modifyExt().postInit(lock);
        }

        auto measure = tryMeasureEmbeddingValue(
            TypeId::MEMO_OBJECT, StorageClass::OBJECT_REF, reinterpret_cast<LangConfig::ObjectPtr>(pyMemo.get())
        );
        ASSERT_FALSE(measure.has_value());
        // This synthetic wrapper is only needed to prove the measurement contract.
        // Avoid unrelated deallocation paths after the durable object is created.
        pyMemo.steal();

        workspace.close();
    }

}
