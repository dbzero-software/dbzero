// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <Python.h>
#include <datetime.h>
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/PyAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/types/PyDecimal.hpp>
#include <utils/SubClass.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/core/serialization/bounded_buf_t.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/dict/o_dict.hpp>
#include <dbzero/object_model/set/o_set.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <utils/ScopedWorkspaceFixture.hpp>

#include <limits>
#include <stdexcept>

namespace tests
{

    using namespace db0;
    using namespace db0::object_model;
    using namespace db0::python;

    class EmbeddedTupleTest: public MemspaceTestBase
    {
    };

    static std::int64_t asInt64(const o_tuple_item &item)
    {
        if (item.itemKind() == StorageClass::PACKED_INT32) {
            return static_cast<std::int64_t>(item.packedIntPayload().value());
        }
        return item.intPayload().value();
    }

    static bool asBool(const o_tuple_item &item)
    {
        return item.boolPayload().value();
    }

    static double asDouble(const o_tuple_item &item)
    {
        return item.doublePayload().value();
    }

    static std::uint64_t asUint64(const o_tuple_item &item)
    {
        return item.uint64Payload().value();
    }

    static std::string asString(const o_tuple_item &item)
    {
        return item.stringPayload().toString();
    }

    static std::vector<std::byte> asBytes(const o_tuple_item &item)
    {
        const auto &payload = item.bytesPayload();
        return { payload.begin(), payload.end() };
    }

    static void throwDecodeError()
    {
        throw std::runtime_error("decode error");
    }

    static std::size_t measureElementBlock(const std::vector<o_tuple<>::Element> &elements)
    {
        std::size_t size = 0;
        for (const auto &element: elements) {
            size += o_tuple_item::measure(element);
        }
        return size;
    }

    static db0::python::shared_py_object<PyTypeObject *> makeMemoType()
    {
        static std::uint64_t memoTypeIndex = 0;
        auto className = std::string("EmbeddedTupleNestedImmutable") + std::to_string(memoTypeIndex);
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

    TEST_F( EmbeddedTupleTest , testTupleStoresInlineAndVariableLengthElements )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x01}, std::byte{0x02}, std::byte{0xff} };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(42),
            o_tuple<>::Element::string("alpha"),
            o_tuple<>::Element::boolean(true),
            o_tuple<>::Element::bytes(bytes)
        };

        v_object<o_tuple<> > tuple(memspace, elements);
        auto expectedElementsSize = measureElementBlock(elements);

        ASSERT_EQ(o_tuple<>::measure(elements),
            o_tuple<>::safeSizeOf(reinterpret_cast<const std::byte *>(tuple.getData())));
        ASSERT_EQ(tuple->size(), 4u);
        ASSERT_EQ(tuple->elementsByteSize(), expectedElementsSize);
        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::PACKED_INT32);
        ASSERT_EQ(asInt64(tuple->item(0)), 42);
        ASSERT_EQ(tuple->item(1).itemKind(), StorageClass::EMBEDDED_STRING);
        ASSERT_EQ(asString(tuple->item(1)), "alpha");
        ASSERT_EQ(tuple->item(2).itemKind(), StorageClass::BOOLEAN);
        ASSERT_TRUE(asBool(tuple->item(2)));
        ASSERT_EQ(tuple->item(3).itemKind(), StorageClass::EMBEDDED_BYTES);
        ASSERT_EQ(asBytes(tuple->item(3)), (std::vector<std::byte>{ std::byte{0x01}, std::byte{0x02}, std::byte{0xff} }));
    }

    TEST_F( EmbeddedTupleTest , testTupleItemStorageClassValuesAreStable )
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
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::DB0_BYTES), 23u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::BOOLEAN), 28u);
        ASSERT_EQ(static_cast<std::uint8_t>(StorageClass::PACKED_INT32), 253u);
    }

    TEST_F( EmbeddedTupleTest , testTupleUsesPackedInt32KindOnlyWhenItSavesAtLeastTwoBytes )
    {
        auto memspace = getMemspace();
        constexpr std::int64_t maxPackedInt32 = std::numeric_limits<std::uint32_t>::max();
        constexpr std::int64_t firstInt64AfterPacked = static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max()) + 1;
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(0),
            o_tuple<>::Element::integer(127),
            o_tuple<>::Element::integer(maxPackedInt32),
            o_tuple<>::Element::integer(firstInt64AfterPacked),
            o_tuple<>::Element::integer(-1)
        };

        v_object<o_tuple<> > tuple(memspace, elements);

        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::PACKED_INT32);
        ASSERT_EQ(asInt64(tuple->item(0)), 0);
        ASSERT_EQ(tuple->item(1).itemKind(), StorageClass::PACKED_INT32);
        ASSERT_EQ(asInt64(tuple->item(1)), 127);
        ASSERT_EQ(tuple->item(2).itemKind(), StorageClass::PACKED_INT32);
        ASSERT_EQ(asInt64(tuple->item(2)), maxPackedInt32);
        ASSERT_EQ(tuple->item(3).itemKind(), StorageClass::INT64);
        ASSERT_EQ(asInt64(tuple->item(3)), firstInt64AfterPacked);
        ASSERT_EQ(tuple->item(4).itemKind(), StorageClass::INT64);
        ASSERT_EQ(asInt64(tuple->item(4)), -1);

        ASSERT_EQ(tuple->item(0).sizeOf(), o_tuple_item::measure(elements[0]));
        ASSERT_EQ(tuple->item(3).sizeOf(), o_tuple_item::measure(elements[3]));
        ASSERT_LE(tuple->item(2).sizeOf() + 2, tuple->item(3).sizeOf());
    }

    TEST_F( EmbeddedTupleTest , testTupleStoresUint64ScalarKinds )
    {
        auto memspace = getMemspace();
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::timestamp(1001),
            o_tuple<>::Element::date(2002),
            o_tuple<>::Element::datetime(3003),
            o_tuple<>::Element::datetimeTz(4004),
            o_tuple<>::Element::time(5005),
            o_tuple<>::Element::timeTz(6006),
            o_tuple<>::Element::decimal(7007)
        };

        v_object<o_tuple<> > tuple(memspace, elements);

        ASSERT_EQ(tuple->size(), elements.size());
        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::PTIME64);
        ASSERT_EQ(asUint64(tuple->item(0)), 1001u);
        ASSERT_EQ(tuple->item(1).itemKind(), StorageClass::DATE);
        ASSERT_EQ(asUint64(tuple->item(1)), 2002u);
        ASSERT_EQ(tuple->item(2).itemKind(), StorageClass::DATETIME);
        ASSERT_EQ(asUint64(tuple->item(2)), 3003u);
        ASSERT_EQ(tuple->item(3).itemKind(), StorageClass::DATETIME_TZ);
        ASSERT_EQ(asUint64(tuple->item(3)), 4004u);
        ASSERT_EQ(tuple->item(4).itemKind(), StorageClass::TIME);
        ASSERT_EQ(asUint64(tuple->item(4)), 5005u);
        ASSERT_EQ(tuple->item(5).itemKind(), StorageClass::TIME_TZ);
        ASSERT_EQ(asUint64(tuple->item(5)), 6006u);
        ASSERT_EQ(tuple->item(6).itemKind(), StorageClass::DECIMAL);
        ASSERT_EQ(asUint64(tuple->item(6)), 7007u);
    }

    TEST_F( EmbeddedTupleTest , testTupleMeasureSizeOfAndSafeSizeOfWithMultipleVariableLengthElements )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> shortBytes = { std::byte{0x10}, std::byte{0x20} };
        const std::vector<std::byte> longBytes = {
            std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04},
            std::byte{0x05}, std::byte{0x06}, std::byte{0x07}, std::byte{0x08}
        };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::string(""),
            o_tuple<>::Element::bytes(shortBytes),
            o_tuple<>::Element::string("alpha"),
            o_tuple<>::Element::bytes(longBytes),
            o_tuple<>::Element::string("variable length string payload"),
            o_tuple<>::Element::integer(9001),
            o_tuple<>::Element::string("tail")
        };

        v_object<o_tuple<> > tuple(memspace, elements);
        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto expectedElementBytes = measureElementBlock(elements);
        auto expectedTotalBytes = o_tuple<>::measure(elements);

        ASSERT_EQ(tuple->size(), elements.size());
        ASSERT_EQ(tuple->elementsByteSize(), expectedElementBytes);
        ASSERT_EQ(tuple->sizeOf(), expectedTotalBytes);
        ASSERT_EQ(o_tuple<>::safeSizeOf(begin), expectedTotalBytes);
        ASSERT_EQ(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + expectedTotalBytes)),
            expectedTotalBytes);

        ASSERT_EQ(asString(tuple->item(0)), "");
        ASSERT_EQ(asBytes(tuple->item(1)), shortBytes);
        ASSERT_EQ(asString(tuple->item(2)), "alpha");
        ASSERT_EQ(asBytes(tuple->item(3)), longBytes);
        ASSERT_EQ(asString(tuple->item(4)), "variable length string payload");
        ASSERT_EQ(asInt64(tuple->item(5)), 9001);
        ASSERT_EQ(asString(tuple->item(6)), "tail");
    }

    TEST_F( EmbeddedTupleTest , testCompactTupleOmitsElementsByteSizeMember )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x10}, std::byte{0x20}, std::byte{0x30} };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(11),
            o_tuple<>::Element::string("compact"),
            o_tuple<>::Element::bytes(bytes)
        };

        v_object<o_tuple<> > tuple(memspace, elements);
        v_object<o_compact_tuple> compactTuple(memspace, elements);

        ASSERT_EQ(compactTuple->size(), elements.size());
        ASSERT_EQ(compactTuple->elementsByteSize(), measureElementBlock(elements));
        ASSERT_EQ(compactTuple->sizeOf(), o_compact_tuple::measure(elements));
        ASSERT_EQ(o_compact_tuple::safeSizeOf(reinterpret_cast<const std::byte *>(compactTuple.getData())),
            compactTuple->sizeOf());
        ASSERT_LT(compactTuple->sizeOf(), tuple->sizeOf());
        ASSERT_EQ(asInt64(compactTuple->item(0)), 11);
        ASSERT_EQ(asString(compactTuple->item(1)), "compact");
        ASSERT_EQ(asBytes(compactTuple->item(2)), bytes);
    }

    TEST_F( EmbeddedTupleTest , testSafeSizeOfRejectsTruncatedMultipleVariableLengthTuple )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = {
            std::byte{0xaa}, std::byte{0xbb}, std::byte{0xcc}, std::byte{0xdd}, std::byte{0xee}
        };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::string("first variable payload"),
            o_tuple<>::Element::bytes(bytes),
            o_tuple<>::Element::string("second variable payload"),
            o_tuple<>::Element::bytes(bytes),
            o_tuple<>::Element::string("third variable payload")
        };
        v_object<o_tuple<> > tuple(memspace, elements);

        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto size = tuple->sizeOf();

        ASSERT_EQ(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size)), size);
        for (std::size_t truncatedSize = 0; truncatedSize < size; ++truncatedSize) {
            ASSERT_THROW(
                o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + truncatedSize)),
                std::runtime_error
            ) << "truncated size: " << truncatedSize;
        }
    }

    TEST_F( EmbeddedTupleTest , testTupleSurvivesReopen )
    {
        auto memspace = getMemspace();
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::none(),
            o_tuple<>::Element::string("variable length")
        };

        Address address;
        {
            v_object<o_tuple<> > tuple(memspace, elements);
            address = tuple.getAddress();
        }

        v_object<o_tuple<> > tuple(memspace.myPtr(address));

        ASSERT_EQ(tuple->sizeOf(), o_tuple<>::safeSizeOf(reinterpret_cast<const std::byte *>(tuple.getData())));
        ASSERT_EQ(tuple->size(), 2u);
        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::NONE);
        ASSERT_EQ(asString(tuple->item(1)), "variable length");
    }

    TEST_F( EmbeddedTupleTest , testSafeSizeOfValidatesBoundedBufferBeforeHeaderReads )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x01}, std::byte{0x02}, std::byte{0xff} };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(42),
            o_tuple<>::Element::bytes(bytes)
        };
        v_object<o_tuple<> > tuple(memspace, elements);

        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto size = tuple->sizeOf();

        ASSERT_EQ(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size)), size);
        ASSERT_THROW(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + 1)), std::runtime_error);
        ASSERT_THROW(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size - 1)), std::runtime_error);
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsFromPythonTuple )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pyTuple = Py_OWN(PyTuple_New(5));
        PySafeTuple_SetItem(*pyTuple, 0, Py_OWN(PyLong_FromLongLong(123)));
        PySafeTuple_SetItem(*pyTuple, 1, Py_OWN(PyUnicode_FromString("python")));
        PySafeTuple_SetItem(*pyTuple, 2, Py_OWN(PyBool_FromLong(1)));
        PySafeTuple_SetItem(*pyTuple, 3, Py_OWN(PyFloat_FromDouble(4.5)));
        PySafeTuple_SetItem(*pyTuple, 4, Py_OWN(PyBytes_FromStringAndSize("\x01\x02", 2)));

        v_object<o_py_tuple> tuple(memspace, *pyTuple);

        ASSERT_EQ(o_py_tuple::measure(*pyTuple), tuple->sizeOf());
        ASSERT_EQ(tuple->size(), 5u);
        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::PACKED_INT32);
        ASSERT_EQ(asInt64(tuple->item(0)), 123);
        ASSERT_EQ(tuple->item(1).itemKind(), StorageClass::EMBEDDED_STRING);
        ASSERT_EQ(asString(tuple->item(1)), "python");
        ASSERT_EQ(tuple->item(2).itemKind(), StorageClass::BOOLEAN);
        ASSERT_TRUE(asBool(tuple->item(2)));
        ASSERT_EQ(tuple->item(3).itemKind(), StorageClass::FP_NUMERIC64);
        ASSERT_EQ(asDouble(tuple->item(3)), 4.5);
        ASSERT_EQ(tuple->item(4).itemKind(), StorageClass::EMBEDDED_BYTES);
        ASSERT_EQ(asBytes(tuple->item(4)), (std::vector<std::byte>{ std::byte{0x01}, std::byte{0x02} }));
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsFromPythonList )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pyList = Py_OWN(PyList_New(2));
        PySafeList_SetItem(*pyList, 0, Py_OWN(Py_NewRef(Py_None)));
        PySafeList_SetItem(*pyList, 1, Py_OWN(PyUnicode_FromString("list item")));

        v_object<o_py_tuple> tuple(memspace, *pyList);

        ASSERT_EQ(o_py_tuple::measure(*pyList), tuple->sizeOf());
        ASSERT_EQ(tuple->size(), 2u);
        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::NONE);
        ASSERT_EQ(tuple->item(1).itemKind(), StorageClass::EMBEDDED_STRING);
        ASSERT_EQ(asString(tuple->item(1)), "list item");
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsFromImmutableMemoElement )
    {
        Py_Initialize();

        ScopedWorkspaceFixture workspace_fixture("embedded-tuple-nested-memo");
        auto fixture = workspace_fixture.fixture();
        auto nestedClass = getTestClass(fixture);
        auto pyMemoType = makeMemoType();
        ASSERT_TRUE(pyMemoType.get());

        auto pyMemo = Py_OWN(reinterpret_cast<db0::python::MemoImmutableObject *>(
            db0::python::MemoObjectStub_new(pyMemoType.get())
        ));
        pyMemo->makeNew(nestedClass);
        auto *nestedInitializer = dynamic_cast<ImmutableObjectInitializer *>(
            InitManager::instance.findInitializer(pyMemo->ext())
        );
        ASSERT_NE(nestedInitializer, nullptr);
        nestedInitializer->set({0, 0}, StorageClass::INT64, Value(23));

        auto pyTuple = Py_OWN(PyTuple_New(1));
        PySafeTuple_SetItem(*pyTuple, 0, Py_OWN(Py_NewRef(reinterpret_cast<PyObject *>(pyMemo.get()))));

        auto memspace = getMemspace();
        v_object<o_py_tuple> tuple(memspace, *pyTuple);

        ASSERT_EQ(tuple->size(), 1u);
        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::EMBEDDED_OBJECT);

        const auto &nestedObject = o_embedded_object::__const_ref(tuple->item(0).embeddedPayload().begin());
        ASSERT_EQ(nestedObject.getClassRef(), nestedClass->getClassRef());
        auto fixedValue = nestedObject.fixedValue(0);
        ASSERT_TRUE(fixedValue.has_value());
        ASSERT_EQ(fixedValue->m_kind, StorageClass::INT64);
        ASSERT_EQ(fixedValue->m_value, 23u);

        workspace_fixture.close();
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsDeeplyNestedCollections )
    {
        Py_Initialize();
        auto memspace = getMemspace();

        auto pyRoot = Py_OWN(PyTuple_New(3));

        auto pyNestedList = Py_OWN(PyList_New(2));
        PySafeList_SetItem(*pyNestedList, 0, Py_OWN(PyLong_FromLongLong(11)));

        auto pyDeepDict = Py_OWN(PyDict_New());
        ASSERT_EQ(PySafeDict_SetItem(
            *pyDeepDict, Py_OWN(PyUnicode_FromString("answer")), Py_OWN(PyLong_FromLongLong(42))
        ), 0);

        auto pyDeepList = Py_OWN(PyList_New(2));
        PySafeList_SetItem(*pyDeepList, 0, Py_OWN(PyUnicode_FromString("deep")));
        PySafeList_SetItem(*pyDeepList, 1, Py_OWN(Py_NewRef(*pyDeepDict)));

        auto pyInnerTuple = Py_OWN(PyTuple_New(2));
        PySafeTuple_SetItem(*pyInnerTuple, 0, Py_OWN(PyLong_FromLongLong(22)));
        PySafeTuple_SetItem(*pyInnerTuple, 1, Py_OWN(Py_NewRef(*pyDeepList)));

        auto pyInnerDict = Py_OWN(PyDict_New());
        ASSERT_EQ(PySafeDict_SetItem(
            *pyInnerDict, Py_OWN(PyUnicode_FromString("tuple")), Py_OWN(Py_NewRef(*pyInnerTuple))
        ), 0);
        PySafeList_SetItem(*pyNestedList, 1, Py_OWN(Py_NewRef(*pyInnerDict)));
        PySafeTuple_SetItem(*pyRoot, 0, Py_OWN(Py_NewRef(*pyNestedList)));

        auto pyNumbers = Py_OWN(PyList_New(2));
        PySafeList_SetItem(*pyNumbers, 0, Py_OWN(PyLong_FromLongLong(3)));
        PySafeList_SetItem(*pyNumbers, 1, Py_OWN(PyLong_FromLongLong(4)));

        auto pyFlags = Py_OWN(PySet_New(nullptr));
        ASSERT_EQ(PySafeSet_Add(*pyFlags, Py_OWN(Py_NewRef(Py_True))), 0);
        ASSERT_EQ(PySafeSet_Add(*pyFlags, Py_OWN(PyUnicode_FromString("ok"))), 0);

        auto pyRootDict = Py_OWN(PyDict_New());
        ASSERT_EQ(PySafeDict_SetItem(
            *pyRootDict, Py_OWN(PyUnicode_FromString("numbers")), Py_OWN(Py_NewRef(*pyNumbers))
        ), 0);
        ASSERT_EQ(PySafeDict_SetItem(
            *pyRootDict, Py_OWN(PyUnicode_FromString("flags")), Py_OWN(Py_NewRef(*pyFlags))
        ), 0);
        PySafeTuple_SetItem(*pyRoot, 1, Py_OWN(Py_NewRef(*pyRootDict)));

        auto pyRootSet = Py_OWN(PySet_New(nullptr));
        ASSERT_EQ(PySafeSet_Add(*pyRootSet, Py_OWN(PyUnicode_FromString("root-set"))), 0);
        ASSERT_EQ(PySafeSet_Add(*pyRootSet, Py_OWN(PyLong_FromLongLong(99))), 0);
        auto pySetTuple = Py_OWN(PyTuple_New(2));
        PySafeTuple_SetItem(*pySetTuple, 0, Py_OWN(PyUnicode_FromString("set-tuple")));
        PySafeTuple_SetItem(*pySetTuple, 1, Py_OWN(PyLong_FromLongLong(123)));
        ASSERT_EQ(PySafeSet_Add(*pyRootSet, Py_OWN(Py_NewRef(*pySetTuple))), 0);
        PySafeTuple_SetItem(*pyRoot, 2, Py_OWN(Py_NewRef(*pyRootSet)));

        v_object<o_py_tuple> tuple(memspace, *pyRoot);

        ASSERT_EQ(o_py_tuple::measure(*pyRoot), tuple->sizeOf());
        ASSERT_EQ(tuple->size(), 3u);

        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::EMBEDDED_TUPLE);
        const auto &nestedList = o_tuple<>::__const_ref(tuple->item(0).embeddedPayload().begin());
        ASSERT_EQ(nestedList.size(), 2u);
        ASSERT_EQ(asInt64(nestedList.item(0)), 11);
        ASSERT_EQ(nestedList.item(1).itemKind(), StorageClass::EMBEDDED_DICT);

        const auto &innerDict = o_dict::__const_ref(nestedList.item(1).embeddedPayload().begin());
        auto *innerTupleItem = innerDict.get(o_dict::Element::string("tuple"));
        ASSERT_NE(innerTupleItem, nullptr);
        ASSERT_EQ(innerTupleItem->itemKind(), StorageClass::EMBEDDED_TUPLE);

        const auto &innerTuple = o_tuple<>::__const_ref(innerTupleItem->embeddedPayload().begin());
        ASSERT_EQ(innerTuple.size(), 2u);
        ASSERT_EQ(asInt64(innerTuple.item(0)), 22);
        ASSERT_EQ(innerTuple.item(1).itemKind(), StorageClass::EMBEDDED_TUPLE);

        const auto &deepList = o_tuple<>::__const_ref(innerTuple.item(1).embeddedPayload().begin());
        ASSERT_EQ(deepList.size(), 2u);
        ASSERT_EQ(asString(deepList.item(0)), "deep");
        ASSERT_EQ(deepList.item(1).itemKind(), StorageClass::EMBEDDED_DICT);

        const auto &deepDict = o_dict::__const_ref(deepList.item(1).embeddedPayload().begin());
        auto *answer = deepDict.get(o_dict::Element::string("answer"));
        ASSERT_NE(answer, nullptr);
        ASSERT_EQ(asInt64(*answer), 42);

        ASSERT_EQ(tuple->item(1).itemKind(), StorageClass::EMBEDDED_DICT);
        const auto &rootDict = o_dict::__const_ref(tuple->item(1).embeddedPayload().begin());
        auto *numbersItem = rootDict.get(o_dict::Element::string("numbers"));
        ASSERT_NE(numbersItem, nullptr);
        ASSERT_EQ(numbersItem->itemKind(), StorageClass::EMBEDDED_TUPLE);
        const auto &numbers = o_tuple<>::__const_ref(numbersItem->embeddedPayload().begin());
        ASSERT_EQ(numbers.size(), 2u);
        ASSERT_EQ(asInt64(numbers.item(0)), 3);
        ASSERT_EQ(asInt64(numbers.item(1)), 4);

        auto *flagsItem = rootDict.get(o_dict::Element::string("flags"));
        ASSERT_NE(flagsItem, nullptr);
        ASSERT_EQ(flagsItem->itemKind(), StorageClass::EMBEDDED_SET);
        const auto &flags = o_set::__const_ref(flagsItem->embeddedPayload().begin());
        ASSERT_EQ(flags.size(), 2u);
        ASSERT_TRUE(flags.contains(o_set::Element::boolean(true)));
        ASSERT_TRUE(flags.contains(o_set::Element::string("ok")));

        ASSERT_EQ(tuple->item(2).itemKind(), StorageClass::EMBEDDED_SET);
        const auto &rootSet = o_set::__const_ref(tuple->item(2).embeddedPayload().begin());
        ASSERT_EQ(rootSet.size(), 3u);
        ASSERT_TRUE(rootSet.contains(o_set::Element::string("root-set")));
        ASSERT_TRUE(rootSet.contains(o_set::Element::integer(99)));

        const o_tuple_item *setTupleItem = nullptr;
        for (auto it = rootSet.begin(); it != rootSet.end(); ++it) {
            if (it->itemKind() == StorageClass::EMBEDDED_TUPLE) {
                setTupleItem = &*it;
                break;
            }
        }
        ASSERT_NE(setTupleItem, nullptr);
        const auto &setTuple = o_tuple<>::__const_ref(setTupleItem->embeddedPayload().begin());
        ASSERT_EQ(setTuple.size(), 2u);
        ASSERT_EQ(asString(setTuple.item(0)), "set-tuple");
        ASSERT_EQ(asInt64(setTuple.item(1)), 123);
    }

    TEST_F( EmbeddedTupleTest , testPyTupleMeasureSizeOfAndSafeSizeOfWithMultipleVariableLengthElements )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pyTuple = Py_OWN(PyTuple_New(6));
        PySafeTuple_SetItem(*pyTuple, 0, Py_OWN(PyUnicode_FromString("")));
        PySafeTuple_SetItem(*pyTuple, 1, Py_OWN(PyBytes_FromStringAndSize("abc", 3)));
        PySafeTuple_SetItem(*pyTuple, 2, Py_OWN(PyUnicode_FromString("middle variable string")));
        PySafeTuple_SetItem(*pyTuple, 3, Py_OWN(PyBytes_FromStringAndSize("0123456789", 10)));
        PySafeTuple_SetItem(*pyTuple, 4, Py_OWN(PyUnicode_FromString("tail variable string")));
        PySafeTuple_SetItem(*pyTuple, 5, Py_OWN(PyLong_FromLongLong(77)));

        v_object<o_py_tuple> tuple(memspace, *pyTuple);
        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto measured = o_py_tuple::measure(*pyTuple);

        ASSERT_EQ(tuple->size(), 6u);
        ASSERT_EQ(tuple->sizeOf(), measured);
        ASSERT_EQ(o_py_tuple::safeSizeOf(begin), measured);
        ASSERT_EQ(o_py_tuple::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + measured)), measured);
        ASSERT_EQ(asString(tuple->item(0)), "");
        ASSERT_EQ(asBytes(tuple->item(1)), (std::vector<std::byte>{ std::byte{'a'}, std::byte{'b'}, std::byte{'c'} }));
        ASSERT_EQ(asString(tuple->item(2)), "middle variable string");
        ASSERT_EQ(asBytes(tuple->item(3)), (std::vector<std::byte>{
            std::byte{'0'}, std::byte{'1'}, std::byte{'2'}, std::byte{'3'}, std::byte{'4'},
            std::byte{'5'}, std::byte{'6'}, std::byte{'7'}, std::byte{'8'}, std::byte{'9'}
        }));
        ASSERT_EQ(asString(tuple->item(4)), "tail variable string");
        ASSERT_EQ(asInt64(tuple->item(5)), 77);
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsFromDateTimeAndDecimal )
    {
        Py_Initialize();
        db0::python::init_datetime();
        if (!PyDateTimeAPI) {
            PyDateTime_IMPORT;
        }
        ASSERT_NE(PyDateTimeAPI, nullptr);
        auto memspace = getMemspace();
        auto pyTuple = Py_OWN(PyTuple_New(5));
        auto decimal = Py_OWN(PyObject_CallFunction(db0::python::getDecimalClass(), "s", "123.45"));
        ASSERT_NE(decimal.get(), nullptr);
        PySafeTuple_SetItem(*pyTuple, 0, Py_OWN(PyDate_FromDate(2026, 5, 19)));
        PySafeTuple_SetItem(*pyTuple, 1, Py_OWN(PyDateTime_FromDateAndTime(2026, 5, 19, 12, 34, 56, 789)));
        PySafeTuple_SetItem(*pyTuple, 2, Py_OWN(PyTime_FromTime(12, 34, 56, 789)));
        PySafeTuple_SetItem(*pyTuple, 3, Py_OWN(Py_NewRef(*decimal)));
        PySafeTuple_SetItem(*pyTuple, 4, Py_OWN(PyUnicode_FromString("tail")));

        v_object<o_py_tuple> tuple(memspace, *pyTuple);

        ASSERT_EQ(tuple->item(0).itemKind(), StorageClass::DATE);
        ASSERT_EQ(asUint64(tuple->item(0)), db0::python::pyDateToUint64(PyTuple_GET_ITEM(*pyTuple, 0)));
        ASSERT_EQ(tuple->item(1).itemKind(), StorageClass::DATETIME);
        ASSERT_EQ(asUint64(tuple->item(1)), db0::python::pyDateTimeToToUint64(PyTuple_GET_ITEM(*pyTuple, 1)));
        ASSERT_EQ(tuple->item(2).itemKind(), StorageClass::TIME);
        ASSERT_EQ(asUint64(tuple->item(2)), db0::python::pyTimeToUint64(PyTuple_GET_ITEM(*pyTuple, 2)));
        ASSERT_EQ(tuple->item(3).itemKind(), StorageClass::DECIMAL);
        ASSERT_EQ(asUint64(tuple->item(3)), db0::python::pyDecimalToUint64(PyTuple_GET_ITEM(*pyTuple, 3)));
        ASSERT_EQ(tuple->item(4).itemKind(), StorageClass::EMBEDDED_STRING);
        ASSERT_EQ(asString(tuple->item(4)), "tail");
    }

}
