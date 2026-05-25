// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/ContentIndex.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <utils/SubClass.hpp>

namespace tests
{

    using namespace db0;
    using namespace db0::object_model;

    class ContentIndexTest: public testing::Test
    {
    protected:
        ContentIndexTest()
            : m_workspace("", {}, {}, {}, {}, db0::object_model::initializer())
        {
        }

        void SetUp() override
        {
            m_fixture = m_workspace.getFixture("content-index-test");
        }

        void TearDown() override
        {
            m_workspace.close();
        }

        std::shared_ptr<Class> makeClass(const char *name)
        {
            static std::uint64_t typeIndex = 0;
            auto typeId = std::string("tests/content-index/") + name + "/" + std::to_string(typeIndex++);
            ClassFlags flags;
            flags.set(ClassOptions::IMMUTABLE, true);
            return std::shared_ptr<Class>(new SubClass(
                m_fixture, name, std::nullopt, typeId.c_str(), "test_prefix", {}, flags, nullptr
            ));
        }

        std::unique_ptr<ObjectImmutableImpl> makeObject(const std::shared_ptr<Class> &type, std::int64_t value)
        {
            auto object = std::make_unique<ObjectImmutableImpl>(type);
            setInitializerValue(*object, type, value);
            {
                db0::FixtureLock lock(m_fixture);
                object->postInit(lock);
            }
            object->incRef(false);
            return object;
        }

        static ImmutableObjectInitializer &setInitializerValue(
            ObjectImmutableImpl &object, const std::shared_ptr<Class> &type, std::int64_t value
        )
        {
            auto memberLoc = type->findField("value");
            if (!memberLoc.first) {
                memberLoc.first = type->addField("value", 0);
            }
            auto field = memberLoc.first.get(0).getIndexAndOffset();
            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(object)
            );
            if (!initializer) {
                THROWF(db0::InternalException) << "Immutable initializer not found" << THROWF_END;
            }
            initializer->set(field, StorageClass::INT64, Value(value));
            return *initializer;
        }

        Workspace m_workspace;
        db0::swine_ptr<Fixture> m_fixture;
    };

    TEST_F(ContentIndexTest, testLookupFindsInsertedEquivalentInitializer)
    {
        auto type = makeClass("ContentIndexLookup");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();

        index.insert((*object)->getObject(), object->getUniqueAddress());

        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 42);
        auto result = index.lookup(initializer);

        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(object->getUniqueAddress(), *result);
    }

    TEST_F(ContentIndexTest, testLookupMissesDifferentContent)
    {
        auto type = makeClass("ContentIndexDifferentContent");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());

        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 43);

        ASSERT_FALSE(index.lookup(initializer).has_value());
    }

    TEST_F(ContentIndexTest, testLookupMissesSameFieldsFromDifferentClass)
    {
        auto indexedType = makeClass("ContentIndexIndexedType");
        auto lookupType = makeClass("ContentIndexLookupType");
        auto object = makeObject(indexedType, 42);
        auto &index = indexedType->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());

        ObjectImmutableImpl probe(lookupType);
        auto &initializer = setInitializerValue(probe, lookupType, 42);

        ASSERT_FALSE(index.lookup(initializer).has_value());
    }

    TEST_F(ContentIndexTest, testRollbackDiscardsPendingInsert)
    {
        auto type = makeClass("ContentIndexRollback");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());
        index.rollback();

        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 42);

        ASSERT_FALSE(index.lookup(initializer).has_value());
    }

    TEST_F(ContentIndexTest, testRemoveHidesInsertedCandidate)
    {
        auto type = makeClass("ContentIndexRemove");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());

        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 42);
        ASSERT_TRUE(index.lookup(initializer).has_value());

        index.remove((*object)->getObject(), object->getUniqueAddress());
        ASSERT_FALSE(index.lookup(initializer).has_value());
    }

    TEST_F(ContentIndexTest, testDuplicateInsertIsAccepted)
    {
        auto type = makeClass("ContentIndexDuplicateInsert");
        auto object = makeObject(type, 42);
        auto equivalentObject = makeObject(type, 42);
        auto &index = type->getContentIndex();

        ASSERT_NO_THROW(index.insert((*object)->getObject(), object->getUniqueAddress()));
        ASSERT_NO_THROW(index.insert((*equivalentObject)->getObject(), equivalentObject->getUniqueAddress()));

        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 42);
        auto result = index.lookup(initializer);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(*result == object->getUniqueAddress() || *result == equivalentObject->getUniqueAddress());
    }

    TEST_F(ContentIndexTest, testLookupManyRealImmutableObjectReferences)
    {
        auto type = makeClass("ContentIndexManyImmutableObjects");
        auto &index = type->getContentIndex();
        std::vector<std::unique_ptr<ObjectImmutableImpl>> objects;
        objects.reserve(100);

        for (std::int64_t value = 0; value < 100; ++value) {
            auto object = makeObject(type, value);
            index.insert((*object)->getObject(), object->getUniqueAddress());
            objects.push_back(std::move(object));
        }

        for (std::int64_t value = 0; value < 100; ++value) {
            ObjectImmutableImpl probe(type);
            auto &initializer = setInitializerValue(probe, type, value);
            auto result = index.lookup(initializer);

            ASSERT_TRUE(result.has_value()) << "value=" << value;
            ASSERT_EQ(objects[static_cast<std::size_t>(value)]->getUniqueAddress(), *result) << "value=" << value;
        }
    }

    TEST_F(ContentIndexTest, testClassCreatesContentIndexLazilyAndReopensIt)
    {
        auto type = makeClass("ContentIndexClassIntegration");
        ASSERT_FALSE(type->hasContentIndex());

        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        ASSERT_TRUE(type->hasContentIndex());
        index.insert((*object)->getObject(), object->getUniqueAddress());
        type->commit();

        Class reopened(m_fixture, type->getAddress());
        ASSERT_TRUE(reopened.hasContentIndex());
        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 42);

        auto result = reopened.getContentIndex().lookup(initializer);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(object->getUniqueAddress(), *result);
    }

}
