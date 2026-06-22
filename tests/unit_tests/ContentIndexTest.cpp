// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

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

    TEST_F(ContentIndexTest, testContainsFindsInsertedAddress)
    {
        auto type = makeClass("ContentIndexLookup");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();

        index.insert((*object)->getObject(), object->getUniqueAddress());

        ASSERT_TRUE(index.contains((*object)->getObject(), object->getUniqueAddress()));
    }

    TEST_F(ContentIndexTest, testLookupMissesDifferentContent)
    {
        auto type = makeClass("ContentIndexDifferentContent");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());

        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 43);

        ASSERT_FALSE(index.lookupAddress(initializer).has_value());
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

        ASSERT_FALSE(index.lookupAddress(initializer).has_value());
    }

    TEST_F(ContentIndexTest, testLookupAddressReturnsMatchingAddress)
    {
        auto type = makeClass("ContentIndexLookupObject");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());

        ObjectImmutableImpl probe(type);
        auto &initializer = setInitializerValue(probe, type, 42);

        ASSERT_EQ(index.lookupAddress(initializer), object->getUniqueAddress());
    }

    TEST_F(ContentIndexTest, testRollbackDiscardsPendingInsert)
    {
        auto type = makeClass("ContentIndexRollback");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());
        index.rollback();
        ASSERT_EQ(index.size(), 0);

        ASSERT_FALSE(index.contains((*object)->getObject(), object->getUniqueAddress()));
    }

    TEST_F(ContentIndexTest, testRemoveHidesInsertedCandidate)
    {
        auto type = makeClass("ContentIndexRemove");
        auto object = makeObject(type, 42);
        auto &index = type->getContentIndex();
        index.insert((*object)->getObject(), object->getUniqueAddress());

        ASSERT_TRUE(index.contains((*object)->getObject(), object->getUniqueAddress()));
        ASSERT_EQ(index.size(), 1);

        index.remove((*object)->getObject(), object->getUniqueAddress());
        ASSERT_EQ(index.size(), 0);
        ASSERT_FALSE(index.contains((*object)->getObject(), object->getUniqueAddress()));
    }

    TEST_F(ContentIndexTest, testDuplicateInsertIsCountedOncePerAddress)
    {
        auto type = makeClass("ContentIndexDuplicateInsert");
        auto object = makeObject(type, 42);
        auto equivalentObject = makeObject(type, 42);
        auto &index = type->getContentIndex();

        ASSERT_NO_THROW(index.insert((*object)->getObject(), object->getUniqueAddress()));
        ASSERT_NO_THROW(index.insert((*equivalentObject)->getObject(), equivalentObject->getUniqueAddress()));
        ASSERT_EQ(index.size(), 2);
        ASSERT_TRUE(index.contains((*object)->getObject(), object->getUniqueAddress()));
        ASSERT_TRUE(index.contains((*equivalentObject)->getObject(), equivalentObject->getUniqueAddress()));
    }

    TEST_F(ContentIndexTest, testBucketRemainsReachableAcrossMorphingInsertAndRemove)
    {
        auto type = makeClass("ContentIndexBucketMorphing");
        auto &index = type->getContentIndex();
        std::vector<std::unique_ptr<ObjectImmutableImpl>> objects;
        objects.reserve(8);

        for (std::int64_t i = 0; i < 8; ++i) {
            auto object = makeObject(type, 42);
            index.insert((*object)->getObject(), object->getUniqueAddress());
            objects.push_back(std::move(object));
        }

        ASSERT_EQ(index.size(), objects.size());
        for (const auto &object : objects) {
            ASSERT_TRUE(index.contains((*object)->getObject(), object->getUniqueAddress()))
                << "address=" << object->getUniqueAddress();
        }

        std::vector<bool> removed(objects.size(), false);
        std::vector<std::size_t> removedIndexes = {3, 6, 1, 5};
        for (auto removedIndex : removedIndexes) {
            auto removedAddress = objects[removedIndex]->getUniqueAddress();
            index.remove((*objects[removedIndex])->getObject(), removedAddress);
            removed[removedIndex] = true;

            ASSERT_FALSE(index.contains((*objects[removedIndex])->getObject(), removedAddress))
                << "removedIndex=" << removedIndex;
            for (std::size_t i = 0; i < objects.size(); ++i) {
                if (removed[i]) {
                    continue;
                }
                ASSERT_TRUE(index.contains((*objects[i])->getObject(), objects[i]->getUniqueAddress()))
                    << "remainingIndex=" << i << " removedIndex=" << removedIndex;
            }
        }
        ASSERT_EQ(index.size(), objects.size() - removedIndexes.size());
    }

    TEST_F(ContentIndexTest, testContainsManyRealImmutableObjectReferences)
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
            ASSERT_TRUE(index.contains(
                (*objects[static_cast<std::size_t>(value)])->getObject(),
                objects[static_cast<std::size_t>(value)]->getUniqueAddress()
            )) << "value=" << value;
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

        auto reopened = std::make_shared<Class>(m_fixture, type->getAddress());
        ASSERT_TRUE(reopened->hasContentIndex());
        ASSERT_TRUE(reopened->getContentIndex().contains((*object)->getObject(), object->getUniqueAddress()));
    }

    TEST_F(ContentIndexTest, testLegacyClassVersionDoesNotExposeContentIndex)
    {
        auto type = std::static_pointer_cast<SubClass>(makeClass("ContentIndexLegacyClass"));
        type->forceObjVersionForTest(0);

        ASSERT_FALSE(type->hasContentIndex());
        ASSERT_THROW(type->getContentIndex(), db0::InputException);

        const Class &constType = *type;
        ASSERT_THROW(constType.getContentIndex(), db0::InputException);
    }

}
