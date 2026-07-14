// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/bindings/python/iter/PyObjectIterator.hpp>
#include <dbzero/core/collections/full_text/FT_Iterator.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/tags/ObjectIteratorPool.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/WorkspaceView.hpp>
#include <utils/utils.hpp>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

namespace tests
{

    using namespace db0;

    namespace
    {
        class DetachableUniqueAddressIterator final: public FT_Iterator<UniqueAddress>
        {
        public:
            bool m_detached = false;
            std::size_t m_detach_count = 0;

            UniqueAddress getKey() const override {
                return UniqueAddress(Address::fromOffset(1), 1);
            }

            bool isEnd() const override {
                return false;
            }

            const std::type_info &typeId() const override {
                return typeid(DetachableUniqueAddressIterator);
            }

            void next(void *buf = nullptr) override {
                if (buf) {
                    auto key = getKey();
                    std::memcpy(buf, &key, sizeof(UniqueAddress));
                }
            }

            void operator++() override {
            }

            void operator--() override {
            }

            bool join(UniqueAddress, int = -1) override {
                return true;
            }

            void joinBound(UniqueAddress) override {
            }

            std::pair<UniqueAddress, bool> peek(UniqueAddress key) const override {
                return {key, true};
            }

            bool isNextKeyDuplicated() const override {
                return false;
            }

            std::unique_ptr<FT_Iterator<UniqueAddress> > beginTyped(int = -1) const override {
                return std::make_unique<DetachableUniqueAddressIterator>();
            }

            bool limitBy(UniqueAddress) override {
                return true;
            }

            std::ostream &dump(std::ostream &os) const override {
                return os << "DetachableUniqueAddressIterator";
            }

            void stop() override {
            }

            void detach() override {
                m_detached = true;
                ++m_detach_count;
            }

            FTIteratorType getSerialTypeId() const override {
                return FTIteratorType::Invalid;
            }

            void getSignature(std::vector<std::byte> &v) const override {
                v.resize(v.size() + FT_IteratorBase::SIGNATURE_SIZE);
            }

        protected:
            void serializeFTIterator(std::vector<std::byte> &) const override {
            }

            double compareToImpl(const FT_IteratorBase &) const override {
                return 1.0;
            }
        };
    }

    class ObjectIteratorPoolTest: public testing::Test
    {
    protected:
        static constexpr const char *prefix_name = "object-iterator-pool-test";
        static constexpr const char *file_name = "object-iterator-pool-test.db0";

        void SetUp() override
        {
            db0::tests::drop(file_name);
            if (!Py_IsInitialized()) {
                Py_InitializeEx(0);
            }
            ASSERT_EQ(PyType_Ready(&db0::python::PyObjectIteratorType), 0);
        }

        void TearDown() override
        {
            db0::tests::drop(file_name);
        }

        static std::function<void(db0::swine_ptr<Fixture> &, bool, bool, bool)> pythonFixtureInitializer()
        {
            auto object_model_initializer = db0::object_model::initializer();
            return [object_model_initializer](db0::swine_ptr<Fixture> &fixture, bool is_new, bool read_only, bool is_snapshot) {
                object_model_initializer(fixture, is_new, read_only, is_snapshot);
                if (!is_snapshot) {
                    auto &iterator_pool = fixture->addResource<db0::object_model::ObjectIteratorPool>();
                    fixture->addIteratorDetachHandler([&iterator_pool](std::uint64_t generation) {
                        return iterator_pool.detach(generation);
                    });
                    fixture->addCloseHandler([&iterator_pool](bool commit) {
                        if (!commit) {
                            iterator_pool.close();
                        }
                    });
                }
            };
        }

        static db0::python::shared_py_object<db0::python::PyObjectIterator *> makePyIterator(
            db0::swine_ptr<Fixture> fixture, DetachableUniqueAddressIterator *&query_ptr)
        {
            auto query = std::make_unique<DetachableUniqueAddressIterator>();
            query_ptr = query.get();
            auto object_iterator = std::make_shared<object_model::ObjectIterator>(fixture, std::move(query));
            auto py_iter = db0::python::PyObjectIteratorDefault_new();
            py_iter->makeNew(object_iterator);
            return py_iter;
        }
    };

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolDetachesRegisteredIterators)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto &pool = fixture->get<db0::object_model::ObjectIteratorPool>();
        DetachableUniqueAddressIterator *query_ptr = nullptr;
        auto py_iter = makePyIterator(fixture, query_ptr);

        pool.add(py_iter->getSharedPtr());

        ASSERT_EQ(pool.size(), 1u);
        ASSERT_EQ(pool.detach(), 1u);
        ASSERT_TRUE(query_ptr->m_detached);
        ASSERT_EQ(pool.size(), 1u);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolDetachShortCircuitsSameGeneration)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto &pool = fixture->get<db0::object_model::ObjectIteratorPool>();
        DetachableUniqueAddressIterator *query_ptr = nullptr;
        auto py_iter = makePyIterator(fixture, query_ptr);

        pool.add(py_iter->getSharedPtr());

        ASSERT_EQ(pool.detach(1), 1u);
        ASSERT_EQ(query_ptr->m_detach_count, 1u);
        ASSERT_EQ(pool.detach(1), 0u);
        ASSERT_EQ(query_ptr->m_detach_count, 1u);
        ASSERT_EQ(pool.detach(2), 1u);
        ASSERT_EQ(query_ptr->m_detach_count, 2u);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testFixtureDetachBatchDetachesOnlyOnce)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        std::vector<std::uint64_t> generations;
        fixture->addIteratorDetachHandler([&generations](std::uint64_t generation) {
            generations.push_back(generation);
            return 1;
        });

        {
            auto detach_guard = fixture->beginIteratorDetach();
            ASSERT_EQ(fixture->detachIterators(), 1u);
            ASSERT_EQ(fixture->detachIterators(), 0u);
            ASSERT_EQ(generations.size(), 1u);
        }

        ASSERT_EQ(fixture->detachIterators(), 1u);
        ASSERT_EQ(generations.size(), 2u);
        ASSERT_NE(generations[0], generations[1]);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolCleanupDropsExpiredNativeIterators)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto &pool = fixture->get<db0::object_model::ObjectIteratorPool>();
        DetachableUniqueAddressIterator *query_ptr = nullptr;
        auto py_iter = makePyIterator(fixture, query_ptr);

        pool.add(py_iter->getSharedPtr());
        py_iter->reset();

        ASSERT_EQ(pool.cleanup(), 1u);
        ASSERT_EQ(pool.size(), 0u);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolDropsDestroyedPythonIteratorsImmediately)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto &pool = fixture->get<db0::object_model::ObjectIteratorPool>();
        DetachableUniqueAddressIterator *query_ptr = nullptr;
        auto py_iter = makePyIterator(fixture, query_ptr);

        pool.add(py_iter->getSharedPtr());

        ASSERT_EQ(pool.size(), 1u);
        py_iter.reset();
        ASSERT_EQ(pool.size(), 0u);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolDetachCompactsExpiredNativeIterators)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto &pool = fixture->get<db0::object_model::ObjectIteratorPool>();
        DetachableUniqueAddressIterator *live_query_ptr = nullptr;
        DetachableUniqueAddressIterator *expired_query_ptr = nullptr;
        auto live_iter = makePyIterator(fixture, live_query_ptr);
        auto expired_iter = makePyIterator(fixture, expired_query_ptr);

        pool.add(live_iter->getSharedPtr());
        pool.add(expired_iter->getSharedPtr());
        expired_iter->reset();

        ASSERT_EQ(pool.detach(), 1u);
        ASSERT_TRUE(live_query_ptr->m_detached);
        ASSERT_EQ(pool.size(), 1u);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolClosePreventsNewRegistrations)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto &pool = fixture->get<db0::object_model::ObjectIteratorPool>();
        DetachableUniqueAddressIterator *query_ptr = nullptr;
        auto py_iter = makePyIterator(fixture, query_ptr);

        pool.close();
        pool.add(py_iter->getSharedPtr());

        ASSERT_TRUE(pool.isClosed());
        ASSERT_EQ(pool.size(), 0u);
        ASSERT_EQ(pool.detach(), 0u);
        ASSERT_FALSE(query_ptr->m_detached);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolRegistrationDoesNotIncreasePythonRefcount)
    {
        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        auto &pool = fixture->get<db0::object_model::ObjectIteratorPool>();
        DetachableUniqueAddressIterator *query_ptr = nullptr;
        auto py_iter = makePyIterator(fixture, query_ptr);
        auto *py_object = reinterpret_cast<PyObject *>(py_iter.get());
        auto refcount_before = Py_REFCNT(py_object);

        pool.add(py_iter->getSharedPtr());

        ASSERT_EQ(Py_REFCNT(py_object), refcount_before);
        ASSERT_EQ(pool.size(), 1u);
        workspace.close();
    }

    TEST_F(ObjectIteratorPoolTest, testObjectIteratorPoolRegistersForLiveFixturesButNotSnapshots)
    {
        {
            Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
            auto fixture = workspace.getFixture(prefix_name);
            ASSERT_NE(fixture->tryGet<db0::object_model::ObjectIteratorPool>(), nullptr);
            fixture->commit();
            workspace.close();
        }

        {
            Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
            auto read_only_fixture = workspace.getFixture(prefix_name, AccessType::READ_ONLY);
            ASSERT_NE(read_only_fixture->tryGet<db0::object_model::ObjectIteratorPool>(), nullptr);
            workspace.close();
        }

        Workspace workspace("", {}, {}, {}, {}, pythonFixtureInitializer());
        auto fixture = workspace.getFixture(prefix_name);
        ASSERT_NE(fixture->tryGet<db0::object_model::ObjectIteratorPool>(), nullptr);
        fixture->commit();
        auto snapshot = workspace.getWorkspaceView(fixture->getStateNum());
        auto snapshot_fixture = snapshot->getFixture(prefix_name, AccessType::READ_ONLY);
        ASSERT_EQ(snapshot_fixture->tryGet<db0::object_model::ObjectIteratorPool>(), nullptr);
        workspace.close();
    }

}
