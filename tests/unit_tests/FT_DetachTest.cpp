// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/ScopedWorkspaceFixture.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>
#include <dbzero/core/collections/full_text/FT_ANDNOTIterator.hpp>
#include <dbzero/core/collections/full_text/CartesianProduct.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>
#include <dbzero/core/collections/full_text/FT_IndexIterator.hpp>
#include <dbzero/core/collections/full_text/FT_ORXIterator.hpp>
#include <dbzero/core/collections/full_text/FT_SpanIterator.hpp>
#include <dbzero/object_model/ObjectModel.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <array>

namespace tests
{

    using namespace db0;

    class FT_DetachTest: public MemspaceTestBase
    {
    protected:
        using IndexT = MorphingBIndex<std::uint64_t>;

        void insert(IndexT &index, std::initializer_list<std::uint64_t> values)
        {
            index.bulkInsertUnique(values.begin(), values.end());
        }

        void erase(IndexT &index, std::initializer_list<std::uint64_t> values)
        {
            index.bulkErase(values.begin(), values.end());
        }

        std::unique_ptr<FT_IndexIterator<IndexT, std::uint64_t> > makeIndexIterator(IndexT &index, int direction)
        {
            return std::make_unique<FT_IndexIterator<IndexT, std::uint64_t>>(
                index, direction
            );
        }
    };

    class DetachableUniqueAddressIterator final: public FT_Iterator<UniqueAddress>
    {
    public:
        bool m_detached = false;

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

    class ObjectIteratorDetachTest: public testing::Test
    {
    };

    TEST_F(FT_DetachTest, testIndexIteratorReattachesToSameKeyAfterMorphingMutation)
    {
        auto memspace = getMemspace();
        IndexT index(memspace, bindex::type::empty, 4);
        insert(index, {1, 3, 5});

        auto it = makeIndexIterator(index, -1);
        ASSERT_EQ(it->getKey(), 5u);
        it->detach();

        insert(index, {2, 4, 6});

        ASSERT_FALSE(it->isEnd());
        ASSERT_EQ(it->getKey(), 5u);
        std::uint64_t key = 0;
        it->next(&key);
        ASSERT_EQ(key, 5u);
    }

    TEST_F(FT_DetachTest, testIndexIteratorReattachesToDirectionClosestKeyWhenSavedKeyWasRemoved)
    {
        auto memspace = getMemspace();
        IndexT index(memspace, bindex::type::empty, 4);
        insert(index, {1, 3, 5, 7});

        auto backward = makeIndexIterator(index, -1);
        ASSERT_TRUE(backward->join(5, -1));
        backward->detach();
        erase(index, {5});
        ASSERT_FALSE(backward->isEnd());
        ASSERT_EQ(backward->getKey(), 3u);

        auto forward = makeIndexIterator(index, 1);
        ASSERT_TRUE(forward->join(3, 1));
        forward->detach();
        erase(index, {3});
        ASSERT_FALSE(forward->isEnd());
        ASSERT_EQ(forward->getKey(), 7u);
    }

    TEST_F(FT_DetachTest, testIndexIteratorDetachedAtEndStaysAtEnd)
    {
        auto memspace = getMemspace();
        IndexT index(memspace, bindex::type::empty, 4);
        insert(index, {1});

        auto it = makeIndexIterator(index, -1);
        it->next();
        ASSERT_TRUE(it->isEnd());
        it->detach();
        insert(index, {2});

        ASSERT_TRUE(it->isEnd());
    }

    TEST_F(FT_DetachTest, testAndIteratorReattachesChildrenAfterMutation)
    {
        auto memspace = getMemspace();
        IndexT left(memspace, bindex::type::empty, 4);
        IndexT right(memspace, bindex::type::empty, 4);
        insert(left, {1, 3, 5});
        insert(right, {3, 5});

        auto it = std::make_unique<FT_JoinANDIterator<std::uint64_t>>(
            makeIndexIterator(left, -1), makeIndexIterator(right, -1), -1
        );
        ASSERT_EQ(it->getKey(), 5u);
        it->detach();
        erase(left, {5});
        erase(right, {5});

        ASSERT_FALSE(it->isEnd());
        ASSERT_EQ(it->getKey(), 3u);
    }

    TEST_F(FT_DetachTest, testOrIteratorRebuildsHeapAfterMutation)
    {
        auto memspace = getMemspace();
        IndexT left(memspace, bindex::type::empty, 4);
        IndexT right(memspace, bindex::type::empty, 4);
        insert(left, {1, 5});
        insert(right, {3});

        FT_ORIteratorFactory<std::uint64_t> factory;
        factory.add(makeIndexIterator(left, -1));
        factory.add(makeIndexIterator(right, -1));
        auto it = factory.release(-1);
        ASSERT_EQ(it->getKey(), 5u);
        it->detach();
        erase(left, {5});
        insert(right, {7});

        ASSERT_FALSE(it->isEnd());
        ASSERT_EQ(it->getKey(), 3u);
    }

    TEST_F(FT_DetachTest, testAndNotIteratorReattachesAfterMutation)
    {
        auto memspace = getMemspace();
        IndexT base(memspace, bindex::type::empty, 4);
        IndexT excluded(memspace, bindex::type::empty, 4);
        insert(base, {1, 3, 5});
        insert(excluded, {3});

        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> iterators;
        iterators.emplace_back(makeIndexIterator(base, -1));
        iterators.emplace_back(makeIndexIterator(excluded, -1));
        FT_ANDNOTIterator<std::uint64_t> it(std::move(iterators), -1);
        ASSERT_EQ(it.getKey(), 5u);
        it.detach();
        erase(base, {5});
        insert(excluded, {3, 7});

        ASSERT_FALSE(it.isEnd());
        ASSERT_EQ(it.getKey(), 1u);
    }

    TEST_F(FT_DetachTest, testSpanIteratorDetachesInnerIterator)
    {
        auto memspace = getMemspace();
        IndexT index(memspace, bindex::type::empty, 4);
        insert(index, {15, 31, 47});

        FT_SpanIterator<std::uint64_t> it(makeIndexIterator(index, -1), 4, -1);
        ASSERT_EQ(it.getKey(), 47u);
        it.detach();
        erase(index, {47});

        ASSERT_FALSE(it.isEnd());
        ASSERT_EQ(it.getKey(), 31u);
    }

    TEST_F(FT_DetachTest, testCartesianProductReattachesComponents)
    {
        auto memspace = getMemspace();
        IndexT left(memspace, bindex::type::empty, 4);
        IndexT right(memspace, bindex::type::empty, 4);
        insert(left, {1, 3, 5});
        insert(right, {10, 20});

        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> components;
        components.emplace_back(makeIndexIterator(left, 1));
        components.emplace_back(makeIndexIterator(right, 1));
        CartesianProduct<std::uint64_t> it(std::move(components), 1);
        std::array<std::uint64_t, 2> key {3, 10};
        ASSERT_TRUE(it.join(key.data(), 1));
        ASSERT_EQ(it.getKey()[0], 3u);
        ASSERT_EQ(it.getKey()[1], 10u);

        it.detach();
        erase(left, {3});

        ASSERT_FALSE(it.isEnd());
        ASSERT_EQ(it.getKey()[0], 5u);
        ASSERT_EQ(it.getKey()[1], 10u);
    }

    TEST_F(FT_DetachTest, testNestedCompositeIteratorReattachesAfterProductionLikeMutation)
    {
        auto memspace = getMemspace();
        IndexT alpha(memspace, bindex::type::empty, 4);
        IndexT beta(memspace, bindex::type::empty, 4);
        IndexT base(memspace, bindex::type::empty, 4);
        IndexT excluded_red(memspace, bindex::type::empty, 4);
        IndexT excluded_blue(memspace, bindex::type::empty, 4);

        insert(alpha, {10, 30, 50, 70});
        insert(beta, {20, 40, 60});
        insert(base, {15, 20, 30, 40, 50, 60, 70});
        insert(excluded_red, {20, 70});
        insert(excluded_blue, {15});

        FT_ORIteratorFactory<std::uint64_t> include_factory;
        include_factory.add(makeIndexIterator(alpha, -1));
        include_factory.add(makeIndexIterator(beta, -1));

        FT_ORIteratorFactory<std::uint64_t> exclude_factory;
        exclude_factory.add(makeIndexIterator(excluded_red, -1));
        exclude_factory.add(makeIndexIterator(excluded_blue, -1));

        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> right_branch;
        right_branch.emplace_back(makeIndexIterator(base, -1));
        right_branch.emplace_back(exclude_factory.release(-1));

        std::list<std::unique_ptr<FT_Iterator<std::uint64_t>>> root_branches;
        root_branches.emplace_back(include_factory.release(-1));
        root_branches.emplace_back(std::make_unique<FT_ANDNOTIterator<std::uint64_t>>(std::move(right_branch), -1));

        FT_JoinANDIterator<std::uint64_t> it(std::move(root_branches), -1);
        ASSERT_FALSE(it.isEnd());
        ASSERT_EQ(it.getKey(), 60u);
        ASSERT_TRUE(it.join(50, -1));
        ASSERT_EQ(it.getKey(), 50u);

        it.detach();

        erase(alpha, {50, 70});
        insert(alpha, {45, 55});
        erase(beta, {40});
        insert(beta, {35, 65});
        erase(base, {50, 60});
        insert(base, {35, 45, 55, 65});
        erase(excluded_red, {70});
        insert(excluded_blue, {55});

        ASSERT_FALSE(it.isEnd());
        ASSERT_EQ(it.getKey(), 45u);

        std::vector<std::uint64_t> results;
        for (std::size_t guard = 0; !it.isEnd() && guard < 16; ++guard) {
            std::uint64_t key = 0;
            it.next(&key);
            results.emplace_back(key);
        }

        ASSERT_TRUE(it.isEnd());
        ASSERT_EQ(results, (std::vector<std::uint64_t> {45, 35, 30}));
    }

    TEST_F(ObjectIteratorDetachTest, testObjectIteratorDetachDelegatesToUnderlyingIterator)
    {
        ScopedWorkspaceFixture workspace_fixture("object-iterator-detach-test");
        auto fixture = workspace_fixture.fixture();
        auto query = std::make_unique<DetachableUniqueAddressIterator>();
        auto *query_ptr = query.get();

        object_model::ObjectIterator iterator(fixture, std::move(query));
        iterator.detach();

        ASSERT_TRUE(query_ptr->m_detached);
        workspace_fixture.close();
    }

}
