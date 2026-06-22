// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include "ContentIndex.hpp"

#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/object/InternContent.hpp>
#include <dbzero/object_model/object/o_immutable_object.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model
{
    ContentIndex::ContentIndex(db0::swine_ptr<db0::Fixture> &fixture, std::shared_ptr<Class> type)
        : super_t(*fixture)
        , m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_class(std::move(type))
        , m_base_index(*fixture)
    {
        modify().m_base_index_ptr = m_base_index.getAddress();
    }

    ContentIndex::ContentIndex(mptr ptr, db0::swine_ptr<db0::Fixture> &fixture, std::shared_ptr<Class> type)
        : super_t(ptr)
        , m_fixture(fixture)
        , m_class_factory(fixture->get<ClassFactory>())
        , m_class(std::move(type))
        , m_base_index(myPtr((*this)->m_base_index_ptr))
    {
    }

    ContentIndex::~ContentIndex()
    {
        assert(m_pending_updates.empty() && "ContentIndex::flush() or close() must be called before destruction");
    }

    void ContentIndex::resyncBucket(typename BaseIndexT::iterator &iterator, const BucketIndexT &bucket) const
    {
        iterator.modifyItem().value.m_index_address = bucket.getAddress();
        iterator.modifyItem().value.m_type = bucket.getIndexType();
    }

    void ContentIndex::incrementSize() const
    {
        ++const_cast<ContentIndex *>(this)->modify().m_size;
    }

    void ContentIndex::decrementSize() const
    {
        --const_cast<ContentIndex *>(this)->modify().m_size;
    }

    void ContentIndex::applyInsert(HashT hash, UniqueAddress address) const
    {
        auto iterator = m_base_index.find(hash);
        if (iterator == m_base_index.end()) {
            BucketIndexT bucket(getMemspace(), address);
            m_base_index.insert({hash, bucket});
            incrementSize();
            return;
        }

        auto bucket = (*iterator).value.getIndex(getMemspace());
        if (bucket.contains(address)) {
            return;
        }
        auto oldAddress = bucket.getAddress();
        bucket.insert(address);
        if (bucket.getAddress() != oldAddress) {
            resyncBucket(iterator, bucket);
        }
        incrementSize();
    }

    void ContentIndex::applyRemove(HashT hash, UniqueAddress address) const
    {
        auto iterator = m_base_index.find(hash);
        if (iterator == m_base_index.end()) {
            return;
        }

        auto bucket = (*iterator).value.getIndex(getMemspace());
        if (!bucket.contains(address)) {
            return;
        }

        if (bucket.size() == 1) {
            m_base_index.erase(iterator);
            bucket.destroy();
            decrementSize();
            return;
        }

        auto oldAddress = bucket.getAddress();
        bucket.erase(address);
        if (bucket.getAddress() != oldAddress) {
            resyncBucket(iterator, bucket);
        }
        decrementSize();
    }

    void ContentIndex::insert(const o_embedded_object &key, UniqueAddress address) const
    {
        auto fixture = m_fixture;
        auto hash = intern_hash(fixture, key);
        m_pending_updates.push_back({true, hash, address});
    }

    void ContentIndex::remove(const o_embedded_object &key, UniqueAddress address) const
    {
        auto fixture = m_fixture;
        auto hash = intern_hash(fixture, key);
        m_pending_updates.push_back({false, hash, address});
    }

    bool ContentIndex::contains(HashT hash, UniqueAddress address) const
    {
        flush();

        auto iterator = m_base_index.find(hash);
        if (iterator == m_base_index.end()) {
            return false;
        }

        auto bucket = (*iterator).value.getIndex(getMemspace());
        return bucket.contains(address);
    }

    bool ContentIndex::contains(const o_embedded_object &key, UniqueAddress address) const
    {
        auto fixture = m_fixture;
        return contains(intern_hash(fixture, key), address);
    }

    bool ContentIndex::contains(const ImmutableObjectInitializer &initializer, UniqueAddress address) const
    {
        auto fixture = m_fixture;
        return contains(intern_hash(fixture, initializer), address);
    }

    bool ContentIndex::candidateMatches(const ImmutableObjectInitializer &initializer, UniqueAddress candidate) const
    {
        auto fixture = m_fixture;
        auto candidateObject = LangConfig::LangToolkit::unloadAnyObject(
            fixture, candidate.getAddress(), m_class_factory, nullptr, candidate.getInstanceId(), AccessFlags {}
        );
        return intern_compare(
            fixture, initializer, LangConfig::LangToolkit::getMemoImmutableObject(candidateObject.get())
        ) == 0;
    }

    std::optional<UniqueAddress> ContentIndex::lookup(const ImmutableObjectInitializer &initializer) const
    {
        flush();

        auto fixture = m_fixture;
        auto iterator = m_base_index.find(intern_hash(fixture, initializer));
        if (iterator == m_base_index.end()) {
            return std::nullopt;
        }

        auto bucket = (*iterator).value.getIndex(getMemspace());
        auto bucketIterator = bucket.beginJoin(1);
        while (!bucketIterator.is_end()) {
            auto candidateAddress = *bucketIterator;
            if (candidateMatches(initializer, candidateAddress)) {
                return candidateAddress;
            }
            ++bucketIterator;
        }
        return std::nullopt;
    }

    void ContentIndex::rollback()
    {
        m_pending_updates.clear();
    }

    void ContentIndex::flush() const
    {
        if (m_pending_updates.empty()) {
            return;
        }

        auto pendingUpdates = std::move(m_pending_updates);
        m_pending_updates.clear();
        for (const auto &update : pendingUpdates) {
            if (update.m_insert) {
                applyInsert(update.m_hash, update.m_address);
            } else {
                applyRemove(update.m_hash, update.m_address);
            }
        }
    }

    void ContentIndex::commit() const
    {
        flush();
        m_base_index.commit();
        super_t::commit();
    }

    void ContentIndex::detach() const
    {
        m_base_index.detach();
        super_t::detach();
    }

    void ContentIndex::close()
    {
        m_pending_updates.clear();
    }

    bool ContentIndex::empty() const
    {
        return m_pending_updates.empty() && m_base_index.empty();
    }

    std::uint64_t ContentIndex::size() const
    {
        flush();
        return (*this)->m_size;
    }

}
