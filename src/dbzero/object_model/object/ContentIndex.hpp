// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp>
#include <dbzero/core/memory/Memspace.hpp>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>

namespace db0
{
    class Fixture;
}

namespace db0::object_model
{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_content_index: public db0::o_fixed_versioned<o_content_index>
    {
        Address m_base_index_ptr = {};
        std::array<std::uint64_t, 4> m_reserved = {0, 0, 0, 0};
    };
DB0_PACKED_END

    using ContentBucketIndex = db0::MorphingBIndex<UniqueAddress>;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR ContentBucketRef
    {
        db0::Address m_index_address = {};
        db0::bindex::type m_type = db0::bindex::type::empty;

        ContentBucketRef() = default;

        ContentBucketRef(const ContentBucketIndex &index)
            : m_index_address(index.getAddress())
            , m_type(index.getIndexType())
        {
        }

        ContentBucketIndex getIndex(db0::Memspace &memspace) const
        {
            return {memspace, m_index_address, m_type};
        }
    };
DB0_PACKED_END

    class ContentIndex: public db0::v_object<o_content_index>
    {
    public:
        using super_t = db0::v_object<o_content_index>;
        using HashT = std::uint64_t;
        using BucketIndexT = ContentBucketIndex;
        using BucketItemT = db0::key_value<HashT, ContentBucketRef>;
        using BaseIndexT = db0::v_bindex<BucketItemT>;

        ContentIndex(db0::swine_ptr<db0::Fixture> &);
        ContentIndex(mptr, db0::swine_ptr<db0::Fixture> &);
        ~ContentIndex();

        void insert(const o_embedded_object &, UniqueAddress) const;
        void remove(const o_embedded_object &, UniqueAddress) const;
        std::optional<UniqueAddress> lookup(const ImmutableObjectInitializer &) const;

        void rollback();
        void flush() const;
        void commit() const;
        void detach() const;
        void close();
        bool empty() const;

    private:
        db0::swine_ptr<db0::Fixture> m_fixture;
        mutable BaseIndexT m_base_index;
        struct PendingUpdate
        {
            bool m_insert = false;
            HashT m_hash = 0;
            UniqueAddress m_address = {};
        };
        mutable std::vector<PendingUpdate> m_pending_updates;

        void applyInsert(HashT, UniqueAddress) const;
        void applyRemove(HashT, UniqueAddress) const;
        void resyncBucket(typename BaseIndexT::iterator &, HashT, const BucketIndexT &) const;
        bool candidateMatches(const ImmutableObjectInitializer &, UniqueAddress) const;
    };

}
