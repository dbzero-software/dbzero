// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "FieldID.hpp"
#include <optional>
#include <string>
#include <unordered_map>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp>
#include <dbzero/core/collections/map/v_map.hpp>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/core/vspace/db0_ptr.hpp>
#include <dbzero/core/vspace/v_object.hpp>

namespace db0::object_model

{

    using FieldOffsetMapItem = db0::key_value<std::uint32_t, std::uint32_t>;
    using FieldClusterOffsetMap = db0::v_bindex<FieldOffsetMapItem, db0::Address, FieldOffsetMapItem::comparer>;
    using FieldExceptionOffsetMap = db0::v_bindex<FieldOffsetMapItem, db0::Address, FieldOffsetMapItem::comparer>;
    using NameOffsetMap = db0::v_map<db0::o_string, db0::o_simple<std::uint32_t>, db0::o_string::comp_t>;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_field_id_mapper: public db0::o_fixed_versioned<o_field_id_mapper>
    {
        db0_ptr<FieldClusterOffsetMap> m_field_cluster_offsets_ptr;
        db0_ptr<FieldExceptionOffsetMap> m_field_exception_offsets_ptr;
        db0_ptr<NameOffsetMap> m_name_offsets_ptr;
        std::uint32_t m_next_name_offset = FieldID::getClusterSize();
        std::uint32_t m_next_cluster_offset = FieldID::getClusterSize();

        o_field_id_mapper() = default;
    };
DB0_PACKED_END

    class FieldIDMapper: public db0::v_object<o_field_id_mapper>
    {
    public:
        using super_t = db0::v_object<o_field_id_mapper>;

        FieldIDMapper() = default;
        FieldIDMapper(db0::Memspace &);
        FieldIDMapper(db0::mptr);

        static constexpr std::uint32_t CLUSTER_SIZE = FieldID::getClusterSize();

        // Retrieve existing or assign a new field offset.
        std::uint32_t assignFieldOffset(FieldID);
        std::uint32_t assignFieldOffset(const char *field_name);

        std::uint32_t onFieldIDAssigned(const char *field_name, FieldID);
        bool renameField(const char *old_field_name, const char *new_field_name);

        std::optional<std::uint32_t> tryGetAssignedFieldOffset(FieldID) const;

        void detach() const;
        void commit() const;

    protected:
        std::uint32_t getFieldIDMappingCount() const;
        std::uint32_t getFieldIDClusterMappingCount() const;
        std::uint32_t getFieldIDExceptionMappingCount() const;
        std::uint32_t getNameMappingCount() const;
        bool hasFieldIDClusterMappingCollection() const;
        bool hasFieldIDExceptionMappingCollection() const;
        bool hasNameMappingCollection() const;

    private:
        mutable FieldClusterOffsetMap m_field_cluster_offsets;
        mutable FieldExceptionOffsetMap m_field_exception_offsets;
        mutable NameOffsetMap m_name_offsets;

        mutable std::unordered_map<std::uint32_t, std::uint32_t> m_field_cluster_offset_cache;
        mutable std::unordered_map<std::uint32_t, std::uint32_t> m_field_exception_offset_cache;
        mutable std::unordered_map<std::string, std::uint32_t> m_name_offset_cache;

        std::optional<std::uint32_t> tryGetFieldClusterOffset(std::uint32_t cluster_id) const;
        std::optional<std::uint32_t> tryGetFieldExceptionOffset(FieldID) const;
        std::optional<std::uint32_t> tryGetNameOffset(const char *field_name) const;
        FieldClusterOffsetMap *getFieldClusterOffsets() const;
        FieldClusterOffsetMap &getOrCreateFieldClusterOffsets();
        FieldExceptionOffsetMap *getFieldExceptionOffsets() const;
        FieldExceptionOffsetMap &getOrCreateFieldExceptionOffsets();
        NameOffsetMap *getNameOffsets() const;
        NameOffsetMap &getOrCreateNameOffsets();
        std::uint32_t assignNextNameOffset();
        std::uint32_t assignNextClusterOffset();
        std::uint32_t assignFieldExceptionOffset(FieldID, std::uint32_t offset);
    };

}
