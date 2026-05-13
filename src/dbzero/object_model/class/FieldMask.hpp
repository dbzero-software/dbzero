// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <dbzero/core/collections/b_index/v_bindex.hpp>
#include <dbzero/core/collections/full_text/key_value.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/utils/FlagSet.hpp>

namespace db0

{

    class VObjectCache;

}

namespace db0::object_model

{

    enum class FieldMaskOptions : std::uint8_t
    {
        CREATE = 0x01,
        READ = 0x02,
        UPDATE = 0x04,
        DELETE = 0x08,
    };

}

DECLARE_ENUM_VALUES(db0::object_model::FieldMaskOptions, 4)

namespace db0::object_model

{

    using FieldMaskFlags = db0::FlagSet<FieldMaskOptions>;

    class FieldMask: public db0::v_bvector<std::uint8_t>
    {
    public:
        using super_t = db0::v_bvector<std::uint8_t>;
        using super_t::super_t;

        FieldMask() = default;

        void setMask(std::uint32_t field_offset, FieldMaskFlags);
        std::optional<FieldMaskFlags> getMask(std::uint32_t field_offset) const;

    private:
        static constexpr std::uint8_t VALUE_MASK = 0x0f;
        static_assert(db0::FlagSetLimits<FieldMaskOptions>::count() == 4);

        static std::uint64_t getSlot(std::uint32_t field_offset);
        static unsigned int getShift(std::uint32_t field_offset);
        static FieldMaskFlags decode(std::uint8_t slot_value, unsigned int shift);
    };

    using FieldMaskManagerItem = db0::key_value<std::uint64_t, db0::Address>;

    class FieldMaskManager: public db0::v_bindex<FieldMaskManagerItem, db0::Address, FieldMaskManagerItem::comparer>
    {
    public:
        using super_t = db0::v_bindex<FieldMaskManagerItem, db0::Address, FieldMaskManagerItem::comparer>;

        FieldMaskManager(db0::Memspace &, db0::VObjectCache &);
        FieldMaskManager(db0::mptr, db0::VObjectCache &);

        std::shared_ptr<FieldMask> createFieldMask(std::uint64_t account_id);
        std::shared_ptr<FieldMask> tryGetFieldMask(std::uint64_t account_id) const;

    private:
        db0::VObjectCache *m_cache = nullptr;

        std::shared_ptr<FieldMask> fieldMaskFromAddress(db0::Address address) const;
    };

}
