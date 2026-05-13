// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <optional>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <dbzero/core/utils/FlagSet.hpp>

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

}
