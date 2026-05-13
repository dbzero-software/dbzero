// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FieldMask.hpp"

DEFINE_ENUM_VALUES(db0::object_model::FieldMaskOptions, "create", "read", "update", "delete")

namespace db0::object_model

{

    void FieldMask::setMask(std::uint32_t field_offset, FieldMaskFlags mask)
    {
        auto slot = getSlot(field_offset);
        auto shift = getShift(field_offset);
        auto value = static_cast<std::uint8_t>(mask.value() & VALUE_MASK);
        auto slot_value = slot < this->size() ? this->getItem(slot) : std::uint8_t(0);
        slot_value &= ~(VALUE_MASK << shift);
        slot_value |= value << shift;
        this->setItem(slot, slot_value);
    }

    std::optional<FieldMaskFlags> FieldMask::getMask(std::uint32_t field_offset) const
    {
        auto slot = getSlot(field_offset);
        if (slot >= this->size()) {
            return FieldMaskFlags {};
        }

        return decode(this->getItem(slot), getShift(field_offset));
    }

    std::uint64_t FieldMask::getSlot(std::uint32_t field_offset)
    {
        return field_offset / 2;
    }

    unsigned int FieldMask::getShift(std::uint32_t field_offset)
    {
        return field_offset % 2 == 0 ? 0 : 4;
    }

    FieldMaskFlags FieldMask::decode(std::uint8_t slot_value, unsigned int shift)
    {
        return FieldMaskFlags::fromValue((slot_value >> shift) & VALUE_MASK);
    }

}
