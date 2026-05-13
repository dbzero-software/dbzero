// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FieldMask.hpp"
#include <dbzero/core/memory/VObjectCache.hpp>

DEFINE_ENUM_VALUES(db0::object_model::FieldMaskOptions, "create", "read", "update", "delete")

namespace db0::object_model

{

    FieldMask::FieldMask(db0::Memspace &memspace, db0::Address address)
        : super_t(memspace.myPtr(address))
    {
    }

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

    FieldMaskManager::FieldMaskManager(db0::Memspace &memspace, db0::VObjectCache &cache)
        : super_t(memspace)
        , m_cache(&cache)
    {
        assert(m_cache);
    }

    FieldMaskManager::FieldMaskManager(db0::mptr ptr, db0::VObjectCache &cache)
        : super_t(ptr, ptr.getPageSize())
        , m_cache(&cache)
    {
        assert(m_cache);
    }

    std::shared_ptr<FieldMask> FieldMaskManager::createFieldMask(std::uint64_t account_id)
    {
        auto result = tryGetFieldMask(account_id);
        if (result) {
            return result;
        }

        result = m_cache->create<FieldMask>(true);
        super_t::insert(FieldMaskManagerItem(account_id, result->getAddress()));
        return result;
    }

    std::shared_ptr<FieldMask> FieldMaskManager::tryGetFieldMask(std::uint64_t account_id) const
    {
        auto it = super_t::find(account_id);
        if (it == super_t::end()) {
            return nullptr;
        }

        return fieldMaskFromAddress((*it).value);
    }

    std::shared_ptr<FieldMask> FieldMaskManager::fieldMaskFromAddress(db0::Address address) const
    {
        return m_cache->findOrCreate<FieldMask>(address, true, address);
    }

}
