// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "CompositeTagDef.hpp"
#include <cstdint>

namespace db0::object_model

{

    CompositeTagDef::CompositeTagDef(std::vector<ObjectSharedPtr> &&items)
        : m_items(std::move(items))
    {
    }

    const std::vector<CompositeTagDef::ObjectSharedPtr> &CompositeTagDef::getItems() const
    {
        return m_items;
    }

    std::size_t CompositeTagDef::size() const
    {
        return m_items.size();
    }

    bool CompositeTagDef::operator==(const CompositeTagDef &other) const
    {
        if (m_items.size() != other.m_items.size()) {
            return false;
        }
        for (std::size_t i = 0; i < m_items.size(); ++i) {
            if (m_items[i].get() != other.m_items[i].get()) {
                return false;
            }
        }
        return true;
    }

    bool CompositeTagDef::operator!=(const CompositeTagDef &other) const
    {
        return !(*this == other);
    }

    std::uint64_t CompositeTagDef::getHash() const
    {
        std::uint64_t result = 1469598103934665603ull;
        for (auto const &item: m_items) {
            result ^= static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(item.get()));
            result *= 1099511628211ull;
        }
        return result;
    }

}
