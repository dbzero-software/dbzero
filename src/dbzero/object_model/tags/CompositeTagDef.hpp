// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <cstdint>
#include <vector>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{

    class CompositeTagDef
    {
    public:
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = typename LangToolkit::ObjectPtr;
        using ObjectSharedPtr = typename LangToolkit::ObjectSharedPtr;

        CompositeTagDef() = default;
        explicit CompositeTagDef(std::vector<ObjectSharedPtr> &&items);

        const std::vector<ObjectSharedPtr> &getItems() const;
        std::size_t size() const;

        bool operator==(const CompositeTagDef &other) const;
        bool operator!=(const CompositeTagDef &other) const;
        std::uint64_t getHash() const;

    private:
        std::vector<ObjectSharedPtr> m_items;
    };

}
