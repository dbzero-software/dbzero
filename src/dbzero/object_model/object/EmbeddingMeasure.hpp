// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>

namespace db0::object_model
{

    struct EmbeddingMeasure
    {
        // Storage class of the value being measured.
        StorageClass m_storageClass = StorageClass::UNDEFINED;
        // Bytes required by the embedded representation under consideration.
        std::size_t m_embeddedBytes = 0;
        // Bytes required if stored separately; zero until a caller supplies that comparison.
        std::size_t m_separateStorageBytes = 0;
        // Heuristic count of durable root/member allocations avoided by embedding this value.
        std::uint32_t m_allocationsAvoided = 0;
        // True when measurement depended on a memo/object wrapper view.
        bool m_requiresObjectView = false;
        // True when measurement depended on a collection wrapper/view.
        bool m_requiresCollectionView = false;
    };

    // Returns std::nullopt when the value cannot be embedded by this measurement path.
    std::optional<EmbeddingMeasure> tryMeasureEmbeddingValue(
        db0::bindings::TypeId typeId, StorageClass storageClass, LangConfig::ObjectPtr value
    );

}
