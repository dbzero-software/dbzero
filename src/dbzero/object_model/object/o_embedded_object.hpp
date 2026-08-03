// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/object_model/dict/o_dict.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>

namespace db0::object_model
{
    struct EmbeddedObjectOffsetCollector
    {
        const std::byte *m_root = nullptr;
        std::vector<std::uint64_t> *m_offsets = nullptr;

        void add(const void *object) const
        {
            assert(m_root);
            assert(m_offsets);
            auto offset = static_cast<std::uint64_t>(
                reinterpret_cast<const std::byte *>(object) - m_root
            );
            assert(m_offsets->empty() || m_offsets->back() < offset);
            m_offsets->push_back(offset);
        }
    };

    struct FixedValue
    {
        StorageClass m_kind = StorageClass::UNDEFINED;
        std::uint64_t m_value = 0;

        FixedValue() = default;
        FixedValue(StorageClass kind, std::uint64_t value);

        bool isPack2() const;
        std::optional<FixedValue> unpack2(unsigned int offset) const;
    };

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_embedded_object: public db0::o_base<o_embedded_object, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_embedded_object, 0, false>;
        friend super_t;

    public:
        using Element = o_tuple_item::Element;

        o_embedded_object(std::uint32_t classRef, const ImmutableObjectInitializer &initializer);
        o_embedded_object(
            std::uint32_t classRef, const ImmutableObjectInitializer &initializer,
            EmbeddedObjectOffsetCollector &offsetCollector
        );
        o_embedded_object(
            std::uint32_t classRef, const PosVT::Data &posVtData, unsigned int posVtOffset,
            const XValue *indexVtBegin = nullptr, const XValue *indexVtEnd = nullptr
        );

        std::uint32_t getClassRef() const;
        const PosVT &pos_vt() const;
        PosVT &pos_vt();
        const IndexVT &index_vt() const;
        IndexVT &index_vt();
        const o_dict &field_map() const;
        std::optional<FixedValue> fixedValue(std::uint32_t index, unsigned int fidelityOffset = 0) const;
        const o_tuple_item *variableValue(std::uint32_t index) const;
        std::size_t sizeOf() const;

        static std::size_t measure(std::uint32_t classRef, const ImmutableObjectInitializer &initializer);
        static std::size_t measure(
            std::uint32_t classRef, const PosVT::Data &posVtData, unsigned int posVtOffset,
            const XValue *indexVtBegin = nullptr, const XValue *indexVtEnd = nullptr
        );

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto cursor = buf;
            cursor += super_t::baseSize();
            cursor += db0::packed_int32::safeSizeOf(cursor);
            cursor += PosVT::safeSizeOf(cursor);
            cursor += IndexVT::safeSizeOf(cursor);
            cursor += o_dict::safeSizeOf(cursor);
            return cursor - start;
        }

    protected:
        o_embedded_object() = default;

    private:
        void construct(
            std::uint32_t classRef, const ImmutableObjectInitializer &initializer,
            EmbeddedObjectOffsetCollector *offsetCollector
        );
        const db0::packed_int32 &classRef() const;
    };
DB0_PACKED_END

}
