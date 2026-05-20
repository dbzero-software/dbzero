// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

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

        std::uint32_t getClassRef() const;
        const PosVT &pos_vt() const;
        const IndexVT &index_vt() const;
        const o_dict &field_map() const;
        std::optional<FixedValue> fixedValue(std::uint32_t index, unsigned int fidelityOffset = 0) const;
        const o_tuple_item *variableValue(std::uint32_t index) const;
        std::size_t sizeOf() const;

        static std::size_t measure(std::uint32_t classRef, const ImmutableObjectInitializer &initializer);

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
        const db0::packed_int32 &classRef() const;
    };
DB0_PACKED_END

}
