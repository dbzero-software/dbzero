// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/object_model/value/StorageClass.hpp>

namespace db0::object_model
{

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_tuple_item: public db0::o_base<o_tuple_item, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_tuple_item, 0, false>;
        friend super_t;

    public:
        struct Element
        {
            struct BytesView
            {
                const std::byte *m_data = nullptr;
                std::size_t m_size = 0;
            };

            StorageClass m_storage_class = StorageClass::UNDEFINED;
            union Payload
            {
                std::int64_t m_int_value;
                double m_double_value;
                bool m_bool_value;
                std::string_view m_string_value;
                BytesView m_bytes_value;

                Payload();
            } m_payload;

            static Element none();
            static Element boolean(bool value);
            static Element integer(std::int64_t value);
            static Element floating(double value);
            static Element string(std::string_view value);
            static Element bytes(const std::byte *data, std::size_t size);
            static Element bytes(const std::vector<std::byte> &value);

            std::int64_t intValue() const;
            double doubleValue() const;
            bool boolValue() const;
            std::string stringValue() const;
            const std::byte *bytesData() const;
            std::size_t bytesSize() const;
        };

        explicit o_tuple_item(const Element &element);

        StorageClass storageClass() const;
        std::size_t sizeOf() const;

        static std::size_t measure(const Element &element);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto cursor = buf;
            cursor += super_t::baseSize();
            advancePayload(super_t::__const_ref(buf).m_storage_class, cursor);
            return cursor - start;
        }

    private:
        StorageClass m_storage_class = StorageClass::UNDEFINED;

        void arrangePayload(const Element &element);

    public:
        const o_simple<bool> &boolPayload() const;
        const o_simple<std::int64_t> &intPayload() const;
        const o_simple<double> &doublePayload() const;
        const o_string &stringPayload() const;
        const o_binary &bytesPayload() const;

    private:
        template <typename BufT> static void advancePayload(StorageClass storageClass, BufT &cursor)
        {
            switch (storageClass) {
            case StorageClass::NONE:
                return;
            case StorageClass::BOOLEAN:
                cursor += o_simple<bool>::safeSizeOf(cursor);
                return;
            case StorageClass::INT64:
                cursor += o_simple<std::int64_t>::safeSizeOf(cursor);
                return;
            case StorageClass::FP_NUMERIC64:
                cursor += o_simple<double>::safeSizeOf(cursor);
                return;
            case StorageClass::STRING_REF:
                cursor += o_string::safeSizeOf(cursor);
                return;
            case StorageClass::DB0_BYTES:
                cursor += o_binary::safeSizeOf(cursor);
                return;
            default:
                throwUnsupportedStorageClass();
            }
        }

        static void throwUnsupportedStorageClass();
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_tuple: public db0::o_base<o_tuple, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_tuple, 0, false>;
        friend super_t;

    public:
        using Element = o_tuple_item::Element;

        class const_iterator
        {
        public:
            const_iterator() = default;

            const o_tuple_item &operator*() const;
            const o_tuple_item *operator->() const;
            const_iterator &operator++();
            bool operator==(const const_iterator &other) const;
            bool operator!=(const const_iterator &other) const;

        private:
            friend class o_tuple;

            explicit const_iterator(const o_tuple_item *item);

            const o_tuple_item *m_item = nullptr;
        };

        explicit o_tuple(const std::vector<Element> &elements);

        std::size_t size() const;
        std::size_t elementsByteSize() const;
        bool empty() const;
        const o_tuple_item &item(std::size_t index) const;
        const_iterator begin() const;
        const_iterator end() const;
        std::size_t sizeOf() const;

        static std::size_t measure(const std::vector<Element> &elements);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto cursor = buf;
            cursor += super_t::baseSize();

            cursor += db0::packed_int32::safeSizeOf(cursor);
            auto elementsByteSizeAt = cursor;
            cursor += db0::packed_int32::safeSizeOf(cursor);

            auto elementsByteSize = db0::packed_int32::__const_ref(elementsByteSizeAt).value();
            cursor += elementsByteSize;
            return cursor - start;
        }

    protected:
        o_tuple() = default;

    private:
        const db0::packed_int32 &count() const;
        const db0::packed_int32 &elementsByteSizeMember() const;
        const std::byte *beginOfItems() const;
        static std::size_t measureElements(const std::vector<Element> &elements);
    };
DB0_PACKED_END

}
