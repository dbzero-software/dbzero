// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cassert>
#include <cstdint>
#include <new>
#include <utility>

#include <dbzero/core/collections/full_text/LongTag.hpp>
#include <dbzero/core/collections/pools/StringPools.hpp>

namespace db0::object_model

{

    class TagIndex;

    struct PassiveTag
    {
        db0::LongTagT m_tag = {};
        const std::uint32_t m_source_id = 0;
        db0::pools::RC_LimitedStringPool *m_string_pool = nullptr;
        db0::TagAddress m_owned_string = {};

        explicit PassiveTag(std::uint32_t source_id = 0)
            : m_source_id(source_id)
        {
        }
        PassiveTag(const PassiveTag &) = delete;
        PassiveTag &operator=(const PassiveTag &) = delete;

        PassiveTag(PassiveTag &&other) noexcept
            : m_tag(other.m_tag)
            , m_source_id(other.m_source_id)
            , m_string_pool(other.m_string_pool)
            , m_owned_string(other.m_owned_string)
        {
            other.m_string_pool = nullptr;
        }

        PassiveTag &operator=(PassiveTag &&other) noexcept
        {
            if (this != &other) {
                this->~PassiveTag();
                new (this) PassiveTag(std::move(other));
            }
            return *this;
        }

        ~PassiveTag() {
            release();
        }

        static PassiveTag from_short(db0::TagAddress tag, db0::pools::RC_LimitedStringPool *string_pool,
            db0::TagAddress owned_string = {}, std::uint32_t source_id = 0)
        {
            PassiveTag result(source_id);
            result.m_tag = db0::LongTagT { tag.getValue(), 0 };
            result.m_string_pool = string_pool;
            result.m_owned_string = owned_string;
            return result;
        }

        static PassiveTag from_long(db0::LongTagT tag, std::uint32_t source_id = 0) {
            assert(tag.data[1]);
            PassiveTag result(source_id);
            result.m_tag = tag;
            return result;
        }

        bool hasTag() const {
            return m_tag.data[0] != 0 || m_tag.data[1] != 0;
        }

        bool is_long() const {
            assert(hasTag());
            return m_tag.data[1] != 0;
        }

        db0::TagAddress short_tag() const {
            assert(!is_long());
            return db0::TagAddress::fromValue(m_tag.data[0]);
        }

        const db0::LongTagT &long_tag() const {
            assert(is_long());
            return m_tag;
        }

        bool operator==(const PassiveTag &other) const {
            return m_tag == other.m_tag;
        }
        bool operator!=(const PassiveTag &other) const {
            return !(*this == other);
        }

    private:
        friend class TagIndex;

        bool owns_ref() const {
            return m_string_pool;
        }

        void commit() {
            m_string_pool = nullptr;
        }

        db0::TagAddress owned_string() const {
            return m_owned_string;
        }

        void release() {
            if (m_string_pool) {
                assert(!is_long());
                assert(m_owned_string.isValid());
                m_string_pool->unRefByAddr(m_owned_string);
                m_string_pool = nullptr;
            }
        }
    };

}
