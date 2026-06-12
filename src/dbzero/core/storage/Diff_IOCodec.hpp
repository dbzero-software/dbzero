// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include "diff_buffer.hpp"
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/core/serialization/packed_int_pair.hpp>
#include <cassert>
#include <cstring>
#include <vector>

namespace db0::detail

{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_diff_io_codec_header: public o_fixed<o_diff_io_codec_header>
    {
        std::uint16_t m_size = 0;
        std::uint16_t m_offset = 0;
    };
DB0_PACKED_END

    template <typename AccessT>
    class DiffIOCodecWriter
    {
    public:
        DiffIOCodecWriter(AccessT &access, std::byte *begin, std::byte *end)
            : m_access(access)
            , m_begin(begin)
            , m_current(begin)
            , m_end(end)
            , m_page_size(access.getPageSize())
            , m_header(o_diff_io_codec_header::__new(m_current))
        {
            m_current += m_header.sizeOf();
        }

        bool append(const std::byte *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
            const std::vector<std::uint16_t> &diff_data, bool &overflow)
        {
            using PairT = o_packed_int_pair<std::uint64_t, std::uint32_t>;
            assert(m_current + o_diff_buffer::measure(dp_data, diff_data) + PairT::measure(page_and_state) <= m_end);
            auto begin = m_current;
            PairT::write(m_current, page_and_state);
            if (m_current + o_diff_buffer::sizeOfHeader() > m_begin + m_page_size) {
                m_current = begin;
                return false;
            }
            auto &diff_buf = o_diff_buffer::__new(m_current, dp_data, diff_data);
            m_current += diff_buf.sizeOf();
            assert(m_current <= m_end);
            m_last_size = m_current - begin;
            ++m_header.m_size;
            overflow = m_current > (m_begin + m_page_size);
            return true;
        }

        std::size_t flush()
        {
            std::size_t result = 0;
            while (!empty()) {
                result += flushDP();
            }
            return result;
        }

        std::size_t flushDP()
        {
            if (empty()) {
                return 0;
            }

            m_access.append(m_begin);
            m_header.m_size = 0;
            if (m_current > (m_begin + m_page_size)) {
                m_header.m_offset = m_current - m_begin - m_page_size;
                m_current = m_begin + m_header.sizeOf();
                std::memcpy(m_current, m_begin + m_page_size, m_header.m_offset);
                m_current += m_header.m_offset;
            } else {
                m_header.m_offset = 0;
                m_current = m_begin + m_header.sizeOf();
            }
            return m_page_size;
        }

        void revert()
        {
            assert(m_header.m_size > 0);
            assert(m_current - m_last_size >= m_begin);
            --m_header.m_size;
            m_current -= m_last_size;
        }

        bool isFull() const
        {
            return m_current >= (m_begin + m_page_size);
        }

        bool empty() const
        {
            return m_header.m_size == 0 && m_header.m_offset == 0;
        }

    private:
        AccessT &m_access;
        std::byte * const m_begin;
        std::byte *m_current;
        std::byte const *m_end;
        const std::uint32_t m_page_size;
        o_diff_io_codec_header &m_header;
        std::uint32_t m_last_size = 0;
    };

    template <typename AccessT>
    class DiffIOCodecReader
    {
    public:
        DiffIOCodecReader(const AccessT &access, std::uint64_t page_num, std::byte *begin, std::byte *end)
            : m_access(access)
            , m_page_size(access.getPageSize())
            , m_page_num(page_num)
            , m_begin(begin)
            , m_current(begin + m_page_size)
            , m_end(end)
        {
            m_access.read(page_num, m_begin + m_page_size);
            m_size = o_diff_io_codec_header::__const_ref(m_current).m_size;
            m_current += o_diff_io_codec_header::sizeOf()
                + o_diff_io_codec_header::__const_ref(m_current).m_offset;
            if (m_current > m_end) {
                Settings::m_decode_error();
            }
        }

        bool apply(std::byte *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
            bool &underflow)
        {
            using PairT = o_packed_int_pair<std::uint64_t, std::uint32_t>;
            while (m_size > 0) {
                auto revert_to = m_current;
                auto revert_to_size = m_size;
                auto next_page_and_state = PairT::read(m_current);
                auto diff_buf_size = o_diff_buffer::safeSizeOf(m_current);
                if (next_page_and_state == page_and_state) {
                    if (m_current + diff_buf_size > m_end) {
                        m_current = revert_to;
                        m_size = revert_to_size;
                        underflow = true;
                        return false;
                    }

                    auto &diff_buf = o_diff_buffer::__safe_const_ref(
                        const_bounded_buf_t(Settings::m_decode_error, m_current, m_end)
                    );
                    diff_buf.apply(dp_data, dp_data + m_page_size);
                    m_current += diff_buf_size;
                    --m_size;
                    return true;
                }
                m_current += diff_buf_size;
                --m_size;
            }
            return false;
        }

        void loadNext()
        {
            assert(m_current >= (m_begin + m_page_size));
            auto offset = m_current - (m_begin + m_page_size);
            auto size = m_end - m_current;
            std::memcpy(m_begin + offset, m_current, size);
            m_current = m_begin + offset;
            m_access.read(m_access.nextPageNum(m_page_num), m_begin + m_page_size);
            std::memmove((void*)(m_current + o_diff_io_codec_header::sizeOf()), m_current, size);
            m_current += o_diff_io_codec_header::sizeOf();
        }

    private:
        const AccessT &m_access;
        const std::uint32_t m_page_size;
        const std::uint64_t m_page_num;
        std::byte * const m_begin;
        const std::byte *m_current;
        std::byte const *m_end;
        unsigned int m_size = 0;
    };

    template <typename AccessT>
    std::pair<std::uint64_t, bool> appendDiff(AccessT &access, DiffIOCodecWriter<AccessT> &writer,
        const void *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
        const std::vector<std::uint16_t> &diff_data, bool *is_first_page, std::size_t *bytes_written = nullptr)
    {
        auto add_bytes = [bytes_written](std::size_t bytes) {
            if (bytes_written) {
                *bytes_written += bytes;
            }
        };
        for (;;) {
            if (writer.isFull()) {
                add_bytes(writer.flushDP());
            }
            bool overflow = false;
            auto next_page_num = access.getNextPageNum(is_first_page);
            assert(next_page_num.second > 0);
            if (is_first_page) {
                *is_first_page &= writer.empty();
            }
            if (writer.append((const std::byte*)dp_data, page_and_state, diff_data, overflow)) {
                if (overflow) {
                    if (next_page_num.second > 1) {
                        add_bytes(writer.flushDP());
                    } else {
                        writer.revert();
                        add_bytes(writer.flushDP());
                        continue;
                    }
                }
                return { next_page_num.first, overflow };
            }
            add_bytes(writer.flushDP());
        }
    }

    template <typename AccessT>
    void applyFrom(const AccessT &access, std::uint64_t page_num, void *buffer,
        std::pair<std::uint64_t, std::uint32_t> page_and_state, const char *error_context,
        std::byte *read_begin, std::byte *read_end)
    {
        DiffIOCodecReader<AccessT> reader(access, page_num, read_begin, read_end);
        for (;;) {
            bool underflow = false;
            if (reader.apply((std::byte*)buffer, page_and_state, underflow)) {
                return;
            }
            if (underflow) {
                reader.loadNext();
                continue;
            }
            THROWF(db0::IOException) << error_context << ": storage_page_num=" << page_num
                << ", page_num=" << page_and_state.first << ", state_num=" << page_and_state.second;
        }
    }

}
