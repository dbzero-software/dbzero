// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PageStream.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <cassert>

namespace db0

{

    namespace
    {

        struct ControlPage
        {
            static constexpr std::uint64_t MAGIC = 0x4442305053544354ULL; // "DB0PSTCT"
            static constexpr std::uint32_t VERSION = 1;

            std::uint64_t m_magic = MAGIC;
            std::uint32_t m_version = VERSION;
            std::uint32_t m_generation = 0;
            std::uint32_t m_type = 0;
            std::uint32_t m_control_index = 0;
            std::uint32_t m_first_data_is_first_page = 0;
            std::uint64_t m_next_chunk_page_num = 0;
        };

        constexpr std::uint32_t CONTROL_END = 1;
        constexpr std::uint32_t CONTROL_LINK = 2;

        bool isControlPage(const ControlPage &control, std::uint32_t generation,
            std::uint32_t max_control_index)
        {
            if (control.m_magic != ControlPage::MAGIC || control.m_version != ControlPage::VERSION) {
                return false;
            }
            if (control.m_generation != generation) {
                return false;
            }
            if (control.m_control_index > max_control_index) {
                return false;
            }
            return control.m_type == CONTROL_END || control.m_type == CONTROL_LINK;
        }

    }

    PageStream::PageStream(Page_IO &page_io, std::uint32_t chunk_page_count)
        : m_page_io(page_io)
        , m_chunk_page_count(chunk_page_count)
        , m_data_pages_per_chunk(chunk_page_count - 1)
    {
        if (chunk_page_count < 2) {
            THROWF(db0::InternalException) << "PageStream chunk must contain at least 2 pages";
        }
        if (sizeof(ControlPage) > page_io.getPageSize()) {
            THROWF(db0::InternalException) << "PageStream control page does not fit into a page";
        }
    }

    std::uint64_t PageStream::appendPage(const void *buffer, bool *is_first_page)
    {
        auto [page_num, remaining_pages] = getNextPageNum(is_first_page);
        assert(remaining_pages > 0);

        m_page_io.write(page_num, const_cast<void *>(buffer));
        ++m_current_used_pages;
        return page_num;
    }

    std::pair<std::uint64_t, std::uint32_t> PageStream::getNextPageNum(bool *is_first_page)
    {
        ensureWritableChunk();
        while (m_current_used_pages == m_data_pages_per_chunk) {
            advanceChunk();
        }

        if (is_first_page) {
            *is_first_page = m_current_used_pages == 0 && m_current_first_data_is_first_page;
        }

        return {
            m_current_chunk_page_num + m_current_used_pages,
            m_data_pages_per_chunk - m_current_used_pages
        };
    }

    void PageStream::advanceChunk()
    {
        ensureWritableChunk();
        if (!m_current_next_chunk_page_num) {
            allocateNextChunk();
        } else {
            writeCurrentControl(CONTROL_LINK, m_current_used_pages, m_current_next_chunk_page_num);
            loadNextChunk(m_current_next_chunk_page_num);
        }
    }

    void PageStream::flush()
    {
        if (!m_begin_chunk_page_num) {
            return;
        }
        writeCurrentControl(CONTROL_END, m_current_used_pages);
    }

    void PageStream::close()
    {
        flush();
    }

    void PageStream::clear()
    {
        if (!m_begin_chunk_page_num) {
            return;
        }
        ++m_generation;
        loadNextChunk(*m_begin_chunk_page_num);
        flush();
    }

    void PageStream::resetWriteCursor()
    {
        m_begin_chunk_page_num.reset();
        m_current_chunk_page_num = 0;
        m_current_next_chunk_page_num = 0;
        m_current_used_pages = 0;
        m_current_reuse_pages = 0;
        m_current_first_data_is_first_page = false;
    }

    PageStream::Reader PageStream::getReader() const
    {
        return Reader(*this);
    }

    void PageStream::ensureWritableChunk()
    {
        if (!m_begin_chunk_page_num) {
            allocateFirstChunk();
        }
    }

    void PageStream::allocateFirstChunk()
    {
        bool is_first_page = false;
        m_current_chunk_page_num = m_page_io.reserve(m_chunk_page_count, &is_first_page);
        m_begin_chunk_page_num = m_current_chunk_page_num;
        m_current_next_chunk_page_num = 0;
        m_current_used_pages = 0;
        m_current_reuse_pages = 0;
        m_current_first_data_is_first_page = is_first_page;
    }

    void PageStream::allocateNextChunk()
    {
        bool is_first_page = false;
        auto next_chunk_page_num = m_page_io.reserve(m_chunk_page_count, &is_first_page);

        m_current_next_chunk_page_num = next_chunk_page_num;
        writeCurrentControl(CONTROL_LINK, m_current_used_pages, next_chunk_page_num);

        m_current_chunk_page_num = next_chunk_page_num;
        m_current_next_chunk_page_num = 0;
        m_current_used_pages = 0;
        m_current_reuse_pages = 0;
        m_current_first_data_is_first_page = is_first_page;
    }

    void PageStream::loadNextChunk(std::uint64_t page_num)
    {
        m_current_chunk_page_num = page_num;
        m_current_next_chunk_page_num = 0;
        m_current_reuse_pages = 0;
        m_current_used_pages = 0;
        m_current_first_data_is_first_page = false;

        std::uint32_t old_type = 0;
        std::uint32_t old_control_index = 0;
        std::uint64_t old_next_chunk_page_num = 0;
        bool old_first_data_is_first_page = false;
        if (!findControl(page_num, m_generation - 1, old_type, old_control_index, old_next_chunk_page_num,
            old_first_data_is_first_page)) {
            return;
        }

        m_current_reuse_pages = old_control_index;
        m_current_first_data_is_first_page = old_first_data_is_first_page;
        if (old_type == CONTROL_LINK) {
            m_current_next_chunk_page_num = old_next_chunk_page_num;
        }
    }

    void PageStream::writeCurrentControl(std::uint32_t type, std::uint32_t control_index,
        std::uint64_t next_chunk_page_num)
    {
        assert(control_index <= m_data_pages_per_chunk);
        ControlPage control;
        control.m_generation = m_generation;
        control.m_type = type;
        control.m_control_index = control_index;
        control.m_first_data_is_first_page = m_current_first_data_is_first_page ? 1u : 0u;
        control.m_next_chunk_page_num = next_chunk_page_num;
        m_page_io.writePageOffset(m_current_chunk_page_num + control_index, 0, sizeof(ControlPage), &control);
    }

    bool PageStream::findControl(std::uint64_t chunk_page_num, std::uint32_t generation,
        std::uint32_t &type, std::uint32_t &control_index, std::uint64_t &next_chunk_page_num,
        bool &first_data_is_first_page) const
    {
        ControlPage control;
        for (std::uint32_t index = 0; index <= m_data_pages_per_chunk; ++index) {
            m_page_io.readPageOffset(chunk_page_num + index, 0, sizeof(ControlPage), &control);
            if (isControlPage(control, generation, m_data_pages_per_chunk)) {
                type = control.m_type;
                control_index = control.m_control_index;
                next_chunk_page_num = control.m_next_chunk_page_num;
                first_data_is_first_page = control.m_first_data_is_first_page != 0;
                return true;
            }
        }
        return false;
    }

    PageStream::Reader::Reader(const PageStream &stream)
        : m_stream(stream)
    {
        if (m_stream.m_begin_chunk_page_num) {
            loadChunk(*m_stream.m_begin_chunk_page_num);
        }
    }

    bool PageStream::Reader::readNext(void *buffer, std::uint64_t *page_num)
    {
        while (!m_end) {
            if (m_page_index < m_used_pages) {
                auto current_page_num = m_chunk_page_num + m_page_index;
                m_stream.m_page_io.read(current_page_num, buffer);
                if (page_num) {
                    *page_num = current_page_num;
                }
                ++m_page_index;
                return true;
            }
            if (!m_next_chunk_page_num) {
                m_end = true;
            } else {
                loadChunk(m_next_chunk_page_num);
            }
        }
        return false;
    }

    void PageStream::Reader::loadChunk(std::uint64_t page_num)
    {
        std::uint32_t type = 0;
        std::uint32_t control_index = 0;
        std::uint64_t next_chunk_page_num = 0;
        bool first_data_is_first_page = false;
        if (!m_stream.findControl(page_num, m_stream.m_generation, type, control_index, next_chunk_page_num,
            first_data_is_first_page)) {
            m_end = true;
            return;
        }

        m_chunk_page_num = page_num;
        m_page_index = 0;
        m_used_pages = control_index;
        m_next_chunk_page_num = 0;
        if (type == CONTROL_LINK) {
            m_next_chunk_page_num = next_chunk_page_num;
        }
        m_end = false;
    }

}
