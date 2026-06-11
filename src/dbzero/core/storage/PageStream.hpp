// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Page_IO.hpp"
#include <cstdint>
#include <optional>

namespace db0

{

    class Diff_IO;

    class PageStream
    {
    public:
        class Reader;

        explicit PageStream(Page_IO &, std::uint32_t chunk_page_count = 64);

        std::uint64_t appendPage(const void *buffer, bool *is_first_page = nullptr);
        void flush();
        void close();
        void clear();
        void resetWriteCursor();

        Reader getReader() const;

    private:
        friend class Diff_IO;

        Page_IO &m_page_io;
        const std::uint32_t m_chunk_page_count;
        const std::uint32_t m_data_pages_per_chunk;
        std::optional<std::uint64_t> m_begin_chunk_page_num;
        std::uint64_t m_current_chunk_page_num = 0;
        std::uint64_t m_current_next_chunk_page_num = 0;
        std::uint32_t m_current_used_pages = 0;
        std::uint32_t m_current_reuse_pages = 0;
        std::uint32_t m_generation = 1;
        bool m_current_first_data_is_first_page = false;

        std::pair<std::uint64_t, std::uint32_t> getNextPageNum(bool *is_first_page = nullptr);
        void advanceChunk();
        void ensureWritableChunk();
        void allocateFirstChunk();
        void allocateNextChunk();
        void loadNextChunk(std::uint64_t page_num);
        void writeCurrentControl(std::uint32_t type, std::uint32_t control_index,
            std::uint64_t next_chunk_page_num = 0);
        bool findControl(std::uint64_t chunk_page_num, std::uint32_t generation,
            std::uint32_t &type, std::uint32_t &control_index, std::uint64_t &next_chunk_page_num,
            bool &first_data_is_first_page) const;
    };

    class PageStream::Reader
    {
    public:
        explicit Reader(const PageStream &);

        bool readNext(void *buffer, std::uint64_t *page_num = nullptr);

    private:
        const PageStream &m_stream;
        std::uint64_t m_chunk_page_num = 0;
        std::uint32_t m_page_index = 0;
        std::uint32_t m_used_pages = 0;
        std::uint64_t m_next_chunk_page_num = 0;
        bool m_end = true;

        void loadChunk(std::uint64_t page_num);
    };

}
