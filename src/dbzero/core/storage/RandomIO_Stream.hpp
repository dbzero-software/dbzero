// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#pragma once

#include "Diff_IO.hpp"
#include "diff_buffer.hpp"
#include <cstdint>
#include <utility>
#include <vector>

namespace db0

{

    /**
     * RandomIO_Stream exposes stream-style append/read iteration and random page
     * access on top of a shared Diff_IO/Page_IO store.
     *
     * The stream is identified by an externally stored head page number and
     * stride. Data pages use absolute underlying Page_IO page numbers, so
     * multiple streams can coexist in one Diff_IO without changing address
     * semantics. The logical stream page size may be larger than the underlying
     * Page_IO page size; in that case one logical page is translated to a
     * contiguous group of underlying pages.
     *
     * clear() marks the stream empty by writing a new control sentinel and keeps
     * previously allocated chunks linked so later appends can reuse them.
     */
    class RandomIO_Stream
    {
    public:
        class Reader;

        /**
         * @param page_io shared underlying page store used for all reads/writes        
         * @param stride number of underlying Page_IO pages reserved per stream chunk
         * @param page_size logical stream page size in bytes; defaults to the
         * underlying Page_IO page size and must be its exact multiple
         */
        RandomIO_Stream(Diff_IO &page_io, std::uint32_t stride, std::uint32_t page_size = 0);

        // Open existing stream from a known location (page_num)
        RandomIO_Stream(Diff_IO &page_io, std::uint64_t page_num, std::uint32_t stride,
            std::uint32_t page_size = 0);

        /**
         * Append/read data through the managed RandomIO stream.
         *
         * append() stores a full logical page at the stream cursor, advances the
         * stream, and makes the page visible to Reader. appendDiff() does the
         * same for a diff block encoded against page_and_state. applyFrom()
         * resolves a diff block by walking this stream's managed page chain.
         *
         * These methods differ from appendRandom/readRandom/writeRandom: random
         * access methods operate on absolute underlying Diff_IO page numbers and
         * do not update stream membership or cursor state.
         */
        std::pair<std::uint64_t, bool> appendDiff(const void *dp_data,
            std::pair<std::uint64_t, std::uint32_t> page_and_state,
            const std::vector<std::uint16_t> &diff_data, bool *is_first_page = nullptr);
        void applyFrom(std::uint64_t page_num, void *buffer,
            std::pair<std::uint64_t, std::uint32_t> page_and_state) const;
        std::uint64_t append(const void *buffer, bool *is_first_page = nullptr);

        /**
         * Append/read/write absolute page locations in the underlying Diff_IO store.
         *
         * These methods do not consult or update the managed stream cursor and
         * do not make the page visible to Reader. They are intentionally random
         * access operations over the shared backing store; clear() only changes
         * stream membership and does not invalidate unrelated random locations.
         * readRandom() can also read an absolute page number returned by stream
         * append operations such as append() or appendDiff().
         */
        std::uint64_t appendRandom(const void *buffer);
        void readRandom(std::uint64_t page_num, void *buffer) const;
        void writeRandom(std::uint64_t page_num, const void *buffer);

        std::uint32_t getPageSize() const;

        std::uint64_t getHeadPageNum() const;

        bool modified() const;
        
        void flush();
        void close();

        // Clear the stream part only
        void clear();

        Reader getReader() const;

    protected:
        std::uint64_t getPageNum() const;

    private:
        class CodecAccess;
        class ConstCodecAccess;

        Diff_IO &m_page_io;
        const std::uint32_t m_stride;
        const std::uint32_t m_page_size;
        const std::uint32_t m_page_ratio;
        const std::uint32_t m_data_pages_per_chunk;
        std::vector<std::byte> m_write_buf;
        mutable std::vector<std::byte> m_read_buf;
        mutable std::vector<std::byte> m_control_buf;
        std::uint64_t m_head_page_num = 0;
        std::uint64_t m_current_chunk_page_num = 0;
        std::uint64_t m_current_next_chunk_page_num = 0;
        std::uint32_t m_current_used_pages = 0;
        std::uint32_t m_generation = 1;
        bool m_current_first_data_is_first_page = false;
        bool m_modified = false;

        std::pair<std::uint64_t, std::uint32_t> getNextPageNum(bool *is_first_page = nullptr);
        void advanceChunk();
        void allocateFirstChunk();
        void allocateNextChunk();
        void openExisting(std::uint64_t page_num);
        void loadNextChunk(std::uint64_t page_num);
        std::uint64_t controlPageNum(std::uint64_t chunk_page_num, std::uint32_t control_index) const;
        std::uint64_t dataPageNum(std::uint64_t chunk_page_num, std::uint32_t page_index) const;
        void writeCurrentControl(std::uint32_t type, std::uint32_t control_index,
            std::uint64_t next_chunk_page_num = 0);
        bool findControl(std::uint64_t chunk_page_num, std::uint32_t generation,
            std::uint32_t &type, std::uint32_t &control_index, std::uint64_t &next_chunk_page_num,
            bool &first_data_is_first_page) const;
    };

    class RandomIO_Stream::Reader
    {
    public:
        explicit Reader(const RandomIO_Stream &);

        bool readNext(void *buffer, std::uint64_t *page_num = nullptr);

    private:
        const RandomIO_Stream &m_stream;
        std::uint64_t m_chunk_page_num = 0;
        std::uint32_t m_page_index = 0;
        std::uint32_t m_used_pages = 0;
        std::uint64_t m_next_chunk_page_num = 0;
        bool m_end = true;

        void loadChunk(std::uint64_t page_num);
    };

}
