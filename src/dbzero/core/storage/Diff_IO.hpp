// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Diff_IOCodec.hpp"
#include "Page_IO.hpp"
#include <memory>

namespace db0

{

    // Diff_IO is a Page_IO extension specialized in
    // storage & retrieval of diff sequences
    class Diff_IO: public Page_IO
    {
    public:
        Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address, 
            std::uint32_t page_count, std::uint32_t step_size, std::function<std::uint64_t()> tail_function, 
            std::optional<std::uint32_t> block_num = {});
        // Read-only Diff_IO
        Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size);
        ~Diff_IO();
        
        // Appends a new diff-block to the stream
        // NOTE: that the diff-block may be stored on 2 pages in which case the number of the first one is returned
        // and the continuation page number will be stored in the page header (continuation page number)
        // @param dp_data the data page buffer
        // @param page_and_state relative logical page and state numbers to mark the diff block
        // @param diff_data the diff buffer (see getDiffs)
        // @return page number + overflow flag (where "true" means that 2 pages were written to)
        std::pair<std::uint64_t, bool> appendDiff(const void *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
            const std::vector<std::uint16_t> &diff_data, bool *is_first_page = nullptr);
        
        // Read diff stream and apply changes to the DP-buffer (must be already populated with the base data)
        // @param page_num the storage page number to read from
        // @param buffer the buffer to hold the resulting data page
        // @param page_and_state logical page and state numbers (possibly relative) to identify the diff block
        // Exception raised if the diff block is not found
        void applyFrom(std::uint64_t page_num, void *buffer, std::pair<std::uint64_t, std::uint32_t> page_and_state) const;
        
        // Flush needs to be called before closing the stream
        // and after each transaction
        void flush();

        bool modified() const;
        
        // Write as full-DP
        void write(std::uint64_t page_num, void *buffer);
        
        std::uint64_t append(const void *buffer, bool *is_first_page = nullptr);

        std::uint64_t reserve(std::uint32_t page_count, bool *is_first_page = nullptr);

        void read(std::uint64_t page_num, void *buffer) const;
        
        // @return total bytes written/ diff bytes written
        std::pair<std::size_t, std::size_t> getStats() const;

    protected:
        class CodecAccess
        {
        public:
            explicit CodecAccess(Page_IO &page_io)
                : m_page_io(page_io)
            {
            }

            std::uint32_t getPageSize() const { return m_page_io.getPageSize(); }
            std::pair<std::uint64_t, std::uint32_t> getNextPageNum(bool *is_first_page)
            {
                return m_page_io.getNextPageNum(is_first_page);
            }
            std::uint64_t append(const void *buffer) { return m_page_io.append(buffer); }
            void read(std::uint64_t page_num, void *buffer) const { m_page_io.read(page_num, buffer); }
            std::uint64_t nextPageNum(std::uint64_t page_num) const { return page_num + 1; }

        private:
            Page_IO &m_page_io;
        };

        mutable std::mutex m_mx_write;
        CodecAccess m_codec_access;
        // the data buffer to hold up to 2 data pages
        std::vector<std::byte> m_write_buf;
        mutable std::mutex m_mx_read;
        mutable std::vector<std::byte> m_read_buf;
        std::unique_ptr<detail::DiffIOCodecWriter<CodecAccess>> m_writer;
        // total bytes written to the stream (since class creation) using full-DP method
        std::size_t m_full_dp_bytes_written = 0;
        // total bytes written using the diff mechanism
        std::size_t m_diff_bytes_written = 0;
        bool m_modified = false;
    };
    
}
