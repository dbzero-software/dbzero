// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Diff_IO.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
    
    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, 
        std::uint32_t block_size, std::uint64_t address, std::uint32_t page_count, std::uint32_t step_size, 
        std::function<std::uint64_t()> tail_function, std::optional<std::uint32_t> block_num)
        : Page_IO(header_size, file, page_size, block_size, address, page_count, step_size, tail_function, block_num)
        , m_codec_access(reinterpret_cast<Page_IO&>(*this))
        , m_write_buf(page_size * 2)
        , m_read_buf(page_size * 2)
        , m_writer(std::make_unique<detail::DiffIOCodecWriter<CodecAccess>>(
            m_codec_access, m_write_buf.data(), m_write_buf.data() + m_write_buf.size())
        )
    {
    }
    
    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size)
        : Page_IO(header_size, file, page_size)
        , m_codec_access(reinterpret_cast<Page_IO&>(*this))
        , m_read_buf(page_size * 2)
    {
    }
    
    Diff_IO::~Diff_IO()
    {
    }
    
    std::pair<std::uint64_t, bool> Diff_IO::appendDiff(
        const void *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
        const std::vector<std::uint16_t> &diff_data, bool *is_first_page)
    {
        // must lock because the write-buffer is shared
        std::unique_lock<std::mutex> lock(m_mx_write);
        assert(m_writer);
        auto result = detail::appendDiff(m_codec_access, *m_writer, dp_data, page_and_state, diff_data,
            is_first_page, &m_diff_bytes_written);
        m_modified = true;
        return result;
    }
    
    void Diff_IO::applyFrom(std::uint64_t page_num, void *buffer,
        std::pair<std::uint64_t, std::uint32_t> page_and_state) const
    {
        // must lock because the read-buffer is shared
        std::unique_lock<std::mutex> lock(m_mx_read);
        detail::applyFrom(m_codec_access, page_num, buffer, page_and_state, "Diff block not found",
            m_read_buf.data(), m_read_buf.data() + m_read_buf.size());
    }
    
    void Diff_IO::flush()
    {
        std::unique_lock<std::mutex> lock(m_mx_write);
        if (m_writer) {
            m_diff_bytes_written += m_writer->flush();
        }
        m_modified = false;
    }

    bool Diff_IO::modified() const
    {
        return m_modified;
    }
    
    void Diff_IO::write(std::uint64_t page_num, void *buffer)
    {
        // full-DP write can only be performed after flushing from diff-writer
        std::unique_lock<std::mutex> lock(m_mx_write);
        if (m_writer) {
            m_diff_bytes_written += m_writer->flush();
        }
        Page_IO::write(page_num, buffer);
        m_modified = true;
    }

    void Diff_IO::read(std::uint64_t page_num, void *buffer) const
    {
        assert(!m_writer || m_writer->empty());
        Page_IO::read(page_num, buffer);
    }

    std::uint64_t Diff_IO::append(const void *buffer, bool *is_first_page_ptr)
    {
        // full-DP write can only be performed after flushing from diff-writer
        std::unique_lock<std::mutex> lock(m_mx_write);
        if (m_writer) {
            m_diff_bytes_written += m_writer->flush();
        }
        m_full_dp_bytes_written += m_page_size;
        m_modified = true;
        return Page_IO::append(buffer, is_first_page_ptr);
    }
    
    std::pair<std::size_t, std::size_t> Diff_IO::getStats() const {
        return { m_full_dp_bytes_written + m_diff_bytes_written, m_diff_bytes_written };
    }

}
