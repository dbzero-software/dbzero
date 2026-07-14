// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Diff_IO.hpp"
#include <dbzero/core/serialization/packed_int_pair.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/memory/config.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_diff_header: public o_fixed<o_diff_header>
    {
        // the number of objects contained
        std::uint16_t m_size = 0;
        // offset of the first valid object
        // (bytes before offset can be taken by remnants of the object from the previous page)
        std::uint16_t m_offset = 0;
    };
DB0_PACKED_END    
    
    class DiffWriter
    {
    public:
        // buffer is 2 pages long
        DiffWriter(Page_IO &, std::byte *begin, std::byte *end);
        
        // Append as o_diff_buffer object, if overflow occurs then
        // remainig contents needs to be written to the next (+1) storage page
        // @return false if append unsuccessful (must be appended to next page)
        bool append(const std::byte *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
            const std::vector<std::uint16_t> &diff_data, bool &overflow);
        
        // Flush all buffered contents
        // @return the number of bytes written
        std::size_t flush();

        // Flush current page with the Page_IO and handle overflow data if such exists
        // only flushed if there's been contents written
        // @return the number of bytes written            
        std::size_t flushDP();

        // Revert the last append operation
        void revert();

        // check if a full-page worth of data has been written
        bool isFull() const;

        bool empty() const;

    private:
        Page_IO &m_page_io;
        std::byte * const m_begin;
        std::byte *m_current;
        std::byte const *m_end;
        const std::uint32_t m_page_size;
        // current page's header
        o_diff_header &m_header;
        std::uint32_t m_last_size = 0;
    };

    class DiffReader
    {
    public:
        // buffer is 2 pages long
        DiffReader(Page_IO &, std::uint64_t page_num, std::byte *begin, std::byte *end);
        
        // appy diffs from a specific page / state number into a provided data buffer
        // if underflow occurs then next page needs to be fetched and apply repeated
        bool apply(std::byte *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state, 
            bool &underflow);

        // Load continued data from the next page
        void loadNext();
    
    private:
        Page_IO &m_page_io;
        const std::uint32_t m_page_size;
        const std::uint64_t m_page_num;
        std::byte * const m_begin;
        const std::byte *m_current;
        std::byte const *m_end;
        // the number of objects remaining to be read
        unsigned int m_size = 0;        
    };
    
    DiffWriter::DiffWriter(Page_IO &page_io, std::byte *begin, std::byte *end)
        : m_page_io(page_io)
        , m_begin(begin)
        , m_current(begin)
        , m_end(end)
        , m_page_size(page_io.getPageSize())
        , m_header(o_diff_header::__new(m_current))
    {
        m_current += m_header.sizeOf();
    }
    
    bool DiffWriter::append(const std::byte *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
        const std::vector<std::uint16_t> &diff_data, bool &overflow)
    {
        using PairT = o_packed_int_pair<std::uint64_t, std::uint32_t>;
        assert(m_current + o_diff_buffer::measure(dp_data, diff_data) + PairT::measure(page_and_state) <= m_end);
        auto begin = m_current;
        PairT::write(m_current, page_and_state);
        if (m_current + o_diff_buffer::sizeOfHeader() > m_begin + m_page_size) {
            // unable to fit headers onto current page, revert
            m_current = begin;
            return false;
        }
        auto &diff_buf = o_diff_buffer::__new(m_current, dp_data, diff_data);
        m_current += diff_buf.sizeOf();
        assert(m_current <= m_end);
        m_last_size = m_current - begin;
        ++m_header.m_size;
        // overflows a single DP
        overflow = m_current > (m_begin + m_page_size);
        return true;
    }

    std::size_t DiffWriter::flush()
    {
        std::size_t result = 0;
        while (!empty()) {
            result += flushDP();
        }
        return result;
    }

    std::size_t DiffWriter::flushDP()
    {
        if (empty()) {
            return 0;
        }
        
        m_page_io.append(m_begin);
        m_header.m_size = 0;
        // handle overflowed contents if such exists
        if (m_current > (m_begin + m_page_size)) {
            // offset is equal number of overflowed bytes
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

    void DiffWriter::revert()
    {
        assert(m_header.m_size > 0);
        assert(m_current - m_last_size >= m_begin);
        --m_header.m_size;
        m_current -= m_last_size;        
    }

    bool DiffWriter::isFull() const {
        return m_current >= (m_begin + m_page_size);
    }

    bool DiffWriter::empty() const {
        return m_header.m_size == 0 && m_header.m_offset == 0;
    }
    
    DiffReader::DiffReader(Page_IO &page_io, std::uint64_t page_num, std::byte *begin, std::byte *end)
        : m_page_io(page_io)
        , m_page_size(page_io.getPageSize())
        , m_page_num(page_num)
        , m_begin(begin)
        , m_current(begin + m_page_size)
        , m_end(end)        
    {
        page_io.read(page_num, m_begin + m_page_size);
        m_size = o_diff_header::__const_ref(m_current).m_size;
        // position at the first diff block
        m_current += o_diff_header::sizeOf() + o_diff_header::__const_ref(m_current).m_offset;
        if (m_current > m_end) {
            Settings::m_decode_error();   
        }
    }
    
    bool DiffReader::apply(std::byte *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
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
                    // need to handle the underflow
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
        // unable to locate the diff block
        return false;
    }
    
    void DiffReader::loadNext()
    {
        assert(m_current >= (m_begin + m_page_size));
        // move underflown contents
        auto offset = m_current - (m_begin + m_page_size);
        auto size = m_end - m_current;
        std::memcpy(m_begin + offset, m_current, size);
        m_current = m_begin + offset;
        // read the next page
        m_page_io.read(m_page_num + 1, m_begin + m_page_size);
        // and merge neighboring parts of the diff block (note that header gets overwritten)
        std::memmove((void*)(m_current + o_diff_header::sizeOf()), m_current, size);
        m_current += o_diff_header::sizeOf();
    }
    
    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, 
        std::uint32_t block_size, std::uint64_t address, std::uint32_t page_count, std::uint32_t step_size, 
        std::function<std::uint64_t()> tail_function, std::optional<std::uint32_t> block_num)
        : Page_IO(header_size, file, page_size, block_size, address, page_count, step_size, tail_function, block_num)
        , m_write_buf(page_size * 2)
        , m_read_buf(page_size * 2)
        , m_writer(std::make_unique<DiffWriter>(
            reinterpret_cast<Page_IO&>(*this), m_write_buf.data(), m_write_buf.data() + m_write_buf.size())
        )
    {
    }
    
    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size)
        : Page_IO(header_size, file, page_size)
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
        for (;;) {
            if (m_writer->isFull()) {
                m_diff_bytes_written += m_writer->flushDP();
            }
            bool overflow = false;
            auto next_page_num = Page_IO::getNextPageNum(is_first_page);
            assert(next_page_num.second > 0);
            if (is_first_page) {
                // Must be first write into the first page (of the step)
                // to report result as the is_first_page = true
                *is_first_page &= m_writer->empty();
            }
            if (m_writer->append((const std::byte*)dp_data, page_and_state, diff_data, overflow)) {
                if (overflow) {
                    // on overflow we can either append remnants to the next storage page (+1)
                    // if such is available or revert the append and try again with a fresh buffer
                    if (next_page_num.second > 1) {
                        // flush with the Page_IO
                        m_diff_bytes_written += m_writer->flushDP();
                    } else {
                        m_writer->revert();
                        m_diff_bytes_written += m_writer->flushDP();
                        // continue with a fresh buffer
                        continue;
                    }
                }
                return { next_page_num.first, overflow };
            } else {
                // continue with a fresh buffer                
                m_diff_bytes_written += m_writer->flushDP();
                continue;
            }
        }
    }
    
    void Diff_IO::applyFrom(std::uint64_t page_num, void *buffer,
        std::pair<std::uint64_t, std::uint32_t> page_and_state) const
    {
        // must lock because the read-buffer is shared
        std::unique_lock<std::mutex> lock(m_mx_read);
        DiffReader reader((Page_IO&)*this, page_num, m_read_buf.data(), m_read_buf.data() + m_read_buf.size());
        for (;;) {
            bool underflow = false;
            if (reader.apply((std::byte*)buffer, page_and_state, underflow)) {
                return;
            }
            if (underflow) {
                // repeat after fetching the next page
                reader.loadNext();
                continue;
            }
            THROWF(db0::InternalException) << "Diff block not found";
        }
    }
    
    void Diff_IO::flush()
    {
        std::unique_lock<std::mutex> lock(m_mx_write);
        if (m_writer) {
            m_diff_bytes_written += m_writer->flush();
        }
    }
    
    void Diff_IO::write(std::uint64_t page_num, void *buffer)
    {
        // full-DP write can only be performed after flushing from diff-writer
        std::unique_lock<std::mutex> lock(m_mx_write);
        if (m_writer) {
            m_diff_bytes_written += m_writer->flush();
        }
        Page_IO::write(page_num, buffer);
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
        return Page_IO::append(buffer, is_first_page_ptr);
    }
    
    std::pair<std::size_t, std::size_t> Diff_IO::getStats() const {
        return { m_full_dp_bytes_written + m_diff_bytes_written, m_diff_bytes_written };
    }

}