#include "Diff_IO.hpp"

namespace db0

{

    struct [[gnu::packed]] o_diff_header: public o_fixed<o_diff_header>
    {
        // the number of objects contained
        std::uint16_t m_size = 0;
        // offset of the first valid object
        // (bytes before offset can be taken by remnants of the object from the previous page)
        std::uint16_t m_offset = 0;
    };
    
    class DiffWriter
    {
    public:
        // buffer is 2 pages long
        DiffWriter(Page_IO &, std::byte *begin, std::byte *end);
        
        // Append as o_diff_buffer object, if overflow occurs then
        // remainig contents needs to be written to the next (+1) storage page
        void append(const std::byte *dp_data, const std::vector<std::uint16_t> &diff_data,
            bool &overflow);
        
        // Flush current page with the Page_IO and handle overflow data if such exists
        // only flushed if there's been contents written
        void flush();
        
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
    
    void DiffWriter::append(const std::byte *dp_data, const std::vector<std::uint16_t> &diff_data, bool &overflow)
    {
        assert(m_current + o_diff_buffer::measure(dp_data, diff_data) <= m_end);
        auto &diff_buf = o_diff_buffer::__new(m_current, dp_data, diff_data);
        m_last_size = diff_buf.sizeOf();
        m_current += m_last_size;
        ++m_header.m_size;
        // overflows a single DP
        overflow = m_current > (m_begin + m_page_size);
    }

    void DiffWriter::flush()
    {
        if (empty()) {
            return;
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

    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address,
        std::uint32_t page_count, std::function<std::uint64_t()> tail_function)
        : Page_IO(header_size, file, page_size, block_size, address, page_count, tail_function)
        , m_data_buf(page_size * 2)
        , m_writer(std::make_unique<DiffWriter>(
            reinterpret_cast<Page_IO&>(*this), m_data_buf.data(), m_data_buf.data() + m_data_buf.size()))
    {
    }
    
    Diff_IO::Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size)
        : Page_IO(header_size, file, page_size)    
    {
    }
    
    std::uint64_t Diff_IO::append(const std::byte *dp_data, const std::vector<std::uint16_t> &diff_data)
    {
        assert(m_writer);
        for (;;) {
            if (m_writer->isFull()) {
                m_writer->flush();
            }
            bool overflow = false;
            auto next_page_num = Page_IO::getNextPageNum();
            assert(next_page_num.second > 0);
            m_writer->append(dp_data, diff_data, overflow);
            if (overflow) {
                // on overflow we can either append remnants to the next storage page (+1)
                // if such is available or revert the append and try again with a fresh buffer
                if (next_page_num.second > 1) {
                    // flush with the Page_IO
                    m_writer->flush();
                } else {
                    m_writer->revert();
                    m_writer->flush();
                    // continue wita fresh buffer
                    continue;
                }
            }
            return next_page_num.first;
        }
    }
    
    std::uint64_t Diff_IO::tail() const {
        return Page_IO::tail();
    }

    std::uint32_t Diff_IO::getPageSize() const {
        return Page_IO::getPageSize();
    }

}