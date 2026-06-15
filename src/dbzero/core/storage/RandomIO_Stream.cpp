// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include "RandomIO_Stream.hpp"
#include "Diff_IOCodec.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <algorithm>
#include <cassert>
#include <cstring>

namespace db0

{

    namespace
    {

        struct RandomIOStreamControlPage
        {
            static constexpr std::uint64_t MAGIC = 0x44423052494f5354ULL; // "DB0RIOST"
            static constexpr std::uint32_t VERSION = 1;

            std::uint64_t m_magic;
            std::uint32_t m_version;
            std::uint32_t m_generation;
            std::uint32_t m_type;
            std::uint32_t m_control_index;
            std::uint32_t m_first_data_is_first_page;
            std::uint64_t m_next_chunk_page_num;
        };

        constexpr std::uint32_t CONTROL_END = 1;
        constexpr std::uint32_t CONTROL_LINK = 2;

        std::uint32_t calcPageRatio(std::uint32_t page_size, std::uint32_t underlying_page_size)
        {
            if (page_size < underlying_page_size || page_size % underlying_page_size != 0) {
                THROWF(db0::InternalException)
                    << "RandomIO_Stream page size must be a multiple of the underlying page size";
            }
            return page_size / underlying_page_size;
        }

        std::uint32_t getDataPagesPerChunk(std::uint32_t stride, std::uint32_t page_ratio)
        {
            if (stride < page_ratio + 1) {
                THROWF(db0::InternalException)
                    << "RandomIO_Stream stride must fit at least one data page and one control page";
            }
            return (stride - 1) / page_ratio;
        }

        bool isControlPage(const RandomIOStreamControlPage &control, std::uint32_t generation,
            std::uint32_t max_control_index)
        {
            if (control.m_magic != RandomIOStreamControlPage::MAGIC
                || control.m_version != RandomIOStreamControlPage::VERSION) {
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

    class RandomIO_Stream::CodecAccess
    {
    public:
        explicit CodecAccess(RandomIO_Stream &stream)
            : m_stream(stream)
        {
        }

        std::uint32_t getPageSize() const { return m_stream.getPageSize(); }
        std::pair<std::uint64_t, std::uint32_t> getNextPageNum(bool *is_first_page)
        {
            return m_stream.getNextPageNum(is_first_page);
        }
        std::uint64_t append(const void *buffer) { return m_stream.append(buffer); }
        void read(std::uint64_t page_num, void *buffer) const { m_stream.readRandom(page_num, buffer); }
        std::uint64_t nextPageNum(std::uint64_t page_num) const
        {
            return page_num + m_stream.m_page_ratio;
        }

    private:
        RandomIO_Stream &m_stream;
    };

    class RandomIO_Stream::ConstCodecAccess
    {
    public:
        explicit ConstCodecAccess(const RandomIO_Stream &stream)
            : m_stream(stream)
        {
        }

        std::uint32_t getPageSize() const { return m_stream.getPageSize(); }
        void read(std::uint64_t page_num, void *buffer) const { m_stream.readRandom(page_num, buffer); }
        std::uint64_t nextPageNum(std::uint64_t page_num) const
        {
            return page_num + m_stream.m_page_ratio;
        }

    private:
        const RandomIO_Stream &m_stream;
    };

    RandomIO_Stream::RandomIO_Stream(Diff_IO &page_io, std::uint32_t stride, std::uint32_t page_size)
        : m_page_io(page_io)
        , m_access_type(AccessType::READ_WRITE)
        , m_stride(stride)
        , m_page_size(page_size ? page_size : page_io.getPageSize())
        , m_page_ratio(calcPageRatio(m_page_size, page_io.getPageSize()))
        , m_data_pages_per_chunk(getDataPagesPerChunk(stride, m_page_ratio))
        , m_write_buf(m_page_size * 2)
        , m_read_buf(m_page_size * 2)
        , m_control_buf(page_io.getPageSize())
    {
        if (sizeof(RandomIOStreamControlPage) > m_page_io.getPageSize()) {
            THROWF(db0::InternalException) << "RandomIO_Stream control page does not fit into a page";
        }

        allocateFirstChunk();
    }

    RandomIO_Stream::RandomIO_Stream(Diff_IO &page_io, std::uint64_t page_num, std::uint32_t stride,
        AccessType access_type, std::uint32_t page_size)
        : m_page_io(page_io)
        , m_access_type(access_type)
        , m_stride(stride)
        , m_page_size(page_size ? page_size : page_io.getPageSize())
        , m_page_ratio(calcPageRatio(m_page_size, page_io.getPageSize()))
        , m_data_pages_per_chunk(getDataPagesPerChunk(stride, m_page_ratio))
        , m_write_buf(m_page_size * 2)
        , m_read_buf(m_page_size * 2)
        , m_control_buf(page_io.getPageSize())
    {
        if (sizeof(RandomIOStreamControlPage) > m_page_io.getPageSize()) {
            THROWF(db0::InternalException) << "RandomIO_Stream control page does not fit into a page";
        }

        openExisting(page_num);
    }

    void RandomIO_Stream::openExisting(std::uint64_t page_num)
    {
        m_head_page_num = page_num;
        // in read-only mode we don't allow stream access, just the random one
        if (m_access_type == AccessType::READ_ONLY) {
            return;
        }

        if (page_num >= m_page_io.getEndPageNum()) {
            THROWF(db0::InternalException) << "RandomIO_Stream does not exist";
        }
        
        std::uint64_t chunk_page_num = page_num;
        while (true) {
            std::uint32_t type = 0;
            std::uint32_t control_index = 0;
            std::uint64_t next_chunk_page_num = 0;
            bool first_data_is_first_page = false;
            if (!findControl(chunk_page_num, m_generation, type, control_index, next_chunk_page_num,
                first_data_is_first_page)) {
                THROWF(db0::InternalException) << "RandomIO_Stream control page not found";
            }

            m_current_chunk_page_num = chunk_page_num;
            m_current_used_pages = control_index;
            m_current_next_chunk_page_num = 0;
            m_current_first_data_is_first_page = first_data_is_first_page;

            if (type != CONTROL_LINK) {
                break;
            }

            m_current_next_chunk_page_num = next_chunk_page_num;
            chunk_page_num = next_chunk_page_num;
        }
    }

    std::pair<std::uint64_t, bool> RandomIO_Stream::appendDiff(
        const void *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
        const std::vector<std::uint16_t> &diff_data, bool *is_first_page)
    {
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::AccessTypeException) << "RandomIO_Stream::appendDiff not allowed in read-only mode";
        }
        CodecAccess access(*this);
        detail::DiffIOCodecWriter<CodecAccess> writer(
            access, m_write_buf.data(), m_write_buf.data() + m_write_buf.size());
        auto result = detail::appendDiff(access, writer, dp_data, page_and_state, diff_data, is_first_page);
        writer.flush();        
        m_modified = true;
        return result;
    }
    
    void RandomIO_Stream::applyFrom(std::uint64_t page_num, void *buffer,
        std::pair<std::uint64_t, std::uint32_t> page_and_state) const
    {
        ConstCodecAccess access(*this);
        detail::applyFrom(access, page_num, buffer, page_and_state, "RandomIO_Stream diff block not found",
            m_read_buf.data(), m_read_buf.data() + m_read_buf.size());
    }

    std::uint64_t RandomIO_Stream::append(const void *buffer, bool *is_first_page)
    {
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::AccessTypeException) << "RandomIO_Stream::append not allowed in read-only mode";
        }
        auto [page_num, remaining_pages] = getNextPageNum(is_first_page);
        assert(remaining_pages > 0);

        writeRandom(page_num, buffer);
        ++m_current_used_pages;
        m_modified = true;
        return page_num;
    }

    void RandomIO_Stream::readRandom(std::uint64_t page_num, void *buffer) const
    {
        static_cast<const Page_IO &>(m_page_io).read(page_num, buffer, m_page_ratio);
    }

    std::uint64_t RandomIO_Stream::appendRandom(const void *buffer)
    {
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::AccessTypeException) << "RandomIO_Stream::appendRandom not allowed in read-only mode";
        }
        m_modified = true;
        auto page_num = m_page_io.reserve(m_page_ratio);
        writeRandom(page_num, buffer);
        return page_num;
    }

    void RandomIO_Stream::writeRandom(std::uint64_t page_num, const void *buffer)
    {
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::AccessTypeException) << "RandomIO_Stream::writeRandom not allowed in read-only mode";
        }
        const std::byte *byte_buffer = static_cast<const std::byte *>(buffer);
        auto underlying_page_size = m_page_io.getPageSize();
        for (std::uint32_t i = 0; i < m_page_ratio; ++i) {
            static_cast<Page_IO &>(m_page_io).write(page_num + i, byte_buffer + i * underlying_page_size);
        }
        m_modified = true;
    }

    void RandomIO_Stream::flush()
    {
        if (m_access_type == AccessType::READ_ONLY) {
            return;
        }
        if (!m_modified) {
            return;
        }
        writeCurrentControl(CONTROL_END, m_current_used_pages);
        m_modified = false;
    }

    void RandomIO_Stream::close()
    {
        flush();
    }

    void RandomIO_Stream::clear()
    {
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::AccessTypeException) << "RandomIO_Stream::clear not allowed in read-only mode";
        }
        ++m_generation;
        loadNextChunk(m_head_page_num);
        m_modified = true;
        flush();
    }

    std::pair<std::uint64_t, std::uint32_t> RandomIO_Stream::getNextPageNum(bool *is_first_page)
    {
        while (m_current_used_pages == m_data_pages_per_chunk) {
            advanceChunk();
        }

        if (is_first_page) {
            *is_first_page = m_current_used_pages == 0 && m_current_first_data_is_first_page;
        }

        return {
            dataPageNum(m_current_chunk_page_num, m_current_used_pages),
            m_data_pages_per_chunk - m_current_used_pages
        };
    }

    std::uint64_t RandomIO_Stream::getPageNum() const
    {
        return m_head_page_num;
    }

    std::uint32_t RandomIO_Stream::getPageSize() const
    {
        return m_page_size;
    }

    std::uint64_t RandomIO_Stream::getHeadPageNum() const
    {
        return m_head_page_num;
    }

    bool RandomIO_Stream::modified() const
    {
        return m_modified;
    }

    void RandomIO_Stream::advanceChunk()
    {
        if (!m_current_next_chunk_page_num) {
            allocateNextChunk();
        } else {
            writeCurrentControl(CONTROL_LINK, m_current_used_pages, m_current_next_chunk_page_num);
            loadNextChunk(m_current_next_chunk_page_num);
        }
    }

    void RandomIO_Stream::allocateFirstChunk()
    {
        bool is_first_page = false;
        m_current_chunk_page_num = m_page_io.reserve(m_stride, &is_first_page);
        m_head_page_num = m_current_chunk_page_num;
        m_current_next_chunk_page_num = 0;
        m_current_used_pages = 0;
        m_current_first_data_is_first_page = is_first_page;
        m_modified = true;
    }

    void RandomIO_Stream::allocateNextChunk()
    {
        bool is_first_page = false;
        auto next_chunk_page_num = m_page_io.reserve(m_stride, &is_first_page);

        m_current_next_chunk_page_num = next_chunk_page_num;
        writeCurrentControl(CONTROL_LINK, m_current_used_pages, next_chunk_page_num);

        m_current_chunk_page_num = next_chunk_page_num;
        m_current_next_chunk_page_num = 0;
        m_current_used_pages = 0;
        m_current_first_data_is_first_page = is_first_page;
    }

    void RandomIO_Stream::loadNextChunk(std::uint64_t page_num)
    {
        m_current_chunk_page_num = page_num;
        m_current_next_chunk_page_num = 0;
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

        m_current_first_data_is_first_page = old_first_data_is_first_page;
        if (old_type == CONTROL_LINK) {
            m_current_next_chunk_page_num = old_next_chunk_page_num;
        }
    }

    std::uint64_t RandomIO_Stream::controlPageNum(std::uint64_t chunk_page_num,
        std::uint32_t control_index) const
    {
        return chunk_page_num + control_index * m_page_ratio;
    }

    std::uint64_t RandomIO_Stream::dataPageNum(std::uint64_t chunk_page_num, std::uint32_t page_index) const
    {
        return chunk_page_num + page_index * m_page_ratio;
    }

    void RandomIO_Stream::writeCurrentControl(std::uint32_t type, std::uint32_t control_index,
        std::uint64_t next_chunk_page_num)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        assert(control_index <= m_data_pages_per_chunk);
        RandomIOStreamControlPage control = {
            RandomIOStreamControlPage::MAGIC,
            RandomIOStreamControlPage::VERSION,
            m_generation,
            type,
            control_index,
            m_current_first_data_is_first_page ? 1u : 0u,
            next_chunk_page_num
        };
        std::fill(m_control_buf.begin(), m_control_buf.end(), std::byte{0});
        std::memcpy(m_control_buf.data(), &control, sizeof(control));
        static_cast<Page_IO &>(m_page_io).write(controlPageNum(m_current_chunk_page_num, control_index),
            m_control_buf.data());
    }

    bool RandomIO_Stream::findControl(std::uint64_t chunk_page_num, std::uint32_t generation,
        std::uint32_t &type, std::uint32_t &control_index, std::uint64_t &next_chunk_page_num,
        bool &first_data_is_first_page) const
    {
        RandomIOStreamControlPage control = {};
        for (std::uint32_t index = 0; index <= m_data_pages_per_chunk; ++index) {
            static_cast<const Page_IO &>(m_page_io).read(controlPageNum(chunk_page_num, index),
                m_control_buf.data());
            std::memcpy(&control, m_control_buf.data(), sizeof(control));
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

}
