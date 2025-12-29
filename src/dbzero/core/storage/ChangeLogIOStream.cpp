// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ChangeLogIOStream.hpp"
#include <algorithm>

namespace db0

{

    template <typename o_change_log_t>
    ChangeLogIOStream<o_change_log_t>::ChangeLogIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
        std::function<std::uint64_t()> tail_function, AccessType access_type)
        // enable checksums by default
        : BlockIOStream(m_file, begin, block_size, tail_function, access_type, true)
    {
    }
    
    template <typename o_change_log_t>
    ChangeLogIOStream<o_change_log_t>::ChangeLogIOStream(BlockIOStream &&io_stream)
        : BlockIOStream(std::move(io_stream))
    {
    }
    
    template <typename o_change_log_t>
    const o_change_log_t *ChangeLogIOStream<o_change_log_t>::readChangeLogChunk(std::vector<char> &buffer)
    {
        if (!this->readChunk(buffer)) {
            return nullptr;
        }
        return m_last_change_log_ptr;
    }
    
    template <typename o_change_log_t>
    std::size_t ChangeLogIOStream<o_change_log_t>::readChunk(std::vector<char> &buffer, std::size_t expected_size, 
        std::uint64_t *address)
    {
        auto result = BlockIOStream::readChunk(buffer, expected_size, address);
        if (result) {
            // map to a local buffer
            if (buffer.data() != m_buffer.data()) {
                m_buffer.resize(buffer.size());
                std::copy(buffer.data(), buffer.data() + buffer.size(), m_buffer.data());
            }

            // reference with bounds validation
            const_bounded_buf_t const_buf(Settings::m_decode_error, reinterpret_cast<std::byte*>(m_buffer.data()),
                reinterpret_cast<std::byte*>(m_buffer.data() + m_buffer.size())
            );
            m_last_change_log_ptr = &o_change_log_t::__safe_const_ref(const_buf);
        }
        return result;
    }
    
    template <typename o_change_log_t>
    std::size_t ChangeLogIOStream<o_change_log_t>::readChunk() {
        return this->readChunk(m_buffer);
    }

    template <typename o_change_log_t>
    const o_change_log_t *ChangeLogIOStream<o_change_log_t>::readChangeLogChunk() {
        return readChangeLogChunk(m_buffer);
    }

    template <typename o_change_log_t>
    const o_change_log_t *ChangeLogIOStream<o_change_log_t>::getLastChangeLogChunk() const {
        return m_last_change_log_ptr;
    }
    
    template <typename o_change_log_t>
    ChangeLogIOStream<o_change_log_t>::Reader::Reader(ChangeLogIOStream &stream)
        : m_stream(stream)
        , m_it_next_buffer(m_buffers.end())
    {        
    }

    template <typename o_change_log_t>
    typename ChangeLogIOStream<o_change_log_t>::Reader ChangeLogIOStream<o_change_log_t>::getStreamReader() {
        return *this;
    }
    
    template <typename o_change_log_t>
    const o_change_log_t *ChangeLogIOStream<o_change_log_t>::Reader::readChangeLogChunk()
    {
        if (m_it_next_buffer == m_buffers.end()) {
            // read and cache the result
            m_buffers.emplace_back();
            auto result = m_stream.readChangeLogChunk(m_buffers.back());
            if (!result) {
                // revert incomplete item
                m_buffers.pop_back();                
            }
            m_it_next_buffer = m_buffers.end();
            return result;
        } else {
            // read from a cached buffer
            auto result = &o_change_log_t::__const_ref(m_it_next_buffer->data());
            ++m_it_next_buffer;
            return result;
        }
    }
    
    template <typename o_change_log_t>
    void ChangeLogIOStream<o_change_log_t>::Reader::reset() {
        m_it_next_buffer = m_buffers.begin();
    }
    
    template <typename o_change_log_t>
    typename ChangeLogIOStream<o_change_log_t>::Writer ChangeLogIOStream<o_change_log_t>::getStreamWriter() {
        return Writer(*this);
    }

    template <typename o_change_log_t>
    const o_change_log_t &ChangeLogIOStream<o_change_log_t>::appendChangeLog(const o_change_log_t &change_log)
    {
        // append change log as a separate chunk
        auto size_of = change_log.sizeOf();
        if (m_buffer.size() < size_of) {
            m_buffer.resize(size_of);
        }

        memcpy(m_buffer.data(), &change_log, size_of);
        BlockIOStream::addChunk(size_of);
        BlockIOStream::appendToChunk(m_buffer.data(), size_of);
        m_last_change_log_ptr = &o_change_log_t::__const_ref(m_buffer.data());
        assert(m_last_change_log_ptr->sizeOf() == size_of);

        return *m_last_change_log_ptr;
    }
    
    template <typename o_change_log_t>
    ChangeLogIOStream<o_change_log_t>::Writer::Writer(ChangeLogIOStream &stream)
        : m_stream(stream)
    {
    }

    template <typename o_change_log_t>
    void ChangeLogIOStream<o_change_log_t>::Writer::appendChangeLog(const o_change_log_t &change_log)
    {
        auto size_of = change_log.sizeOf();
        m_buffers.emplace_back(size_of);
        auto &buf = m_buffers.back();
        assert(buf.size() == size_of);
        memcpy(buf.data(), &change_log, size_of);
    }

    template <typename o_change_log_t>
    void ChangeLogIOStream<o_change_log_t>::Writer::flush()
    {
        for (const auto &buf : m_buffers) {
            m_stream.appendChangeLog(o_change_log_t::__const_ref(buf.data()));
        }
        m_buffers.clear();
    }

    template class ChangeLogIOStream<>;
    template class ChangeLogIOStream<db0::o_change_log<db0::o_dram_changelog_header> >;
    template class ChangeLogIOStream<db0::o_change_log<db0::o_dp_changelog_header> >;
    
}