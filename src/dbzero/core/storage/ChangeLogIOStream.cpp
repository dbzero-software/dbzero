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
        if (BlockIOStream::readChunk(buffer)) {
            m_last_change_log_ptr = &o_change_log_t::__const_ref(buffer.data());
            return m_last_change_log_ptr;
        } else {
            return nullptr;
        }
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
    void ChangeLogIOStream<o_change_log_t>::Writer::appendChangeLog(const o_change_log_t &)
    {
        throw std::runtime_error("not implemented");
    }

    template <typename o_change_log_t>
    void ChangeLogIOStream<o_change_log_t>::Writer::flush()
    {
        throw std::runtime_error("not implemented");
    }
    
    template class ChangeLogIOStream<>;
    template class ChangeLogIOStream<db0::o_change_log<db0::o_dp_changelog_header> >;
    
}