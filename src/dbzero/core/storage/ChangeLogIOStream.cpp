#include "ChangeLogIOStream.hpp"
#include <algorithm>

namespace db0

{

    ChangeLogIOStream::ChangeLogIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
        std::function<std::uint64_t()> tail_function, AccessType access_type)
        // enable checksums by default
        : BlockIOStream(m_file, begin, block_size, tail_function, access_type, true)
    {
    }
    
    ChangeLogIOStream::ChangeLogIOStream(BlockIOStream &&io_stream)
        : BlockIOStream(std::move(io_stream))
    {
    }
    
    const o_change_log &ChangeLogIOStream::appendChangeLog(ChangeLogData &&data)
    {
        auto size_of = o_change_log::measure(data);
        if (m_buffer.size() < size_of) {
            m_buffer.resize(size_of);
        }
        
        o_change_log::__new(m_buffer.data(), data);
        // append change log as a separate chunk
        BlockIOStream::addChunk(size_of);
        BlockIOStream::appendToChunk(m_buffer.data(), size_of);
        m_last_change_log_ptr = &o_change_log::__const_ref(m_buffer.data());
        assert(m_last_change_log_ptr->sizeOf() == size_of);

        return *m_last_change_log_ptr;
    }
    
    const o_change_log *ChangeLogIOStream::readChangeLogChunk(std::vector<char> &buffer)
    {
        if (BlockIOStream::readChunk(buffer)) {
            m_last_change_log_ptr = &o_change_log::__const_ref(buffer.data());
            return m_last_change_log_ptr;
        } else {
            return nullptr;
        }
    }

    const o_change_log *ChangeLogIOStream::readChangeLogChunk() {
        return readChangeLogChunk(m_buffer);
    }

    const o_change_log *ChangeLogIOStream::getLastChangeLogChunk() const {
        return m_last_change_log_ptr;
    }
    
    ChangeLogIOStream::Reader::Reader(ChangeLogIOStream &stream)
        : m_stream(stream)
        , m_it_next_buffer(m_buffers.end())
    {        
    }

    ChangeLogIOStream::Reader ChangeLogIOStream::getStreamReader() {
        return *this;
    }
    
    const o_change_log *ChangeLogIOStream::Reader::readChangeLogChunk()
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
            auto result = &o_change_log::__const_ref(m_it_next_buffer->data());
            ++m_it_next_buffer;
            return result;
        }
    }
    
    void ChangeLogIOStream::Reader::reset() {
        m_it_next_buffer = m_buffers.begin();
    }
    
}