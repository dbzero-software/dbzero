#include "ChangeLogIOStream.hpp"
#include <algorithm>

namespace db0

{

    ChangeLogIOStream::ChangeLogIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
        std::function<std::uint64_t()> tail_function, AccessType access_type)
        // enable checksums
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
    
    const o_change_log *ChangeLogIOStream::readChangeLogChunk()
    {
        if (BlockIOStream::readChunk(m_buffer)) {
            m_last_change_log_ptr = &o_change_log::__const_ref(m_buffer.data());
            return m_last_change_log_ptr;
        } else {
            return nullptr;
        }
    }

    const o_change_log *ChangeLogIOStream::getLastChangeLogChunk() const {
        return m_last_change_log_ptr;
    }
    
}