// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "BlockIOStream.hpp"
#include <string.h>
#include <limits>
#include <cassert>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    std::uint64_t checksum(const void *begin, const void *end)
    {
        assert(((const char*)end - (const char*)begin) % 8 == 0);
        const std::uint64_t *int_begin = reinterpret_cast<const std::uint64_t *>(begin);
        const std::uint64_t *int_end = reinterpret_cast<const std::uint64_t *>(end);
        std::uint64_t checksum = 0;
        for (; int_begin != int_end; ++int_begin) {
            checksum ^= *int_begin;
        }
        return checksum;
    }

    BlockIOStream::BlockIOStream(CFile &file, std::uint64_t begin, std::uint32_t block_size,
        std::function<std::uint64_t()> tail_function, AccessType access_type, bool cs)
        : m_file(file)
        , m_head(begin) 
        , m_address(begin)
        , m_block_size(block_size)
        , m_tail_function(tail_function)
        , m_buffer(block_size, 0)
        , m_block_begin(m_buffer.data())
        , m_block_pos(m_block_begin)
        , m_block_end(m_block_begin + m_buffer.size() - 
            (cs ? o_block_io_cs_block_header::sizeOf() : o_block_io_block_header::sizeOf()))
        // block header is stored at the end of the block
        , m_block_header(o_block_io_block_header::__ref(m_block_end))
        , m_cs_block_header(o_block_io_cs_block_header::__ref(m_block_end))
        , m_access_type(access_type)
        // temp block only initialized when checksums enabled
        , m_temp_buf(cs ? block_size : 0)        
        , m_temp_block_end(m_temp_buf.data() + m_temp_buf.size() - o_block_io_cs_block_header::sizeOf())
        , m_temp_block_header(o_block_io_cs_block_header::__ref(m_temp_block_end))
        , m_checksums_enabled(cs)
    {
        if (m_block_size % 8 != 0) {
            THROWF(db0::InternalException) << "BlockIOStream block size must be 64-bit padded";
        }
        if (access_type == AccessType::READ_WRITE && m_file.getAccessType() == AccessType::READ_ONLY) {
            THROWF(db0::InternalException) << "BlockIOStream unable to write to read-only file";
        }
        // Try reading the first full block
        if ((m_address + m_block_size > m_file.size()) || !readBlock(m_address, m_block_begin)) {
            // create a new block, overwrite incomplete blocks (process may have crashed when flushing)
            memset(m_block_begin, 0, m_buffer.size());
            m_modified = (m_access_type == AccessType::READ_WRITE);
            m_eos = true;
        }
    }
    
    BlockIOStream::BlockIOStream(BlockIOStream &&other)
        : m_file(other.m_file)
        , m_head(other.m_head)
        , m_address(other.m_address)
        , m_block_size(other.m_block_size)
        , m_tail_function(other.m_tail_function)
        , m_buffer(std::move(other.m_buffer))
        , m_block_begin(m_buffer.data())
        , m_block_pos(m_block_begin + (other.m_block_pos - other.m_block_begin))
        , m_block_end(m_block_begin + m_buffer.size() - 
            (other.m_checksums_enabled ? o_block_io_cs_block_header::sizeOf():o_block_io_block_header::sizeOf()))
        , m_block_header(o_block_io_block_header::__ref(m_block_end))
        , m_cs_block_header(o_block_io_cs_block_header::__ref(m_block_end))
        , m_chunk_left_bytes(other.m_chunk_left_bytes)
        , m_modified(other.m_modified)
        , m_eos(other.m_eos)
        , m_block_num(other.m_block_num)
        , m_access_type(other.m_access_type)
        , m_closed(other.m_closed)
        , m_temp_buf(std::move(other.m_temp_buf))
        , m_temp_block_end(m_temp_buf.data() + m_temp_buf.size() - o_block_io_cs_block_header::sizeOf())
        , m_temp_block_header(o_block_io_cs_block_header::__ref(m_temp_block_end))
        , m_checksums_enabled(other.m_checksums_enabled)
    {
        other.m_closed = true;
    }

    BlockIOStream::~BlockIOStream() {
        assert((m_closed || m_access_type == AccessType::READ_ONLY) && "Read/Write BlockIOStream not closed");
    }
    
    bool BlockIOStream::getNextBlock(bool write)
    {
        assert(!m_modified || write);
        // take next block from end of file
        bool create_next_block = false;
        if (write && !m_block_header.hasNext()) {
            m_block_header.setNext(nextAddress());
            create_next_block = true;
            assert(m_access_type == AccessType::READ_WRITE);
            m_modified = true;
        }

        // flush modified block to disk
        flushModified();

        // try accessing the next block
        if (!m_block_header.hasNext()) {
            return false;
        }
        
        // check if the next block can be read completely
        auto next_block_address = m_block_header.m_next_block_address;
        bool block_in_range = (m_file.size() >= next_block_address + m_block_size);
        if (!write && !block_in_range) {
            return false;
        }

        if (create_next_block || !block_in_range) {
            m_address = next_block_address;
            m_block_pos = m_block_begin;

            assert(m_access_type == AccessType::READ_WRITE);
            // create a new block / overwrite incomplete block
            memset(m_block_begin, 0, m_buffer.size());
            m_modified = true;
            m_eos = true;
        } else {
            // read full block only
            // note that block gets rejected if checksum is not correct        
            if (!readBlock(next_block_address, m_block_begin)) {
                return false;
            }
            
            m_address = next_block_address;
            m_block_pos = m_block_begin;
        }
        ++m_block_num;
        return true;
    }
    
    bool BlockIOStream::flushModified()
    {
        if (!m_modified) {
            return false;
        }
        
        assert(m_access_type == AccessType::READ_WRITE);
        // calculate combined checksum (data + header)
        assert(m_block_header.m_next_block_address == m_cs_block_header.m_next_block_address);
        if (m_checksums_enabled) {
            m_cs_block_header.m_block_checksum = checksum(m_block_begin, m_block_end) ^ m_cs_block_header.calculateHeaderChecksum();
        }
        m_file.write(m_address, m_buffer.size(), m_block_begin);
        m_modified = false;
        return true;
    }

    void BlockIOStream::addChunk(o_block_io_chunk_header chunk_header, std::uint64_t *address)
    {
        assert(!m_closed);
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::InternalException) << "BlockIOStream unable to append to read-only stream";
        }
        if (!m_eos) {
            THROWF(db0::InternalException) << "BlockIOStream unable to append to stream";
        }
        if (m_chunk_left_bytes) {
            THROWF(db0::InternalException) << "BlockIOStream::addChunk: chunk is not finished";
        }
        
        write(&chunk_header, chunk_header.sizeOf(), address);
        m_chunk_left_bytes = chunk_header.m_chunk_size;
    }
    
    void BlockIOStream::appendToChunk(const void *buffer, std::size_t size) 
    {
        assert(!m_closed);
        if (size > m_chunk_left_bytes) {
            THROWF(db0::InternalException) << "BlockIOStream::appendToChunk: chunk size is too large";
        }
        write(buffer, size);
        m_chunk_left_bytes -= size;
    }
    
    void BlockIOStream::write(const void *buffer, std::size_t size, std::uint64_t *address)
    {
        assert(size > 0);
        assert(m_access_type == AccessType::READ_WRITE);
        auto in = reinterpret_cast<const char *>(buffer);
        if (m_block_pos == m_block_end) {
            // finish block and create the next one
            getNextBlock(true);
        }

        // calculate the absolute address
        assert(m_block_pos != m_block_end);
        if (address) {
            *address = m_address + (m_block_pos - m_block_begin);
        }

        while (size > 0) {
            if (m_block_pos == m_block_end) {
                // finish block and create the next one
                getNextBlock(true);
            }
            auto write_size = std::min(m_block_end - m_block_pos, static_cast<std::ptrdiff_t>(size));
            memcpy(m_block_pos, in, write_size);
            m_block_pos += write_size;
            in += write_size;
            size -= write_size;
            if (!m_modified) {
                m_modified = true;
            }
        }
    }

    std::size_t BlockIOStream::readChunk(std::vector<char> &buffer, std::size_t expected_size, std::uint64_t *address)
    {
        assert(!m_closed);
        if (m_eos) {
            // end-of-stream reached, need to call refresh to be able data appended in meantime
            return 0;
        }
        o_block_io_chunk_header chunk_header;
        if (!peek(&chunk_header, o_block_io_chunk_header::sizeOf(), address)) {
            // end of stream (maybe process crashed when flushing?)
            m_eos = true;
            return 0;
        }
        if (!chunk_header.isValid()) {
            // end of the input stream
            m_eos = true;
            return 0;
        }
        if (expected_size && chunk_header.m_chunk_size != expected_size) {
            THROWF(db0::InternalException) << "BlockIOStream::readChunk: chunk size mismatch";
        }
        skip(chunk_header.sizeOf());
        assert(chunk_header.isValid());
        if (buffer.size() < chunk_header.m_chunk_size) {
            buffer.resize(chunk_header.m_chunk_size);
        }
        if (!read(buffer.data(), chunk_header.m_chunk_size)) {
            m_eos = true;
            return 0;
        }
        return chunk_header.m_chunk_size;
    }
    
    bool BlockIOStream::peek(void *buffer, std::size_t size, std::uint64_t *address)
    {
        // switch to next block if at end of the current one
        if (m_block_pos == m_block_end && !getNextBlock(false)) {
            return false;
        }
        
        // retrieve absolute address
        if (address) {
            *address = m_address + (m_block_pos - m_block_begin);
        }
        auto out = reinterpret_cast<char *>(buffer);
        auto pos = m_block_pos;
        while (size > 0) {
            if (pos == m_block_end) {
                // must peek at the next block
                // which may be slow but hopefully will not happen often
                if (!m_block_header.hasNext()) {
                    return false;
                }
                if (m_file.size() < m_block_header.m_next_block_address + size) {
                    return false;
                }
                m_file.read(m_block_header.m_next_block_address, size, out);
                return true;
            }
            auto len = std::min(m_block_end - pos, static_cast<std::ptrdiff_t>(size));
            memcpy(out, pos, len);
            pos += len;
            out += len;
            size -= len;
        }
        return true;
    }

    bool BlockIOStream::read(void *buffer, std::size_t size)
    {
        auto out = reinterpret_cast<char *>(buffer);
        while (size > 0) {
            if (m_block_pos == m_block_end) {
                // finish block and create the next one
                if (!getNextBlock(false)) {
                    // unable to read full block
                    return false;
                }
            }
            auto len = std::min(m_block_end - m_block_pos, static_cast<std::ptrdiff_t>(size));
            memcpy(out, m_block_pos, len);
            m_block_pos += len;
            out += len;
            size -= len;
        }

        if (m_block_pos == m_block_end) {
            // move cursor to the next block (from end of current one), update the EOS flag
            if (!getNextBlock(false)) {
                m_eos = true;
            }
        }

        return true;
    }

    bool BlockIOStream::skip(std::size_t size)
    {
        while (size > 0) {
            if (m_block_pos == m_block_end) {
                // finish block and create the next one
                if (!getNextBlock(false)) {
                    // unable to read full block
                    return false;
                }
            }
            auto len = std::min(m_block_end - m_block_pos, static_cast<std::ptrdiff_t>(size));
            m_block_pos += len;   
            size -= len;
        }

        if (m_block_pos == m_block_end) {
            // move cursor to the next block (from end of current one), update the EOS flag
            if (!getNextBlock(false)) {
                m_eos = true;
            }
        }

        return true;
    }
    
    bool BlockIOStream::refresh()
    {
        if (m_access_type != AccessType::READ_ONLY) {
            THROWF(db0::InternalException) << "BlockIOStream::refresh only allowed for read-only streams";
        }
        if (!eos()) {
            // refresh does not make sense if not at end of the stream
            // return true to indicate "fresh" contents available for reading
            return true;
        }
        
        // contents might've changed without file size change
        m_file.refresh();
        if (m_address + m_block_size <= m_file.size()) {
            std::vector<char> buffer(m_block_size);

            // read full block
            if (readBlock(m_address, buffer.data())) {                
                std::uint64_t *p1 = reinterpret_cast<std::uint64_t *>(buffer.data());
                std::uint64_t *p1_end = reinterpret_cast<std::uint64_t *>(buffer.data() + buffer.size());
                std::uint64_t *p2 = reinterpret_cast<std::uint64_t *>(m_block_begin);

                // compare block contents
                for (;p1 != p1_end; ++p1, ++p2) {
                    if (*p1 != *p2) {
                        memcpy(m_block_begin, buffer.data(), buffer.size());
                        m_eos = false;
                        // contents change detected
                        return true;
                    }
                }            
            }
        }

        // no change, no new contents to read
        return false;
    }

    void BlockIOStream::flush(bool no_fsync)
    {
        // flush modified block to disk
        if (flushModified()) {
            assert(m_access_type == AccessType::READ_WRITE);
            m_file.flush();
            if (!no_fsync) {
                m_file.fsync();
            }
        }
    }
    
    void BlockIOStream::close()
    {
        // mark end of the stream with 0-length chunk (unless already at the end of block and stream)
        if (m_access_type == AccessType::READ_WRITE && m_eos) {
            if (m_block_pos != m_block_end || m_block_header.hasNext()) {
                // add chunk_size = 0 to mark the end of the stream
                addChunk(0);
            }
        }
        flush();
        m_closed = true;
    }
    
    std::uint64_t BlockIOStream::tell() const {
        return m_block_num * m_block_size + (m_block_pos - m_block_begin);
    }

    std::pair<std::size_t, std::size_t> BlockIOStream::tellBlock() const
    {
        // report begin of next block in case of end-of-block reached
        if (m_block_pos == m_block_end) {
            return { m_block_num + 1, 0 };
        }
        return { m_block_num, m_block_pos - m_block_begin };
    }
    
    std::size_t BlockIOStream::getBlockSize() const {
        return m_block_size;
    }
    
    std::uint64_t BlockIOStream::tail() const
    {
        if (!m_eos) {
            assert(false);
            THROWF(db0::InternalException) << "BlockIOStream::tail: Failed (must be EOS)";
        }
        return m_address + m_block_size;
    }
    
    char *BlockIOStream::prepareChunk(std::size_t size, std::vector<char> &buffer) const
    {
        buffer.resize(size + o_block_io_chunk_header::sizeOf(), 0);
        auto &chunk_header = o_block_io_chunk_header::__new(buffer.data(), size);
        return buffer.data() + chunk_header.sizeOf();
    }
    
    void BlockIOStream::writeToChunk(std::uint64_t address, const void *buffer, std::size_t size)
    {
        if (size > (m_block_size - o_block_io_block_header::sizeOf())) {
            THROWF(db0::InternalException) << "BlockIOStream::writeChunk: invalid chunk size";
        }
        if (m_checksums_enabled) {
            THROWF(db0::InternalException) << "BlockIOStream::writeToChunk not allowed with checksums enabled";
        }
        // if write requested to current block then update it
        if (address == m_address) {
            assert(size == static_cast<std::size_t>(m_block_end - m_block_begin));
            std::memcpy(m_block_begin, buffer, size);
            m_modified = true;
        } else {
            // overwrite existing block, except the next block address
            m_file.write(address, size, buffer);
        }
    }
    
    void BlockIOStream::readFromChunk(std::uint64_t address, void *buffer, std::size_t size) const
    {
        if (size > (m_block_size - o_block_io_block_header::sizeOf())) {
            THROWF(db0::InternalException) << "BlockIOStream::readChunk: invalid chunk size";
        }
        m_file.read(address, size, buffer);
    }
    
    bool BlockIOStream::eos() const {
        return m_eos;
    }
    
    bool BlockIOStream::readBlock(std::uint64_t address, void *buffer)
    {        
        if (m_checksums_enabled) {
            assert(m_temp_buf.size() == m_block_size);
            // read into temp buffer first
            m_file.read(address, m_temp_buf.size(), m_temp_buf.data());
            auto ccs = checksum(m_temp_buf.data(), m_temp_block_end) ^ m_temp_block_header.calculateHeaderChecksum();
            // compare calculated & actual checksum
            if (ccs != m_temp_block_header.m_block_checksum) {
                return false;
            }

            memcpy(buffer, m_temp_buf.data(), m_block_size);
        } else {        
            // read into the output buffer directly (since no checksum validations are performed)
            m_file.read(address, m_block_size, buffer);
        }
        return true;
    }
    
    std::pair<std::uint64_t, std::uint64_t> BlockIOStream::getStreamPos() const {
        return { m_address, tell() };
    }
    
    void BlockIOStream::setStreamPos(std::uint64_t address, std::uint64_t stream_pos)
    {
        assert(!m_closed);
        // flush any pending writes before reading
        flush();
        // try reading a full block
        if ((address + m_block_size > m_file.size()) || !readBlock(address, m_block_begin)) {
            THROWF(db0::InternalException) << "BlockIOStream unable to set position in stream";
        }
        
        m_address = address;
        m_block_num = stream_pos / m_block_size;
        m_block_pos = m_block_begin + (stream_pos % m_block_size);
        // this parameter is only used for writing
        m_chunk_left_bytes = 0;    
        m_eos = false;
    }
    
    void BlockIOStream::setStreamPos(const std::pair<std::uint64_t, std::uint64_t> &pos) {
        setStreamPos(pos.first, pos.second);
    }

    void BlockIOStream::setStreamPosHead() {
        setStreamPos(m_head, 0);
    }

    std::uint64_t BlockIOStream::nextAddress() const
    {
        std::uint64_t next_address = tail();
        if (m_tail_function) {
            next_address = std::max(m_tail_function(), next_address);
        }
        return next_address;
    }
    
    void BlockIOStream::saveState(State &state) const
    {
        if (m_closed) {
            THROWF(db0::InternalException) << "BlockIOStream::saveState: stream is closed";
        }
        if (m_modified) {
            THROWF(db0::InternalException) << "BlockIOStream::saveState: stream is modified, must be flushed first";
        }
        if (m_chunk_left_bytes) {
            THROWF(db0::InternalException) << "BlockIOStream::saveState: chunk is not finished";
        }

        state.m_address = m_address;
        state.m_stream_pos = tell();        
        state.m_block_num = m_block_num;
        state.m_eos = m_eos;
    }
    
    void BlockIOStream::restoreState(const State &state)
    {
        assert(!m_closed);
        assert(!m_modified);
        assert(!m_chunk_left_bytes);

        m_address = state.m_address;
        m_block_num = state.m_block_num;
        m_block_pos = m_block_begin + (state.m_stream_pos % m_block_size);
        m_eos = state.m_eos;
        // this parameter is only used for writing
        m_chunk_left_bytes = 0;
        // try reading a full block
        if ((m_address + m_block_size > m_file.size()) || !readBlock(m_address, m_block_begin)) {
            THROWF(db0::InternalException) << "BlockIOStream unable to restore state";
        }
    }
    
    std::size_t BlockIOStream::readChunk() {
        THROWF(db0::InternalException) << "BlockIOStream::readChunk() operation not supported" << THROWF_END;
    }

}