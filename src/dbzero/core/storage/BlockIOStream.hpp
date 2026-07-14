// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <functional>
#include "CFile.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <cassert>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
    
    /**
     * Calculate a buffer's checksum (must be aligned to 8 bytes)
    */
    std::uint64_t checksum(const void *begin, const void *end);

    // block level header
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_block_io_block_header: public o_fixed<o_block_io_block_header>
    {
        std::uint64_t m_next_block_address = 0;

        inline bool hasNext() const {
            return m_next_block_address != 0;
        }

        void setNext(std::uint64_t address) {
            assert(!m_next_block_address);
            m_next_block_address = address;
        }
    };
DB0_PACKED_END
    
    // block level header with a checksum
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_block_io_cs_block_header:
    public o_fixed_ext<o_block_io_cs_block_header, o_block_io_block_header>
    {        
        // checksum calculated over the entire block (excluding checksum field)        
        std::uint64_t m_block_checksum = 0;

        // calculates this header's checksum (may differ from the stored one)
        std::uint64_t calculateHeaderChecksum() const {
            // calculate checksum of this object excluding the checksum field
            return checksum((const char*)this, (const char*)this + sizeOf() - sizeof(m_block_checksum));
        }
    };    
DB0_PACKED_END

    // chunk level header
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_block_io_chunk_header: public o_fixed<o_block_io_chunk_header>
    {
        std::uint32_t m_chunk_size = 0;

        inline o_block_io_chunk_header() = default;
        o_block_io_chunk_header(std::uint32_t chunk_size)
            : m_chunk_size(chunk_size)
        {
        }

        inline bool isValid() const {
            return m_chunk_size != 0;
        }
    };
DB0_PACKED_END

    /**
     * Stream of blocks embeddable into the .db0 file        
     * the stream should be first read and then can be appended to
     */
    class BlockIOStream
    {
    public:
        /**
         * @param m_file underlying file object
         * @param begin the stream's starting location/address
         * @param block_size single block size (including headers)
         * @param tail_function required for read/write streams when multiple streams occupy the same file
         * @param access_type either read/write or read-only
         * @param maintain_checksums if true then block-level checksums will be calculated and validated
        */
        BlockIOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
            std::function<std::uint64_t()> tail_function = {}, AccessType = AccessType::READ_WRITE,
            bool maintain_checksums = false);
        
        BlockIOStream(BlockIOStream &&);
        
        BlockIOStream(const BlockIOStream &) = delete;
        
        virtual ~BlockIOStream();
        
        /**
         * Add a new chunk with a specific header
         * @param header chunk size
         * @param addresss if not null, the absolute address of the chunk is stored here
         **/
        void addChunk(o_block_io_chunk_header header, std::uint64_t *addresss = nullptr);

        void appendToChunk(const void *buffer, std::size_t size);
        
        /**
         * Prepare a valid raw chunk buffer
         * @return writeable/user area of the buffer
        */
        char *prepareChunk(std::size_t size, std::vector<char> &buffer) const;

        /**
         * Attempt reading the next chunk
         * buffer gets resized to accomodate the chunk size
         * @param buffer the chunk bytes are stored here
         * @param expected_size if non-zero, the chunk size must match this value
         * @param address if not null, the absolute address of the chunk is stored here
         * @return the number of bytes read or 0 if EOF
        */
        virtual std::size_t readChunk(std::vector<char> &buffer, std::size_t expected_size = 0, 
            std::uint64_t *address = nullptr);
        
        // Reach the next chunk into the internal buffer (where available)
        // The default implementation throws
        virtual std::size_t readChunk();
        
        /**
         * Refresh method re-reads the tail block from disk.
         * It needs to be called to be able to retrieve file modifications done by other process/instance
         * Refresh is only permitted in read-only mode
         * @return true if the tail block's contents changed
        */
        bool refresh();
        
        // @param no_fsync if true then skip fsync after flush (required only in rare cases)
        void flush(bool no_fsync = true);
        
        void close();
        
        /**
         * Get tail of the stream - i.e. the first address after stream's owned data
         */
        std::uint64_t tail() const;

        /**
         * Return current position in the stream (relative address)
        */
        std::uint64_t tell() const;

        /**
         * Get block number and offset in the block
        */
        std::pair<std::size_t, std::size_t> tellBlock() const;

        std::size_t getBlockSize() const;

        /**
         * Get the end-of-stream flag
        */
        bool eos() const;

        static constexpr std::size_t sizeOfBlockHeader(bool checksums_enabled)
        {
            if (checksums_enabled) {
                return o_block_io_cs_block_header::sizeOf();
            } else {
                return o_block_io_block_header::sizeOf();
            }
        }
        
        static constexpr std::size_t sizeOfChunkHeader() {
            return o_block_io_chunk_header::sizeOf();
        }

        static constexpr std::size_t sizeOfHeaders(bool checksums_enabled) {
            return sizeOfBlockHeader(checksums_enabled) + sizeOfChunkHeader();
        }
        
        AccessType getAccessType() const {
            return m_access_type;
        }
        
        // Get absolute file address (of the block) and the relative address of the current position
        std::pair<std::uint64_t, std::uint64_t> getStreamPos() const;
        
        // Set stream position for reading
        // NOTE: only values returned by getStreamPos() can be used, otherwise the behavior is undefined
        void setStreamPos(std::uint64_t address, std::uint64_t stream_pos);
        // @param stream_pos value as returned from getStreamPos
        void setStreamPos(const std::pair<std::uint64_t, std::uint64_t> &stream_pos);
        
        // Position the stream at its begin / head
        void setStreamPosHead();

        struct State
        {
            std::uint64_t m_address;
            std::uint64_t m_stream_pos;
            std::size_t m_block_num;                     
            bool m_eos;
        };
        
        // Temporarily save the stream's state, to be later restored with restoreState()
        // NOTE: no mutations between saveState() and restoreState() are allowed, or the behavior is undefined
        void saveState(State &) const;
        
        void restoreState(const State &);
        
        // Retrieve the stream's modified flag
        bool modified() const {
            return m_modified;
        }

        /**
         * Overwrite existing chunk with arbitrary data.
         * No validations are performed, needs to be used with caution.
         * Since this operations affects checksum, it's not allowed for streams opened with maintain_checksums = true
         * Cursor position is not affected by this operation.
         * @param address absolute address of the chunk
         * @param buffer data to be written
         * @param size size of the buffer
        */
        void writeToChunk(std::uint64_t address, const void *buffer, std::size_t size);

        /**
         * Read a chunk under a specific address without moving the within-stream cursor's position
         * checksum validation is not performed
         * @param address absolute address of the chunk
         * @param buffer buffer to hold the chunk data
         * @param chunk_size size of the chunk
        */
        void readFromChunk(std::uint64_t address, void *buffer, std::size_t chunk_size) const;
        
    protected:
        CFile &m_file;
        // the stream's starting address
        const std::uint64_t m_head;
        // file address of the current block
        std::uint64_t m_address;
        const std::uint32_t m_block_size;
        std::function<std::uint64_t()> m_tail_function;
        // currently written data block
        std::vector<char> m_buffer;
        char *const m_block_begin;
        char *m_block_pos;
        char *const m_block_end;
        o_block_io_block_header &m_block_header;
        // block header with checksums enabled (only valid when m_checksums_enabled == true)
        o_block_io_cs_block_header &m_cs_block_header;
        std::uint32_t m_chunk_left_bytes = 0;
        bool m_modified = false;
        // end-of-stream flag
        bool m_eos = false;
        // ordinal number of the current block (calculated)
        std::size_t m_block_num = 0;
        const AccessType m_access_type;
        bool m_closed = false;
        std::vector<char> m_temp_buf;
        char *m_temp_block_end;
        // temp block header only valid when m_checksums_enabled == true
        o_block_io_cs_block_header &m_temp_block_header;
        const bool m_checksums_enabled;
        
        /**
         * Flush modified block to disk
         * @return true if any modifications were flushed
        */
        bool flushModified();

        void write(const void *buffer, std::size_t size, std::uint64_t *address = nullptr);

        bool read(void *buffer, std::size_t size);
        
        /**
         * Skip specific number of bytes in the stream
        */
        bool skip(std::size_t size);

        // Read data without moving the cursor
        bool peek(void *buffer, std::size_t size, std::uint64_t *address = nullptr);

        // Finish current block and read or create the next one if requested
        // return false if unable to read next block's data (when reading)
        bool getNextBlock(bool write);
        
        /**
         * Read entire block & validate checksum
         * @return false if validation was not successful
        */
        bool readBlock(std::uint64_t address, void *buffer);

        std::uint64_t nextAddress() const;
    };

}
