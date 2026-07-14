// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <cstdint>
#include <dbzero/core/dram/DRAMSpace.hpp>
#include "BaseStorage.hpp"
#include "BlockIOStream.hpp"
#include <cstring>
#include <dbzero/core/serialization/Types.hpp>
#include <unordered_set>
#include <unordered_map>
#include <atomic>
#include <functional>
#include <optional>
#include <dbzero/core/compiler_attributes.hpp>
#include "BaseStorage.hpp"
#include "ChangeLogIOStream.hpp"

namespace db0

{
    
    class DRAM_Prefix;
    class DRAM_Allocator;
    class CFile;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_dram_chunk_header: public o_fixed<o_dram_chunk_header>
    {
        // hash from the DRAM page contents (header included)
        std::uint64_t m_hash = 0;
        StateNumType m_state_num = 0;
        std::uint64_t m_page_num = 0;
        
        // as invalid
        o_dram_chunk_header() = default;
        o_dram_chunk_header(StateNumType state_num, std::uint64_t page_num = 0)
            : m_state_num(state_num)
            , m_page_num(page_num)
        {
        }

        bool operator!() const;

        // Calculate hash from the entire block's data (including header)
        std::uint64_t calculateHash(const void *data, std::size_t data_size) const;

        // calculate data pointer immediately following the header
        char *getData() {
            return (char*)this + sizeOf();
        }

        const char *getData() const {
            return (const char*)this + sizeOf();
        }

        void setHash(const void *data, std::size_t data_size) {
            m_hash = calculateHash(data, data_size);
        }
    };
DB0_PACKED_END

    struct DRAM_PageInfo
    {
        // the most recent state number
        std::uint64_t m_state_num = 0;
        // page address in the underlying stream
        std::uint64_t m_address = 0;
    };
    
    /**
     * BlockIOStream wrapper with specialization for reading/writing DRAMSpace contents
    */
    class DRAM_IOStream: public BlockIOStream
    {
    public:
        // checksums disabled in this type of stream
        static constexpr bool ENABLE_CHECKSUMS = false;
        using DRAM_ChangeLogT = BaseStorage::DRAM_ChangeLogT;
        using DRAM_ChangeLogStreamT = db0::ChangeLogIOStream<DRAM_ChangeLogT>;
        
        DRAM_IOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size, std::function<std::uint64_t()> tail_function,
            AccessType access_type, std::uint32_t dram_page_size);
        DRAM_IOStream(DRAM_IOStream &&);
        DRAM_IOStream(const DRAM_IOStream &) = delete;
        
        /**
         * Flush all updates done to DRAM_Space into the underlying BlockIOStream
         * the operation updates stream with a complete contents of potential transaction
         * @param state_num the state number under which the modifications are to be stored
         * @param dram_changelog_io the stream to receive DRAM IO "changelog" chunks        
        */
        void flushUpdates(StateNumType state_num, DRAM_ChangeLogStreamT &);
        
        // The purpose of this operation is allowing atomic application of changes
        // this call may end with an IOException without affecting internal state (except populating temporary buffers)
        // @return the latest state number of available changes
        std::optional<StateNumType> beginApplyChanges(DRAM_ChangeLogStreamT &) const;
        
        // Apply buffered changes (allowed on condition beginApplyChanges succeeded)
        // @param max_state_num the last known consistent state number
        // @return false if the applied changes were INCONSISTENT (refresh must be repeated)
        bool completeApplyChanges(StateNumType max_state_num);
        
        /**
         * Get the underlying DRAM pair (prefix and allocator)
        */
        DRAM_Pair getDRAMPair() const;

        static constexpr std::size_t sizeOfHeader() {
            return o_dram_chunk_header::sizeOf();
        }
        
        std::uint64_t tail() const;

        AccessType getAccessType() const {
            return BlockIOStream::getAccessType();
        }

        std::size_t getBlockSize() const {
            return BlockIOStream::getBlockSize();
        }
        
        /**
         * Flush not allowed on DRAM_IOStream, use flushUpdates instead
        */
        void flush();

        void close();
        
        bool empty() const;
        
        /**
         * This operation is only available in read/write mode
         * @return the total number of bytes allocated in the underlying BlockIOStream
        */
        std::size_t getAllocatedSize() const;

        const DRAM_Prefix &getDRAMPrefix() const;
        
        const DRAM_Allocator &getDRAMAllocator() const;
        
        // get the number of random write operations performed while flushing updates
        std::size_t getRandOpsCount() const;
        
#ifndef NDEBUG
        using DRAM_CheckResult = BaseStorage::DRAM_CheckResult;

        void getDRAM_IOMap(std::unordered_map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t> > &) const;
        // Read physical data block from file and detect discrepancies        
        void dramIOCheck(std::vector<DRAM_CheckResult> &) const;
#endif
        
        /**
         * Exhaust the entire change-log (to mark synchronization point)
         * then load entire contents from stream into the DRAM Storage
         * @param max_state_num optional state number to sync up to
        */
        void load(DRAM_ChangeLogStreamT &, std::optional<StateNumType> max_state_num = std::nullopt);
        
        std::size_t getChunkSize() const {
            return m_chunk_size;
        }
        
        std::uint32_t getDRAMPageSize() const {
            return m_dram_page_size;
        }

    private:
        const std::uint32_t m_dram_page_size;
        const std::size_t m_chunk_size;
        // addresses of blocks/chunks which can be overwritten as they contain outdated data
        std::unordered_set<std::uint64_t> m_reusable_chunks;
        // the map of most recent DRAM page locations in the stream
        std::unordered_map<std::uint32_t, DRAM_PageInfo> m_page_map;
        std::shared_ptr<DRAM_Prefix> m_prefix;
        std::shared_ptr<DRAM_Allocator> m_allocator;
        // chunks buffer for the beginApplyChanges / completeApplyChanges operations
        mutable std::unordered_map<std::uint64_t, std::vector<char> > m_read_ahead_chunks;
        
        // @param max_state_num the last known consistent state number
        // @param is_consistent flag set to false if the resulting state cannot be assumed consistent
        // data pages with higher state numbers are ignored
        void *updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr, 
            const o_dram_chunk_header &header, StateNumType max_state_num, bool *is_consistent = nullptr);
        void updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr, 
            const o_dram_chunk_header &header, const void *bytes, StateNumType max_state_num, bool *is_consistent = nullptr);
        
        // Overwrite invalid or corrupted DRAM page with null data
        void trashDRAMPage(std::uint64_t address);
        
        // the number of random write operations performed while flushing updates
        std::uint64_t m_rand_ops = 0;
        
        std::vector<char> getTrashDRAMPage() const;
        std::ostream &dumpPageMap(std::ostream &os) const;
    };
    
    // Calculate hash to determine if the dram chunk is valid
    bool isDRAM_ChunkValid(std::uint32_t dram_page_size, const std::vector<char> &chunk_data);
    bool isDRAM_ChunkValid(std::uint32_t dram_page_size, const o_dram_chunk_header &header, const void *data_begin,
        const void *data_end);
    
    // Extract state number from a valid DRAM chunk
    StateNumType getDRAM_ChunkStateNum(const std::vector<char> &chunk_data);
    
    // Pre-fetch changes into the chunks buffer
    // @param callback optional function to be called for each changelog chunk read
    // @return the latest state number of available changes
    std::optional<StateNumType> fetchDRAM_IOChanges(const DRAM_IOStream &dram_io, 
        DRAM_IOStream::DRAM_ChangeLogStreamT &changelog_io,
        std::unordered_map<std::uint64_t, std::vector<char> > &chunks_buf, 
        std::function<void(const DRAM_IOStream::DRAM_ChangeLogT &)> callback = {});
    
    // Flush changes from the buffer
    void flushDRAM_IOChanges(DRAM_IOStream &dram_io,
        const std::unordered_map<std::uint64_t, std::vector<char> > &chunks_buf);
    
    // Append all chunks from the buffer
    void appendDRAM_IOChunks(DRAM_IOStream &dram_io,
        const std::vector<std::vector<char> > &chunks_buf);
    
}