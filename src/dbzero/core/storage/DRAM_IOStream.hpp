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
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN
    
    class DRAM_Prefix;
    class DRAM_Allocator;
    class CFile;
    class ChangeLogIOStream;

    struct DB0_PACKED_ATTR o_dram_chunk_header: public o_fixed<o_dram_chunk_header>
    {
        std::uint64_t m_state_num = 0;
        std::uint64_t m_page_num = 0;        

        o_dram_chunk_header() = default;
        o_dram_chunk_header(std::uint64_t state_num, std::uint64_t page_num = 0)
            : m_state_num(state_num)
            , m_page_num(page_num)
        {
        }

        // calculate data pointer immediately following the header
        char *getData() {
            return (char*)this + sizeOf();
        }

        const char *getData() const {
            return (const char*)this + sizeOf();
        }
    };

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
    class DRAM_IOStream: protected BlockIOStream
    {
    public:
        // checksums disabled in this type of stream
        static constexpr bool ENABLE_CHECKSUMS = false;
        
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
        void flushUpdates(std::uint64_t state_num, ChangeLogIOStream &dram_changelog_io);
        
        // The purpose of this operation is allowing atomic application of changes
        // this call may end with an IOException without affecting internal state (except populating temporary buffers)
        // @return the latest state number of available changes
        void beginApplyChanges(ChangeLogIOStream &changelog_io) const;
        
        // Apply buffered changes (allowed on condition beginApplyChanges succeeded)
        bool completeApplyChanges();
    
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
        mutable std::unordered_set<std::uint64_t> m_addr_set;        
        
        /**
         * Load entire contents from stream into the DRAM Storage
        */
        void load();
        void *updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr, 
            const o_dram_chunk_header &header);
        void updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr, 
            const o_dram_chunk_header &header, const void *bytes);
        
        // the number of random write operations performed while flushing updates
        std::uint64_t m_rand_ops = 0;
        
        // Create new read-ahead buffer
        std::vector<char> &createReadAheadBuffer(std::uint64_t address, std::size_t size) const;
        // Retrieve existing read-ahead buffer
        const std::vector<char> &getReadAheadBuffer(std::uint64_t address) const;
    };
    
DB0_PACKED_END
}