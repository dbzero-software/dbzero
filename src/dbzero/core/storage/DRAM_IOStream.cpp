// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DRAM_IOStream.hpp"
#include "BlockIOStream.hpp"
#include <dbzero/core/utils/FlagSet.hpp>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include "ChangeLogIOStream.hpp"
#include <dbzero/core/utils/hash_func.hpp>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/core/memory/utils.hpp>

namespace db0

{
    
    DRAM_IOStream::DRAM_IOStream(CFile &m_file, std::uint64_t begin, std::uint32_t block_size,
        std::function<std::uint64_t()> tail_function, AccessType access_type, std::uint32_t dram_page_size)
        : BlockIOStream(m_file, begin, block_size, tail_function, access_type, DRAM_IOStream::ENABLE_CHECKSUMS)
        , m_dram_page_size(dram_page_size)
        , m_chunk_size(dram_page_size + o_dram_chunk_header::sizeOf())
        , m_prefix(std::make_shared<DRAM_Prefix>(m_dram_page_size))
        , m_allocator(std::make_shared<DRAM_Allocator>(m_dram_page_size))        
    {
    }
    
    DRAM_IOStream::DRAM_IOStream(DRAM_IOStream &&other)
        : BlockIOStream(std::move(other))
        , m_dram_page_size(other.m_dram_page_size)
        , m_chunk_size(other.m_chunk_size)
        , m_reusable_chunks(std::move(other.m_reusable_chunks))
        , m_page_map(std::move(other.m_page_map))
        , m_prefix(other.m_prefix)
        , m_allocator(other.m_allocator)        
    {
    }
    
    void DRAM_IOStream::trashDRAMPage(std::uint64_t address)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        // mark as reusable
        m_reusable_chunks.insert(address);
        auto raw_block = getTrashDRAMPage();
        writeToChunk(address, raw_block.data(), raw_block.size());
        ++m_rand_ops;        
    }
    
    void *DRAM_IOStream::updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr,
        const o_dram_chunk_header &header, StateNumType max_state_num, bool *is_consistent)
    {
        // NOTE: header may be invalid (i.e. copied chunk marked as invalid on copy post-processing)
        // NOTE: ignore changes beyond the last known consistent state number
        if (!!header && header.m_state_num <= max_state_num) {
            if (is_consistent) {
                *is_consistent = true;
            }
            // page map = page_num / state_num
            auto dram_page = m_page_map.find(header.m_page_num);
            // NOTE: even if the same state number is encountered, the page is updated
            // (the previous version might've been incomplete!!)
            if (dram_page == m_page_map.end() || header.m_state_num >= dram_page->second.m_state_num) {
                // update DRAM to most recent page version, page not marked as dirty
                auto result = m_prefix->update(header.m_page_num, false);
                if (dram_page == m_page_map.end()) {
                    // mark address as taken
                    if (allocs_ptr) {
                        allocs_ptr->insert(header.m_page_num * m_dram_page_size);
                    }
                } else {
                    // mark previously occupied block as reusable (read/write mode only)
                    if (m_access_type == AccessType::READ_WRITE) {
                        m_reusable_chunks.insert(dram_page->second.m_address);
                    }
                }
                
                // update DRAM page info
                m_page_map[header.m_page_num] = { header.m_state_num, address };
                // remove address from reusables
                {
                    auto it = m_reusable_chunks.find(address);
                    if (it != m_reusable_chunks.end()) {
                        m_reusable_chunks.erase(it);
                    }
                }
                return result;
            }
        }

        if (is_consistent) {
            // NOTE: null header is assumed as not violating consistency
            // NOTE: we allow up to +1 state number to differ (from the transaction being synced to)
            *is_consistent = !header || header.m_state_num <= max_state_num + 1;
        }

        // mark block as reusable (read/write mode only)
        if (m_access_type == AccessType::READ_WRITE) {
            m_reusable_chunks.insert(address);
        }
        return nullptr;
    }
    
    void DRAM_IOStream::updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr,
        const o_dram_chunk_header &header, const void *bytes, StateNumType max_state_num, bool *is_consistent)
    {
        auto result = updateDRAMPage(address, allocs_ptr, header, max_state_num, is_consistent);
        if (result) {
            std::memcpy(result, bytes, m_dram_page_size);
        }
    }
    
    void DRAM_IOStream::load(DRAM_ChangeLogStreamT &changelog_io, std::optional<StateNumType> max_state_num)
    {
        // Exhaust the change-log stream first and retrieve the last valid state number
        // its position marks the synchronization point
        while (changelog_io.readChangeLogChunk());

        std::vector<char> buffer(m_chunk_size, 0);
        const auto &header = o_dram_chunk_header::__ref(buffer.data());
        auto bytes = buffer.data() + header.sizeOf();
        
        auto last_chunk_ptr = changelog_io.getLastChangeLogChunk();
        if (!last_chunk_ptr) {
            // no data to load
            return;
        }
        
        // The last known consistent state number (unless explicitly provided)
        if (!max_state_num) {
            max_state_num = last_chunk_ptr->m_state_num;
        }
        std::unordered_set<std::size_t> allocs;
        for (;;) {
            auto block_id = tellBlock();
            std::uint64_t chunk_addr;
            if (!readChunk(buffer, m_chunk_size, &chunk_addr)) {
                // end of stream reached
                break;
            }

            // make sure chunks are aligned with blocks (and one chunk per block)
            if (block_id.second != 0 || (!eos() && block_id.first == tellBlock().first)) {
                THROWF(db0::IOException) << "DRAM_IOStream::load error: unaligned block";
            }
            
            // NOTE: ignore invalid or incomplete DRAM chunks
            // this does not automatically indicate any error - it may occur filesystem writes are not assumed atomic 
            // - this chunk might simply be too fresh to be included
            // NOTE: also pages from future (abruptly terminated) transactions are reverted
            if (!isDRAM_ChunkValid(m_dram_page_size, header, bytes, buffer.data() + buffer.size())
                || header.m_state_num > *max_state_num)
            {
                // overwrite the page to prevent from being included in the future
                // this is only permitted in read/write mode !!
                if (m_access_type == AccessType::READ_WRITE) {
                    trashDRAMPage(chunk_addr);
                }
                continue;
            }

            updateDRAMPage(chunk_addr, &allocs, header, bytes, *max_state_num);
        }
        m_allocator->update(allocs);
    }
    
    std::ostream &DRAM_IOStream::dumpPageMap(std::ostream &os) const
    {
        std::vector<std::pair<std::uint32_t, DRAM_PageInfo> > sorted_pages(m_page_map.begin(), m_page_map.end());
        std::sort(sorted_pages.begin(), sorted_pages.end(), [](const auto &a, const auto &b) {
            return a.first < b.first;
        });
        for (const auto &item: sorted_pages) {
            std::cout << "(" << item.first << ": state_num=" << item.second.m_state_num
                      << ", address=" << item.second.m_address << "),";
        }
        return os;
    }
    
    void DRAM_IOStream::flushUpdates(StateNumType state_num, DRAM_ChangeLogStreamT &dram_changelog_io)
    {
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "DRAM_IOStream::flushUpdates error: read-only stream";
        }
        
        // prepare block to overwrite reusable addresses
        std::vector<char> raw_block;
        auto buffer = prepareChunk(m_chunk_size, raw_block);
        auto &reusable_header = o_dram_chunk_header::__new(buffer, state_num);
        buffer += reusable_header.sizeOf();
        
        std::unordered_set<std::uint64_t> last_changelog;
        if (dram_changelog_io.getLastChangeLogChunk()) {
            for (auto addr: *dram_changelog_io.getLastChangeLogChunk()) {
                last_changelog.insert(addr);
            }
        }
        
        // Finds reusable block, note that blocks from the last change log are not reused
        // otherwise the reader process might not be able to access the last transaction
        auto find_reusable = [&, this]() -> std::optional<std::uint64_t> {
            for (auto it = m_reusable_chunks.begin(); it != m_reusable_chunks.end(); ++it) {
                if (last_changelog.find(*it) == last_changelog.end()) {
                    auto result = *it;
                    m_reusable_chunks.erase(it);
                    return result;
                }
            }            
            return std::nullopt;
        };

        auto update_page_location = [&, this](std::uint64_t page_num, std::uint64_t address) {
            // remove address from reusable
            {
                auto it = m_reusable_chunks.find(address);
                if (it != m_reusable_chunks.end()) {
                    m_reusable_chunks.erase(it);
                }
            }
            auto dram_page = m_page_map.find(page_num);
            if (dram_page != m_page_map.end()) {
                assert(dram_page->second.m_address != address);
                // add the old page location to reusable addresses
                m_reusable_chunks.insert(dram_page->second.m_address);
            }
            // update to most recent location (and state number)
            m_page_map[page_num] = { state_num, address };
        };
        
        // flush all changes done to DRAM Prefix (append modified pages only)
        std::vector<std::uint64_t> dram_changelog;
        m_prefix->flushDirty([&, this](std::uint64_t page_num, const void *page_buffer) {
            // the last page must be stored in a new block to mark end of the sequence
            auto reusable_addr = find_reusable();
            if (reusable_addr) {
                reusable_header.m_page_num = page_num;
                std::memcpy(reusable_header.getData(), page_buffer, m_dram_page_size);
                reusable_header.setHash(page_buffer, m_dram_page_size);
                // overwrite chunk in the reusable block
                writeToChunk(*reusable_addr, raw_block.data(), raw_block.size());
                ++m_rand_ops;
                dram_changelog.push_back(*reusable_addr);
                // update to the last known page location, collect previous location as reusable
                update_page_location(page_num, *reusable_addr);
            } else {
                // make sure all chunks are block-aligned
                assert(tellBlock().second == 0);
                std::uint64_t chunk_addr;
                // append data into a new chunk / block
                addChunk(m_chunk_size, &chunk_addr);
                o_dram_chunk_header header(state_num, page_num);
                header.setHash(page_buffer, m_dram_page_size);
                appendToChunk(&header, sizeof(header));
                appendToChunk(page_buffer, m_dram_page_size);
                dram_changelog.push_back(chunk_addr);
                // update to the last known page location, collect previous location as reusable
                update_page_location(page_num, chunk_addr);
            }
#ifndef NDEBUG                
            if (Settings::__dram_io_flush_poison == 1) {
                // flush / fsync before poisoned op (to purpusefully corrupt data)
                BlockIOStream::flush(false);
            }
            checkPoisonedOp(Settings::__dram_io_flush_poison);
#endif
        });
        
        // flush all DRAM data updates before changelog updates
        BlockIOStream::flush();
        // output changelog, no RLE encoding, no duplicates
        ChangeLogData cl_data(std::move(dram_changelog), false, false, false);
        dram_changelog_io.appendChangeLog(std::move(cl_data), state_num);
    }
    
#ifndef NDEBUG
    void DRAM_IOStream::dramIOCheck(std::vector<DRAM_CheckResult> &check_result) const
    {
        std::vector<char> raw_block;
        auto buffer = prepareChunk(m_chunk_size, raw_block);
        auto &header = o_dram_chunk_header::__new(buffer, 0);
        buffer += header.sizeOf();
                
        for (auto &entry: m_page_map) {
            BlockIOStream::readFromChunk(entry.second.m_address, raw_block.data(), raw_block.size());
            if (header.m_page_num != entry.first) {
                check_result.push_back({ entry.second.m_address, header.m_page_num, entry.first });
            }
        }
    }
#endif
    
    DRAM_Pair DRAM_IOStream::getDRAMPair() const {
        return { m_prefix, m_allocator };
    }
    
    std::optional<StateNumType> DRAM_IOStream::beginApplyChanges(DRAM_ChangeLogStreamT &changelog_io) const
    {
        assert(m_read_ahead_chunks.empty());
        if (m_access_type == AccessType::READ_WRITE) {
            THROWF(db0::InternalException) << "DRAM_IOStream::applyChanges require read-only stream";
        }
        
        return fetchDRAM_IOChanges(*this, changelog_io, m_read_ahead_chunks);
    }
    
    bool DRAM_IOStream::completeApplyChanges(StateNumType max_state_num)
    {
        bool is_consistent = true;
        for (const auto &item: m_read_ahead_chunks) {
            auto address = item.first;
            const auto &buffer = item.second;
            const auto &header = o_dram_chunk_header::__const_ref(buffer.data() + o_block_io_chunk_header::sizeOf());
            // NOTE: ignore invalid or incomplete DRAM chunks (too fresh to be included)
            if (!isDRAM_ChunkValid(m_dram_page_size, header, header.getData(), buffer.data() + buffer.size())) {
                // NOTE: since we don't know chunk's actual status, must assume inconsistency
                is_consistent = false;
                continue;
            }
            bool consistent_update;
            updateDRAMPage(address, nullptr, header, header.getData(), max_state_num, &consistent_update);
            is_consistent &= consistent_update;
        }
        m_read_ahead_chunks.clear();
        return is_consistent;
    }
    
    void DRAM_IOStream::flush() {
        THROWF(db0::IOException) << "DRAM_IOStream::flush not allowed";
    }

    bool DRAM_IOStream::empty() const {
        return m_prefix->empty();
    }

    const DRAM_Prefix &DRAM_IOStream::getDRAMPrefix() const {
        return *m_prefix;
    }

    const DRAM_Allocator &DRAM_IOStream::getDRAMAllocator() const {
        return *m_allocator;
    }

    std::size_t DRAM_IOStream::getAllocatedSize() const
    {
        if (m_access_type == AccessType::READ_ONLY) {
            THROWF(db0::IOException) << "DRAM_IOStream::getAllocatedSize require read/write stream";
        }
        // total allocated size equals pages + reusable chunks
        std::size_t block_count = m_page_map.size() + m_reusable_chunks.size();
        return block_count * m_block_size;
    }
    
    std::size_t DRAM_IOStream::getRandOpsCount() const {
        return m_rand_ops;
    }

    std::uint64_t DRAM_IOStream::tail() const {
        return BlockIOStream::tail();
    }

    void DRAM_IOStream::close() {
        BlockIOStream::close();
    }
    
    std::vector<char> DRAM_IOStream::getTrashDRAMPage() const
    {
        std::vector<char> raw_block;
        auto buffer = prepareChunk(m_chunk_size, raw_block);
        // initialize header as invalid
        o_dram_chunk_header::__new(buffer);
        return raw_block;
    }

#ifndef NDEBUG 
    void DRAM_IOStream::getDRAM_IOMap(std::unordered_map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t> > &io_map) const
    {
        for (auto &entry: m_page_map) {
            io_map[entry.first] = { entry.second.m_state_num, entry.second.m_address };
        }
    }
#endif
    
    std::optional<StateNumType> fetchDRAM_IOChanges(const DRAM_IOStream &dram_io,
        DRAM_IOStream::DRAM_ChangeLogStreamT &changelog_io,
        std::unordered_map<std::uint64_t, std::vector<char> > &chunks_buf,
        std::function<void(const DRAM_IOStream::DRAM_ChangeLogT &)> callback)
    {
        auto create_read_ahead_buffer = [&](std::uint64_t address, std::size_t size) -> std::vector<char> & 
        {
            auto it = chunks_buf.find(address);
            if (it != chunks_buf.end()) {
                return it->second;
            }        
            return chunks_buf.emplace(address, size).first->second;        
        };

        auto stream_pos = changelog_io.getStreamPos();
        std::optional<StateNumType> max_state_num;
        try {
            // Must continue until exhausting the change-log
            for (;;) {
                // Note that change log and the data chunks may be updated by other process while we read it
                // the consistent state is only guaranteed after reaching end of the stream        
                auto change_log_ptr = changelog_io.readChangeLogChunk();
                if (!change_log_ptr) {
                    // change-log exhausted
                    break;
                }
                
                // Visit the complete change log, reading modified pages
                // NOTE: even if the same page appears in the log we must read if EACH time
                // this is because: a) file writes are NOT atomic, b) DP might be modified while we process the log
                // NOTE: this might be optimized when modifiaction timestamps are introduced                
                while (change_log_ptr) {
                    if (callback) {
                        callback(*change_log_ptr);
                    }
                    
                    max_state_num = change_log_ptr->m_state_num;
                    for (auto address: *change_log_ptr) {
                        // buffer must include BlockIOStream's chunk header and data
                        auto &buffer = create_read_ahead_buffer(address, dram_io.getChunkSize() + o_block_io_chunk_header::sizeOf());
                        // the address reported in changelog must already be available in the stream
                        // it may come from a more recent update as well (and potentially may only be partially written)
                        // therefore chunk-level checksum validation is necessary
                        dram_io.readFromChunk(address, buffer.data(), buffer.size());
#ifndef NDEBUG
                        // Optional sleep for time-sensitive tests (e.g. copy_prefix)
                        if (db0::Settings::__sleep_interval > 0) {
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(db0::Settings::__sleep_interval));
                        }                
#endif                
                    }
                    change_log_ptr = changelog_io.readChangeLogChunk();
                }
            }

            return max_state_num;

        } catch (db0::IOException &) {
            changelog_io.setStreamPos(stream_pos);
            chunks_buf.clear();            
            throw;
        }
    }
    
    bool isDRAM_ChunkValid(std::uint32_t dram_page_size, const o_dram_chunk_header &header,
        const void *data_begin, const void *data_end)
    {
        if (static_cast<const char*>(data_begin) + dram_page_size > static_cast<const char*>(data_end)) {
            THROWF(db0::IOException) << "isDRAM_ChunkValid: invalid chunk size";
        }
        // determine if valid by comparing header hash values (calculated vs stored)
        return !!header && header.calculateHash(data_begin, dram_page_size) == header.m_hash;
    }
    
    bool isDRAM_ChunkValid(std::uint32_t dram_page_size, const std::vector<char> &buffer)
    {
        // NOTE: the buffer already includes chunk header
        const auto &header = o_dram_chunk_header::__const_ref(buffer.data() + o_block_io_chunk_header::sizeOf());
        return isDRAM_ChunkValid(dram_page_size, header, header.getData(), buffer.data() + buffer.size());
    }
    
    StateNumType getDRAM_ChunkStateNum(const std::vector<char> &chunk_data)
    {
        const auto &header = o_dram_chunk_header::__const_ref(chunk_data.data() + o_block_io_chunk_header::sizeOf());
        return header.m_state_num;
    }
    
    void flushDRAM_IOChanges(DRAM_IOStream &dram_io,
        const std::unordered_map<std::uint64_t, std::vector<char> > &chunks_buf)
    {
        auto dram_page_size = dram_io.getDRAMPrefix().getPageSize();
        for (const auto &item: chunks_buf) {                
            auto address = item.first;
            const auto &buffer = item.second;
            // NOTE: we don't flush inconsistent / incomplete chunks
            if (!isDRAM_ChunkValid(dram_page_size, buffer)) {
                continue;
            }
            dram_io.writeToChunk(address, buffer.data(), buffer.size());
        }
    }
    
    void appendDRAM_IOChunks(DRAM_IOStream &dram_io, const std::vector<std::vector<char> > &chunks_buf)
    {
        auto dram_page_size = dram_io.getDRAMPrefix().getPageSize();
        for (const auto &buffer : chunks_buf) {
            if (!isDRAM_ChunkValid(dram_page_size, buffer)) {
                continue;
            }
            // NOTE: buffer already includes BlockIOStream's chunk header
            auto chunk_size = buffer.size() - o_block_io_chunk_header::sizeOf();
            auto chunk_data = buffer.data() + o_block_io_chunk_header::sizeOf();
            dram_io.addChunk(chunk_size);
            dram_io.appendToChunk(chunk_data, chunk_size);
        }
    }
    
    std::uint64_t o_dram_chunk_header::calculateHash(const void *data, std::size_t data_size) const {
        return db0::murmurhash64A(data, data_size, m_page_num + m_state_num);
    }

    bool o_dram_chunk_header::operator!() const {
        return m_state_num == 0 && m_page_num == 0 && m_hash == 0;
    }
    
}