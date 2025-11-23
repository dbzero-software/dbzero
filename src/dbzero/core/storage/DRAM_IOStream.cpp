#include "DRAM_IOStream.hpp"
#include "BlockIOStream.hpp"
#include <dbzero/core/utils/FlagSet.hpp>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include "ChangeLogIOStream.hpp"

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
    
    void *DRAM_IOStream::updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr,
        const o_dram_chunk_header &header)
    {
        // page map = page_num / state_num
        auto dram_page = m_page_map.find(header.m_page_num);
        if (dram_page == m_page_map.end() || dram_page->second.m_state_num < header.m_state_num) {
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
        } else {
            // mark block as reusable (read/write mode only)
            if (m_access_type == AccessType::READ_WRITE) {
                m_reusable_chunks.insert(address);
            }
        }
        return nullptr;
    }
    
    void DRAM_IOStream::updateDRAMPage(std::uint64_t address, std::unordered_set<std::size_t> *allocs_ptr,
        const o_dram_chunk_header &header, const void *bytes)
    {
        auto result = updateDRAMPage(address, allocs_ptr, header);
        if (result) {
            std::memcpy(result, bytes, m_dram_page_size);
        }
    }
    
    void DRAM_IOStream::load(ChangeLogIOStream &changelog_io)
    {
        assert(m_access_type == AccessType::READ_WRITE);

        // simply exhaust the change-log stream
        // its position marks the synchronization point
        while (changelog_io.readChangeLogChunk());

        std::vector<char> buffer(m_chunk_size, 0);
        const auto &header = o_dram_chunk_header::__ref(buffer.data());
        auto bytes = buffer.data() + header.sizeOf();
        
        // maximum known state number by page
        // this is required to only select the maximum state per page (discard older mutations)        
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
            
            updateDRAMPage(chunk_addr, &allocs, header, bytes);
        }
        m_allocator->update(allocs);
    }
    
    void DRAM_IOStream::flushUpdates(std::uint64_t state_num, ChangeLogIOStream &dram_changelog_io)
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
            // update to most recent location
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
                appendToChunk(&header, sizeof(header));
                appendToChunk(page_buffer, m_dram_page_size);
                dram_changelog.push_back(chunk_addr);
                // update to the last known page location, collect previous location as reusable
                update_page_location(page_num, chunk_addr);
            }
        });
        
        // flush all DRAM data updates before changelog updates
        BlockIOStream::flush();
        // output changelog, no RLE encoding, no duplicates
        ChangeLogData cl_data(std::move(dram_changelog), false, false, false);
        dram_changelog_io.appendChangeLog(std::move(cl_data));
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
    
    void DRAM_IOStream::beginApplyChanges(ChangeLogIOStream &changelog_io) const
    {
        assert(m_read_ahead_chunks.empty());
        if (m_access_type == AccessType::READ_WRITE) {
            THROWF(db0::InternalException) << "DRAM_IOStream::applyChanges require read-only stream";
        }
        
        fetchDRAM_IOChanges(*this, changelog_io, m_read_ahead_chunks);
    }
    
    bool DRAM_IOStream::completeApplyChanges()
    {
        bool result = false;
        for (const auto &item: m_read_ahead_chunks) {
            auto address = item.first;
            const auto &buffer = item.second;
            const auto &header = o_dram_chunk_header::__const_ref(buffer.data() + o_block_io_chunk_header::sizeOf());
            updateDRAMPage(address, nullptr, header, header.getData());
            result = true;
        }
        m_read_ahead_chunks.clear();        
        return result;
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
    
#ifndef NDEBUG 
    void DRAM_IOStream::getDRAM_IOMap(std::unordered_map<std::uint64_t, std::pair<std::uint64_t, std::uint64_t> > &io_map) const
    {
        for (auto &entry: m_page_map) {
            io_map[entry.first] = { entry.second.m_state_num, entry.second.m_address };
        }
    }
#endif

    void fetchDRAM_IOChanges(const DRAM_IOStream &dram_io, ChangeLogIOStream &changelog_io,
        std::unordered_map<std::uint64_t, std::vector<char> > &chunks_buf)
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
                
                // First collect the change log to only visit each address once
                std::unordered_set<std::uint64_t> addr_set;
                while (change_log_ptr) {
                    for (auto address: *change_log_ptr) {
                        if (addr_set.find(address) == addr_set.end()) {
                            addr_set.insert(address);
                        }
                    }
                    change_log_ptr = changelog_io.readChangeLogChunk();
                }
                
                // Visit the addresses next
                // this is important becase otherwise we might've been accessing outdated or inconsistent DPs
                for (auto address: addr_set) {
                    // buffer must include BlockIOStream's chunk header and data
                    auto &buffer = create_read_ahead_buffer(address, dram_io.getChunkSize() + o_block_io_chunk_header::sizeOf());
                    // the address reported in changelog must already be available in the stream
                    // it may come from a more recent update as well (and potentially may only be partially written)
                    // therefore chunk-level checksum validation is necessary
                    dram_io.readFromChunk(address, buffer.data(), buffer.size());
                }
            }

        } catch (db0::IOException &) {
            changelog_io.setStreamPos(stream_pos);
            chunks_buf.clear();            
            throw;
        }
    }
    
    void flushDRAM_IOChanges(DRAM_IOStream &dram_io,
        std::unordered_map<std::uint64_t, std::vector<char> > &chunks_buf)
    {
        for (const auto &item: chunks_buf) {
            auto address = item.first;
            const auto &buffer = item.second;
            dram_io.writeToChunk(address, buffer.data(), buffer.size());
        }
    }

}