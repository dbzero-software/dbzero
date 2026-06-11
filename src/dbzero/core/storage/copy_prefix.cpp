// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "copy_prefix.hpp"
#include "ExtSpace.hpp"

namespace db0

{
    
    // chunk buffer + append buffer
    using ChunkBufPair = std::pair<
        std::unordered_map<std::uint64_t, std::vector<char> >,
        std::vector<std::vector<char> > >;
    
    ChunkBufPair translateDRAM_Chunks(
        const std::unordered_map<std::uint64_t, std::vector<char> > &&chunk_buf,
        const std::unordered_map<std::uint64_t, std::uint64_t> &addr_map)
    {
        ChunkBufPair result;        
        for (const auto &pair : chunk_buf) {
            auto it_addr = addr_map.find(pair.first);
            if (it_addr == addr_map.end()) {
                // not present in the copied stream, must be appended
                result.second.emplace_back(std::move(pair.second));
            } else {
                // register under translated address
                result.first.emplace(it_addr->second, std::move(pair.second));
            }
        }
        return result;
    }
    
    std::unordered_map<std::uint64_t, std::vector<char> > filterDRAM_Chunks(
        std::unordered_map<std::uint64_t, std::vector<char> > &&chunk_buf, DRAM_FilterT filter)
    {
        auto it = chunk_buf.begin();
        while (it != chunk_buf.end()) {
            // NOTE: this buffer also contains the block IO header at the beginning
            const auto &buffer = it->second;
            const auto &header = o_dram_chunk_header::__const_ref(buffer.data() + o_block_io_chunk_header::sizeOf());
            if (!filter(header, buffer.data() + buffer.size())) {
                // discard this chunk
                it = chunk_buf.erase(it);
            } else {
                ++it;
            }
        }

        return chunk_buf;
    }
    
    std::optional<StateNumType> copyDRAM_IO(DRAM_IOStream &input_io, DRAM_ChangeLogStreamT &input_dram_changelog,
        DRAM_IOStream &output_io, DRAM_ChangeLogStreamT::Writer &output_dram_changelog,
        std::optional<StateNumType> max_state_num)
    {
        using DRAM_ChangeLogT = DRAM_IOStream::DRAM_ChangeLogT;

        // Exhaust the input_dram_changelog first
        // NOTE: we don't need to copy the changelog, just insert an empty item with the latest state number
        input_dram_changelog.setStreamPosHead();
        std::optional<StateNumType> maybe_state_num;
        for (;;) {
            while (auto change_log = input_dram_changelog.readChangeLogChunk()) {
                if (!max_state_num || change_log->m_state_num <= *max_state_num) {
                    maybe_state_num = change_log->m_state_num;
                }
            }
            // continue refreshing until reaching the most recent state
            if (!input_dram_changelog.refresh()) {
                break;
            }
        }

        if (!maybe_state_num) {
            // looks like the DRAM IO is empty
            return {};
        }
        auto state_num = *maybe_state_num;

        // Copy the entire DRAM_IO stream next (possibly inconsistent state)
        // collecting the mapping of chunk addresses
        // NOTE: when copying we ignore: a) incomplete pages (hash mismatch), b) pages beyond the last consistent state number
        // they will be processed later when following up with the changelog
        std::unordered_map<std::uint64_t, std::uint64_t> chunk_addr_map;
        auto dram_page_size = input_io.getDRAMPageSize();
        auto dram_filter = [&](const o_dram_chunk_header &header, const void *data_end) -> bool
        {
            if (!isDRAM_ChunkValid(dram_page_size, header, header.getData(), data_end)) {
                return false;
            }
            // reject chunks beyond the last consistent state number
            return header.m_state_num <= state_num;
        };
        
        auto chunk_filter = [&](const std::vector<char> &buffer, const void *data_end) -> bool
        {
            const auto &header = o_dram_chunk_header::__const_ref(buffer.data());
            return dram_filter(header, data_end);
        };
        
        copyStream(input_io, output_io, &chunk_addr_map, chunk_filter);

        if (max_state_num) {
            output_dram_changelog.appendChangeLog({}, state_num, DRAMChangeLogKind::DRAM_IO);
            output_io.addChunk(0);
            output_io.BlockIOStream::flush();
            return state_num;
        }

        // NOTE: the operation might need to be repeated multiple times
        // if unable to reach a consistent state in one pass (this might be due to a very slow reader process)
        for (;;) {
            // Chunks loaded during  the sync step
            // NOTE: in this step we prefetch to memory to be able to catch up with changes
            std::unordered_map<std::uint64_t, std::vector<char> > chunk_buf;
            while (input_dram_changelog.refresh()) {
                if (auto maybe_state_num = fetchDRAM_IOChanges(input_io, input_dram_changelog, chunk_buf)) {
                    // this is the actually copied last consistent state number
                    state_num = *maybe_state_num;
                }
            }

            // NOTE: at this stage we might also encounter incomplete
            // or new chunks beyond the copied stream which needs to be discarded
            chunk_buf = filterDRAM_Chunks(std::move(chunk_buf), dram_filter);

            // NOTE: flush must be done under translated addresses (or appended to stream if translation not present)
            auto bufs_pair = translateDRAM_Chunks(std::move(chunk_buf), chunk_addr_map);
            flushDRAM_IOChanges(output_io, bufs_pair.first);
            // append new chuks which were not present during the initial copy
            appendDRAM_IOChunks(output_io, bufs_pair.second);                
            // append the sentinel entry with state number only (i.e. empty changelog)
            output_dram_changelog.appendChangeLog({}, state_num, DRAMChangeLogKind::DRAM_IO);
            
            // this operation needs to be continued until exhausting the entire changelog
            if (input_dram_changelog.refresh()) {
               continue;
            } else {
                break;
            }
        }

        output_io.addChunk(0);
        output_io.BlockIOStream::flush();
        return state_num;
    }
    
    std::vector<char> copyStream(BlockIOStream &in, BlockIOStream &out,
        std::unordered_map<std::uint64_t, std::uint64_t> *addr_map, ChunkFilterT filter, bool copy_all)
    {
        // position at the beginning of the stream
        in.setStreamPosHead();
        std::vector<char> buffer;
        std::size_t chunk_size = 0;
        std::uint64_t in_addr, out_addr;
        bool stop_copying = false;
        while (!stop_copying) {
            while ((chunk_size = in.readChunk(buffer, 0, &in_addr)) > 0) {
                // NOTE: this buffer does NOT include the block IO header at the beginning                
                if (filter && !filter(buffer, buffer.data() + chunk_size)) {
                    // stop copying entirely
                    if (!copy_all) {
                        stop_copying = true;
                        break;
                    }
                    // skip this chunk only
                    continue;
                }
                
                out.addChunk(chunk_size, &out_addr);
                out.appendToChunk(buffer.data(), chunk_size);
                // register the mapping
                if (addr_map) {
                    addr_map->emplace(in_addr, out_addr);
                }

#ifndef NDEBUG
                if (db0::Settings::__sleep_interval > 0) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(db0::Settings::__sleep_interval));
                }
#endif
            }
            if (!in.refresh()) {
                // continue refreshing until reaching the most recent state
                break;
            }
        }
        out.flush();
        return buffer;
    }
    
    std::optional<o_dp_changelog_header> copyDPStream(DP_ChangeLogStreamT &in, DP_ChangeLogStreamT &out,
        StateNumType max_state_num)
    {
        using o_change_log_t = DP_ChangeLogStreamT::ChangeLogT;
        in.setStreamPosHead();
        std::vector<char> buffer;
        std::size_t chunk_size = 0;
        std::optional<o_dp_changelog_header> result;
        while ((chunk_size = in.readChunk(buffer)) > 0) {
            const auto &header = o_change_log_t::__const_ref(buffer.data());
            if (header.m_state_num > max_state_num) {
                break;
            }

            out.addChunk(chunk_size);
            out.appendToChunk(buffer.data(), chunk_size);
            result = header;

            if (header.m_state_num == max_state_num) {
                break;
            }
        }
        out.flush();
        return result;
    }
    
    // Debug & validation function - to compare pages of the 2 streams (e.g. source and copy)
    // NOTE: both streams may store under different absolute page numbers but same relative
    // @param rel_page_num relative page number in the ExtSpace
    bool comparePages(const Page_IO &first, const ExtSpace &first_ext_space, const Page_IO &second,
        const ExtSpace &second_ext_space, std::uint64_t rel_page_num)
    {
        if (first.getPageSize() != second.getPageSize()) {
            THROWF(db0::IOException) << "comparePages: page size mismatch between input streams";
        }
        auto page_size = first.getPageSize();
        auto page_num_1 = rel_page_num;
        if (!!first_ext_space) {
            page_num_1 = first_ext_space.getAbsolute(rel_page_num);
            assert(rel_page_num == first_ext_space.getRelative(page_num_1));
        }
        auto page_num_2 = rel_page_num;
        if (!!second_ext_space) {
            page_num_2 = second_ext_space.getAbsolute(rel_page_num);
            assert(rel_page_num == second_ext_space.getRelative(page_num_2));
        }
        std::vector<std::byte> buf_1(page_size);
        first.read(page_num_1, buf_1.data());
        std::vector<std::byte> buf_2(page_size);
        second.read(page_num_2, buf_2.data());
        return memcmp(buf_1.data(), buf_2.data(), page_size) == 0;
    }
    
    void copyPageIO(const Page_IO &in, const ExtSpace &src_ext_space, Page_IO &out,
        std::uint64_t end_page_num, ExtSpace &ext_space)
    {
        std::size_t page_size = in.getPageSize();
        if (page_size != out.getPageSize()) {
            THROWF(db0::IOException) << "copyPageIO: page size mismatch between input and output streams";
        }
        
        Page_IO::Reader reader(in, src_ext_space, end_page_num);        
        std::vector<std::byte> buffer;
        std::uint64_t start_page_num = 0;
        while (auto page_count = reader.next(buffer, start_page_num)) {
            auto buf_ptr = buffer.data();
            if (!!src_ext_space) {
                // translate to relative page number
                start_page_num = src_ext_space.getRelative(start_page_num);
            }
            while (page_count > 0) {
                // page number (absolute) in the output stream
                auto storage_page_num = out.getNextPageNum().first;
                auto count = std::min(page_count, out.getCurrentStepRemainingPages());
                // append as many pages as possible in the current "step"
                out.append(buf_ptr, count);
                buf_ptr += page_size * count;
                // note start_page_num must be registered as relative to storage_page_num
                // note each step might require its own mapping (unless stored as consecutive pages)
                // the de-duplication logic is handled by ExtSpace
                ext_space.addMapping(storage_page_num, start_page_num, count);
                page_count -= count;
                start_page_num += count;
            }
        }        
    }
    
}
