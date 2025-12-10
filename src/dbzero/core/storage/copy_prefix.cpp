// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "copy_prefix.hpp"
#include "ExtSpace.hpp"

namespace db0

{
    
    void copyDRAM_IO(DRAM_IOStream &input_io, DRAM_ChangeLogStreamT &input_dram_changelog,
        DRAM_IOStream &output_io, DRAM_ChangeLogStreamT::Writer &output_dram_changelog)
    {
        using DRAM_ChangeLogT = DRAM_IOStream::DRAM_ChangeLogT;

        // Exhaust the input_dram_changelog first
        input_dram_changelog.setStreamPosHead();
        auto change_log_ptr = input_dram_changelog.readChangeLogChunk();
        
        while (change_log_ptr) {
            output_dram_changelog.appendChangeLog(*change_log_ptr);
            change_log_ptr = input_dram_changelog.readChangeLogChunk();
        }
        
        // Copy the entire DRAM_IO stream next (possibly inconsistent state)
        copyStream(input_io, output_io);
        
        // Callback to copy additional changelog chunks retrieved on post-sync
        auto copy_callback = [&](const DRAM_ChangeLogT &change_log) {
            output_dram_changelog.appendChangeLog(change_log);
        };

        // Chunks loaded during  the sync step
        // NOTE: in this step we prefetch to memory to be able to catch up with changes
        std::unordered_map<std::uint64_t, std::vector<char> > chunk_buf;
        fetchDRAM_IOChanges(input_io, input_dram_changelog, chunk_buf, copy_callback);
        flushDRAM_IOChanges(output_io, chunk_buf);
    }
    
    std::vector<char> copyStream(BlockIOStream &in, BlockIOStream &out)
    {
        // position at the beginning of the stream
        in.setStreamPosHead();
        std::vector<char> buffer;
        std::size_t chunk_size = 0;
        while ((chunk_size = in.readChunk(buffer)) > 0) {
            out.addChunk(chunk_size);
            out.appendToChunk(buffer.data(), chunk_size);
        }
        out.flush();
        return buffer;
    }
    
    std::optional<o_dp_changelog_header> copyDPStream(DP_ChangeLogStreamT &in, DP_ChangeLogStreamT &out)
    {
        auto last_chunk_buf = copyStream(in, out);
        // we can retrieve the end page number from the last appended chunk        
        if (last_chunk_buf.empty()) {            
            // nothing copied
            return {};
        }
        
        using o_change_log_t = DP_ChangeLogStreamT::ChangeLogT;
        return o_change_log_t::__const_ref(last_chunk_buf.data());        
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