// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "BlockIOStream.hpp"
#include "Page_IO.hpp"
#include "Diff_IO.hpp"
#include "DRAM_IOStream.hpp"
#include "ChangeLogTypes.hpp"
#include "ChangeLogIOStream.hpp"
#include "BaseStorage.hpp"

namespace db0

{
    
    class ExtSpace;    
    using DRAM_ChangeLogStreamT = db0::ChangeLogIOStream<BaseStorage::DRAM_ChangeLogT>;
    using DP_ChangeLogStreamT = db0::ChangeLogIOStream<BaseStorage::DP_ChangeLogT>;

    // This routine copies the entire DRAM_IO stream (from begin) in a manner
    // synchronized with the correspoding changelog stream
    // NOTE: output_changelog is NOT flushed (see the design)    
    // @return the finalal copied state number (unless nothing was copied - then std::nullopt)
    std::optional<StateNumType> copyDRAM_IO(DRAM_IOStream &input_io, DRAM_ChangeLogStreamT &input_dram_changelog,
        DRAM_IOStream &output_io, DRAM_ChangeLogStreamT::Writer &output_dram_changelog);
    
    using ChunkFilterT = std::function<bool(const std::vector<char> &chunk_buffer, const void *data_end)>;
    using DRAM_FilterT = std::function<bool(const o_dram_chunk_header &, const void *data_end)>;
    
    // Copy entire contents from one BlockIOStream to another (type agnostic)
    // @param addr_map optional map to receive address translation (from source to destination)
    // @param chunk_filter optional filter to decide whether a specific chunk is to be copied
    // @param copy_all if true then all chunks are copy attempted, otherwise will stop copying on first non-matched, 
    // this parameter only makes sense when chunk_filter is provided
    // @return the last copied chunk data
    std::vector<char> copyStream(BlockIOStream &in, BlockIOStream &out,
        std::unordered_map<std::uint64_t, std::uint64_t> *addr_map = nullptr, 
        ChunkFilterT chunk_filter = {}, bool copy_all = true);
    
    // DP-changelog specialization
    // @param max_state_num the maximum state number to be copied (inclusive)
    // @return the last chunk's header (if anything copied)
    std::optional<o_dp_changelog_header> copyDPStream(DP_ChangeLogStreamT &in, DP_ChangeLogStreamT &out, 
        StateNumType max_state_num);
    
    // Copy raw contents of a specific Page_IO up to a specific storage page number
    // @param in the input (source) Page_IO (must NOT define ext-space - i.e. absolute / relative mapping)
    // @param src_ext_space the source ExtSpace (to retrieve relative mappings if any)
    // @param out the output Page_IO
    // @param end_page_num the storage page number (not to be exceeded on copy)
    // @param ext_space the ExtSpace to assign new relative page numbers on copy
    // NOTE: after copy the source "absolute" page numbers will be corresponding do destination's relative page numbers
    // therefore we have no need to translate the source DRAM_IO
    void copyPageIO(const Page_IO &in, const ExtSpace &src_ext_space, Page_IO &out, 
        std::uint64_t end_page_num, ExtSpace &ext_space);
    
}