#pragma once

#include "BlockIOStream.hpp"
#include "Page_IO.hpp"
#include "Diff_IO.hpp"
#include "DRAM_IOStream.hpp"
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
    void copyDRAM_IO(DRAM_IOStream &input_io, DRAM_ChangeLogStreamT &input_dram_changelog,
        DRAM_IOStream &output_io, DRAM_ChangeLogStreamT::Writer &output_dram_changelog);
    
    // Copy entire contents from one BlockIOStream to another (type agnostic)
    // @return the last copied chunk data
    std::vector<char> copyStream(BlockIOStream &in, BlockIOStream &out);
    
    // DP-changelog specialization
    // @return the end storage page number (if anything copied)
    std::optional<std::uint64_t> copyDPStream(DP_ChangeLogStreamT &in, DP_ChangeLogStreamT &out);
    
    // Copy raw contents of a specific Page_IO up to a specific storage page number
    // @param in the input (source) Page_IO (must NOT define ext-space - i.e. absolute / relative mapping)
    // @param out the output Page_IO
    // @param end_page_num the storage page number (not to be exceeded on copy)
    // @param ext_space the ExtSpace to assign new relative page numbers on copy
    // NOTE: after copy the source "absolute" page numbers will be corresponding do destination's relative page numbers
    // therefore we have no need to translate the source DRAM_IO
    void copyPageIO(const Page_IO &in, Page_IO &out, std::uint64_t end_page_num, ExtSpace &ext_space);
    
}