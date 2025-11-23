#pragma once

#include "BlockIOStream.hpp"
#include "Page_IO.hpp"
#include "Diff_IO.hpp"
#include "DRAM_IOStream.hpp"
#include "ChangeLogIOStream.hpp"

namespace db0

{
    
    // This routine copies the entire DRAM_IO stream (from begin) in a manner
    // synchronized with the correspoding changelog stream
    // NOTE: output_changelog is NOT flushed (see the design)    
    void copyDRAM_IO(DRAM_IOStream &input_io, ChangeLogIOStream &input_dram_changelog,
        DRAM_IOStream &output_io, ChangeLogIOStream::Writer &output_dram_changelog);
    
    // Copy entire contents from one BlockIOStream to another
    void copyStream(BlockIOStream &in, BlockIOStream &out);
    
    void copyPageIO(const Page_IO &input_io, const ChangeLogIOStream &input_dp_changelog,
        Page_IO &output_io, ChangeLogIOStream::Writer &output_dp_changelog);
    
}