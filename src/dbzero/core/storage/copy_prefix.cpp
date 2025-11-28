#include "copy_prefix.hpp"
#include "ExtSpace.hpp"

namespace db0

{
    
    void copyDRAM_IO(DRAM_IOStream &input_io, DRAM_ChangeLogStreamT &input_dram_changelog,
        DRAM_IOStream &output_io, DRAM_ChangeLogStreamT::Writer &output_dram_changelog)
    {
        // Exhaust the input_dram_changelog first
        input_dram_changelog.setStreamPosHead();
        auto change_log_ptr = input_dram_changelog.readChangeLogChunk();
        
        while (change_log_ptr) {
            output_dram_changelog.appendChangeLog(*change_log_ptr);
            change_log_ptr = input_dram_changelog.readChangeLogChunk();
        }
        
        // Copy the entire DRAM_IO stream next
        copyStream(input_io, output_io);

        // Chunks loaded during  the sync step
        // NOTE: in this step we prefetch to memory to be able to catch up with changes
        std::unordered_map<std::uint64_t, std::vector<char> > chunk_buf;
        fetchDRAM_IOChanges(input_io, input_dram_changelog, chunk_buf);
        flushDRAM_IOChanges(output_io, chunk_buf);
    }
    
    std::vector<char> copyStream(BlockIOStream &in, BlockIOStream &out)
    {
        // FIXME: log
        std::cout << "Copying stream from tail: " << in.tail() << std::endl;
        // position at the beginning of the stream
        in.setStreamPosHead();
        std::vector<char> buffer;
        std::size_t chunk_size = 0;
        while ((chunk_size = in.readChunk(buffer)) > 0) {
            // FIXME: log
            std::cout << "Copying chunk of size: " << chunk_size << std::endl;
            out.addChunk(chunk_size);
            out.appendToChunk(buffer.data(), chunk_size);
        }
        out.flush();
        return buffer;
    }
    
    std::uint64_t copyDPStream(DP_ChangeLogStreamT &in, DP_ChangeLogStreamT &out)
    {
        auto last_chunk_buf = copyStream(in, out);
        // we can retrieve the end page number from the last appended chunk        
        if (last_chunk_buf.empty()) {
            // nothing copied
            return 0;
        }

        using o_change_log_t = DP_ChangeLogStreamT::ChangeLogT;
        auto &last_chunk = o_change_log_t::__const_ref(last_chunk_buf.data());
        return last_chunk.m_end_storage_page_num;
    }
    
    void copyPageIO(const Page_IO &in, Page_IO &out, std::uint64_t end_page_num, ExtSpace &ext_space)
    {
        std::size_t page_size = in.getPageSize();
        if (page_size != out.getPageSize()) {
            THROWF(db0::IOException) << "copyPageIO: page size mismatch between input and output streams";
        }
        
        Page_IO::Reader reader(in, end_page_num);
        std::vector<std::byte> buffer;
        std::uint64_t start_page_num = 0;
        while (auto page_count = reader.next(buffer, start_page_num)) {
            auto buf_ptr = buffer.data();
            while (page_count > 0) {
                // FIXME: log
                std::cout << "Copy page count: " << page_count << " starting at page num: " << start_page_num << std::endl;
                // page number (absolute) in the output stream
                auto storage_page_num = out.getNextPageNum().first;
                auto count = std::min(page_count, out.getCurrentStepRemainingPages());
                // append as many pages as possible in current "step"                
                out.append(buf_ptr, count);
                buf_ptr += page_size * count;
                // note start_page_num must be registered as relative to storage_page_num
                ext_space.addMapping(storage_page_num, start_page_num);
                page_count -= count;
            }
        }
    }
    
}