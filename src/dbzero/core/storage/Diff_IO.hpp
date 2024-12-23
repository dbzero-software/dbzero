#include "Page_IO.hpp"
#include "DiffWriter.hpp"
#include "diff_buffer.hpp"

namespace db0

{

    // Diff_IO is a Page_IO extension specialized in
    // storage & retrieval of diff sequences
    class Diff_IO: protected Page_IO
    {
    public:
        Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address, 
            std::uint32_t page_count, std::function<std::uint64_t()> tail_function);
        // Read-only Diff_IO
        Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size);
        
        // Appends a new diff-block to the stream
        // NOTE: that the diff-block may be stored on 2 pages in which case the number of the first one is returned
        // and the continuation page number will be stored in the page footer
        // @return page number
        std::uint64_t append(const o_diff_buffer &);
        
        // Flush needs to be called before closing the stream
        // and after each transaction
        void flush();

        std::uint64_t tail() const;

        std::uint32_t getPageSize() const;

    protected:
        // the data buffer for the current page being collected        
        std::vector<std::byte> m_data_buf;
        // DiffWriter m_dp_writer;
    };
    
}