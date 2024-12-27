#include "Page_IO.hpp"
#include "diff_buffer.hpp"
#include <memory>

namespace db0

{

    class DiffWriter;

    // Diff_IO is a Page_IO extension specialized in
    // storage & retrieval of diff sequences
    class Diff_IO: public Page_IO
    {
    public:
        Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address, 
            std::uint32_t page_count, std::function<std::uint64_t()> tail_function);
        // Read-only Diff_IO
        Diff_IO(std::size_t header_size, CFile &file, std::uint32_t page_size);
        ~Diff_IO();
        
        // Appends a new diff-block to the stream
        // NOTE: that the diff-block may be stored on 2 pages in which case the number of the first one is returned
        // and the continuation page number will be stored in the page header (continuation page number)
        // @param dp_data the data page buffer
        // @param page_and_state relative logical page and state numbers to mark the diff block
        // @param diff_data the diff buffer (see getDiffs)
        // @return page number
        std::uint64_t appendDiff(const void *dp_data, std::pair<std::uint64_t, std::uint32_t> page_and_state,
            const std::vector<std::uint16_t> &diff_data);
        
        // Read diff stream and apply changes to the DP-buffer (must be already populated with the base data)
        // @param page_num the storage page number to read from
        // @param buffer the buffer to hold the resulting data page
        // @param page_and_state logical page and state numbers (possibly relative) to identify the diff block
        // Exception raised if the diff block is not found
        void applyFrom(std::uint64_t page_num, void *buffer, std::pair<std::uint64_t, std::uint32_t> page_and_state) const;

        // Flush needs to be called before closing the stream
        // and after each transaction
        void flush();
        
        // Write as full-DP
        void write(std::uint64_t page_num, void *buffer);
        
    protected:
        mutable std::mutex m_mx_write;
        // the data buffer to hold up to 2 data pages
        std::vector<std::byte> m_write_buf;
        mutable std::mutex m_mx_read;
        mutable std::vector<std::byte> m_read_buf;
        std::unique_ptr<DiffWriter> m_writer;
    };
    
}