#pragma once

#include "CFile.hpp"
#include <functional>

namespace db0

{
    
    /**
     * Page_IO organizes file's data into blocks of pages
     * pages are identified by absolute numbers which enables fast address calculation
    */
    class Page_IO
    {
    public: 
        // Read/write Page_IO
        // @param header_size the fixed file-level offset to be taken into account when calculating page address
        // @param file the underlying file object
        // @param page_size the size of a single page in bytes
        // @param block_size size of a unit block of pages to be pre-allocated by the stream
        // @param address of the currently active block
        // @param page_count the number of pages already stored in the current block
        // @param tail_function a function returning current (unflushed) size of the file
        Page_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address,
            std::uint32_t page_count, std::function<std::uint64_t()> tail_function);
        
        // Read-only Page_IO
        Page_IO(std::size_t header_size, CFile &file, std::uint32_t page_size);
        
        ~Page_IO();

        // Appends a new page to the stream
        // @return page number (aka storage page number)
        std::uint64_t append(const void *buffer);
        
        void read(std::uint64_t page_num, void *buffer) const;
        
        /**
         * Overwrite existing page
        */
        void write(std::uint64_t page_num, void *buffer);
        
        std::uint64_t tail() const;

        std::uint32_t getPageSize() const;

    protected:
        const std::size_t m_header_size;        
        const std::uint32_t m_page_size;
        const std::uint32_t m_block_size = 0;
        // maximum number of pages in block
        const std::uint32_t m_block_capacity = 0;

        // Get the next page number to be assigned by the "append" method (first)
        // and the number of consecutive pages available in the current block
        std::pair<std::uint64_t, std::uint32_t> getNextPageNum();

    private:
        CFile &m_file;
        // begin address of the current block
        std::uint64_t m_address = 0;
        // the number of pages already stored in current block
        std::uint32_t m_page_count = 0;
        // number of the 1st page in current block
        std::uint64_t m_first_page_num = 0;
        std::function<std::uint64_t()> m_tail_function;
        const AccessType m_access_type;

        std::uint64_t getPageNum(std::uint64_t address) const;
        void allocateNextBlock();
    };

}