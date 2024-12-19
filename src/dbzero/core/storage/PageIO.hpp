#pragma once

#include "CFile.hpp"
#include <functional>

namespace db0

{
    
    /**
     * PageIO organizes file's data into blocks of pages
     * pages are identified by absolute numbers which enables fast address calculation
    */
    class PageIO
    {
    public: 
        // Read/write PageIO
        PageIO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size, std::uint64_t address,
            std::uint32_t page_count, std::function<std::uint64_t()> tail_function);
        // Read-only PageIO
        PageIO(std::size_t header_size, CFile &file, std::uint32_t page_size);

        // return page number
        std::uint64_t append(const void *buffer);

        void read(std::uint64_t page_num, void *buffer) const;
        
        /**
         * Overwrite existing page
        */
        void write(std::uint64_t page_num, void *buffer);

        std::uint64_t tail() const;

        std::uint32_t getPageSize() const;

    private:
        const std::size_t m_header_size;
        CFile &m_file;
        const std::uint32_t m_page_size;
        const std::uint32_t m_block_size = 0;
        // maximum number of pages in block
        const std::uint32_t m_block_capacity = 0;
        // begin address of the current block
        std::uint64_t m_address = 0;
        // the number of pages already stored in current block
        std::uint32_t m_page_count = 0;
        // number of the 1st page in current block
        std::uint64_t m_first_page_num = 0;
        std::function<std::uint64_t()> m_tail_function;
        const AccessType m_access_type;

        std::uint64_t getPageNum(std::uint64_t address) const;
    };

}