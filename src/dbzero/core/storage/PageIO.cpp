#include "PageIO.hpp"
#include <iostream>
#include <cassert>

namespace db0

{

    PageIO::PageIO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size,
        std::uint64_t address, std::uint32_t page_count, std::function<std::uint64_t()> tail_function)  
        : m_header_size(header_size)
        , m_file(file)
        , m_page_size(page_size)
        , m_block_size(block_size)
        , m_block_capacity(block_size / page_size)
        , m_address(address)
        , m_page_count(page_count)
        , m_first_page_num(getPageNum(address))
        , m_tail_function(tail_function)
        , m_access_type(AccessType::READ_WRITE)
    {
    }
    
    PageIO::PageIO(std::size_t header_size, CFile &file, std::uint32_t page_size)
        : m_header_size(header_size)
        , m_file(file)
        , m_page_size(page_size)
        , m_access_type(AccessType::READ_ONLY)
    {
    }

    std::uint64_t PageIO::append(const void *buffer)
    {
        if (m_access_type != AccessType::READ_WRITE) {
            THROWF(db0::InternalException) << "PageIO instance is not writable";
        }
        if (m_page_count == m_block_capacity) {
            // allocate the next block by appending it to the file
            m_address = std::max(this->tail(), m_tail_function());
            m_first_page_num = getPageNum(m_address);
            m_page_count = 0;
        }
        
        m_file.write(m_address + m_page_count * m_page_size, m_page_size, buffer);
        return m_first_page_num + (m_page_count++);
    }

    void PageIO::read(std::uint64_t page_num, void *buffer) const {
        m_file.read(m_header_size + page_num * m_page_size, m_page_size, buffer);
    }
    
    void PageIO::write(std::uint64_t page_num, void *buffer) {
        m_file.write(m_header_size + page_num * m_page_size, m_page_size, buffer);
    }
    
    std::uint64_t PageIO::getPageNum(std::uint64_t address) const {
        return ((address - m_header_size) / m_block_size) * m_block_capacity;
    }

    std::uint64_t PageIO::tail() const {
        assert(m_access_type == AccessType::READ_WRITE);
        return m_address + m_block_size;
    }

}