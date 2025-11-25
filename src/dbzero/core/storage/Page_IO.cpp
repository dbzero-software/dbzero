#include "Page_IO.hpp"
#include <iostream>
#include <cassert>

namespace db0

{

    Page_IO::Page_IO(std::size_t header_size, CFile &file, std::uint32_t page_size, std::uint32_t block_size,
        std::uint64_t address, std::uint32_t page_count, std::uint32_t step_size, std::function<std::uint64_t()> tail_function,
        std::optional<std::uint32_t> block_num)
        : m_header_size(header_size)
        , m_page_size(page_size)
        , m_block_size(block_size)
        , m_block_capacity(block_size / page_size)
        , m_step_size(step_size)
        , m_file(file)
        , m_address(address)
        , m_page_count(page_count)
        , m_first_page_num(getPageNum(address))        
        , m_tail_function(tail_function)
        , m_access_type(AccessType::READ_WRITE)
        , m_block_num(block_num)
    {
        assert(block_size % page_size == 0);
    }
    
    Page_IO::Page_IO(std::size_t header_size, CFile &file, std::uint32_t page_size)
        : m_header_size(header_size)        
        , m_page_size(page_size)        
        , m_file(file)        
        , m_access_type(AccessType::READ_ONLY)
    {
    }
    
    Page_IO::~Page_IO()
    {
    }
    
    std::uint64_t Page_IO::append(const void *buffer)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (m_page_count == m_block_capacity) {
            allocateNextBlock();
        }
        
        m_file.write(m_address + m_page_count * m_page_size, m_page_size, buffer);
        return m_first_page_num + (m_page_count++);
    }
    
    void Page_IO::allocateNextBlock()
    {
        if (m_block_num && *m_block_num < (m_step_size - 1)) {
            // allocate next block within the step
            m_address += m_block_size;
            m_first_page_num += m_block_capacity;
            m_page_count = 0;
            ++(*m_block_num);
        } else {
            // allocate the next step / block by appending it to the file            
            m_address = std::max(this->tail(), m_tail_function());
            m_first_page_num = getPageNum(m_address);
            m_page_count = 0;
            // initiate the next full step
            m_block_num = 0;
        }
    }
    
    void Page_IO::read(std::uint64_t page_num, void *buffer) const {
        m_file.read(m_header_size + page_num * m_page_size, m_page_size, buffer);
    }
    
    void Page_IO::write(std::uint64_t page_num, void *buffer) {
        m_file.write(m_header_size + page_num * m_page_size, m_page_size, buffer);
    }
    
    std::uint64_t Page_IO::getPageNum(std::uint64_t address) const {
        return ((address - m_header_size) / m_block_size) * m_block_capacity;
    }
    
    std::uint64_t Page_IO::tail() const
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (m_block_num) {
            // reserve space up to end of the step
            return m_address + (m_step_size - *m_block_num) * m_block_size;
        } else {
            // step not known, return end of current block
            return m_address + m_block_size;
        }
    }
    
    std::uint32_t Page_IO::getPageSize() const {
        return m_page_size;        
    }
    
    std::pair<std::uint64_t, std::uint32_t> Page_IO::getNextPageNum()
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (m_page_count == m_block_capacity) {
            allocateNextBlock();
        }
        return { m_first_page_num + m_page_count, m_block_capacity - m_page_count };
    }
    
    std::uint64_t Page_IO::getEndPageNum() const
    {
        assert(m_access_type == AccessType::READ_WRITE);
        return m_first_page_num + m_page_count;        
    }
    
}