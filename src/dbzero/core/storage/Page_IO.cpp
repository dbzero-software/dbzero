// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Page_IO.hpp"
#include <iostream>
#include <cassert>

namespace db0

{
    void ReservePool::add(std::uint64_t page_num, std::uint32_t page_count)
    {
        if (page_count == 0) {
            return;
        }
        if (!m_strides.empty() && m_strides.back().m_page_num + m_strides.back().m_page_count == page_num) {
            m_strides.back().m_page_count += page_count;
        } else {
            m_strides.push_back({ page_num, page_count });
        }
    }

    bool ReservePool::empty() const
    {
        return m_strides.empty();
    }

    std::pair<std::uint64_t, std::uint32_t> ReservePool::next() const
    {
        assert(!m_strides.empty());
        return { m_strides.front().m_page_num, m_strides.front().m_page_count };
    }

    std::uint64_t ReservePool::pop()
    {
        auto result = tryPop(1);
        assert(result);
        return *result;
    }

    std::optional<std::uint64_t> ReservePool::tryPop(std::uint32_t page_count)
    {
        assert(page_count > 0);
        if (m_strides.empty() || m_strides.front().m_page_count < page_count) {
            return {};
        }

        auto result = m_strides.front().m_page_num;
        m_strides.front().m_page_num += page_count;
        m_strides.front().m_page_count -= page_count;
        if (m_strides.front().m_page_count == 0) {
            m_strides.pop_front();
        }
        return result;
    }

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
        assert(m_address == m_header_size + m_first_page_num * m_page_size);
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
    
    std::uint64_t Page_IO::append(const void *buffer, bool *is_first_page_ptr)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (!m_reserve_pool.empty()) {
            auto page_num = m_reserve_pool.pop();
            if (is_first_page_ptr) {
                *is_first_page_ptr = isFirstPageInStep(page_num);
            }
            write(page_num, buffer);
            return page_num;
        }

        if (m_page_count == m_block_capacity) {
            allocateNextBlock();
        }
        
        if (is_first_page_ptr) {
            // first page of the first block in the step
            *is_first_page_ptr = isFirstPageInStep(m_first_page_num + m_page_count);
        }

        m_file.write(m_address + m_page_count * m_page_size, m_page_size, buffer);
        return m_first_page_num + (m_page_count++);
    }
    
    std::uint64_t Page_IO::append(const void *buffer, std::uint64_t page_count)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (page_count == 1) {
            return append(buffer);
        }
        
        if (auto available_page_num = m_reserve_pool.tryPop(page_count)) {
            auto result = *available_page_num;
            m_file.write(m_header_size + result * m_page_size, page_count * m_page_size, buffer);
            return result;
        }

        if (m_page_count == m_block_capacity) {
            allocateNextBlock();
        }

        auto result = m_first_page_num + m_page_count;
        auto step_remaining = getCurrentStepRemainingPages();
        if (page_count > step_remaining) {
            THROWF(db0::InternalException)
                << "Page_IO::append: multi-page append must fit in the current consecutive step";
        }
        const std::byte *byte_buffer = static_cast<const std::byte *>(buffer);        
        auto to_write_bytes = page_count * m_page_size;
        m_file.write(m_address + m_page_count * m_page_size, to_write_bytes, byte_buffer);
        byte_buffer += to_write_bytes;
        moveBy(page_count);
        return result;
    }

    std::uint64_t Page_IO::reserve(std::uint32_t page_count, bool *is_first_page_ptr)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (page_count == 0) {
            THROWF(db0::InternalException)
                << "Page_IO::reserve: page count must be greater than zero";
        }

        if (m_page_count == m_block_capacity) {
            allocateNextBlock();
        }

        if (m_block_num) {
            if (page_count > m_step_size * m_block_capacity) {
                THROWF(db0::InternalException) 
                    << "Page_IO::reserve: unable to reserve more pages than fit in a step";
            }
            while (getCurrentStepRemainingPages() < page_count) {
                auto step_remaining = getCurrentStepRemainingPages();
                collectReservePool(step_remaining);
                moveBy(step_remaining);
                allocateNextBlock();
            }
        } else if (page_count > (m_block_capacity - m_page_count)) {
            THROWF(db0::InternalException) 
                << "Page_IO::reserve: unable to reserve a contiguous range without step access";
        }

        if (is_first_page_ptr) {
            *is_first_page_ptr = isFirstPageInStep(m_first_page_num + m_page_count);
        }
        auto result = m_first_page_num + m_page_count;
        if (m_block_num) {
            moveBy(page_count);
        } else {
            m_page_count += page_count;
        }
        return result;
    }
    
    void Page_IO::allocateNextBlock()
    {
        if (m_block_num && *m_block_num < (m_step_size - 1)) {
            // allocate next block within the step
            m_address += m_block_size;
            m_first_page_num += m_block_capacity;
            assert(m_address == m_header_size + m_first_page_num * m_page_size);
            m_page_count = 0;
            ++(*m_block_num);
        } else {
            // allocate the next step / block by appending it to the file            
            m_address = std::max(this->tail(), m_tail_function());
            m_first_page_num = getPageNum(m_address);
            assert(m_address == m_header_size + m_first_page_num * m_page_size);
            m_page_count = 0;
            // initiate the next full step
            m_block_num = 0;
        }
    }

    void Page_IO::collectReservePool(std::uint32_t page_count)
    {
        auto page_num = m_first_page_num + m_page_count;
        m_reserve_pool.add(page_num, page_count);
    }
    
    void Page_IO::read(std::uint64_t page_num, void *buffer) const {
        m_file.read(m_header_size + page_num * m_page_size, m_page_size, buffer);
    }

    void Page_IO::read(std::uint64_t page_num, void *buffer, std::uint32_t page_count) const {
        m_file.read(m_header_size + page_num * m_page_size, page_count * m_page_size, buffer);
    }

    void Page_IO::readPageOffset(std::uint64_t page_num, std::uint32_t offset, std::size_t size, void *buffer) const
    {
        assert(offset + size <= m_page_size);
        m_file.read(m_header_size + page_num * m_page_size + offset, size, buffer);
    }

    void Page_IO::write(std::uint64_t page_num, const void *buffer) {
        m_file.write(m_header_size + page_num * m_page_size, m_page_size, buffer);
    }

    void Page_IO::writePageOffset(std::uint64_t page_num, std::uint32_t offset, std::size_t size,
        const void *buffer)
    {
        assert(offset + size <= m_page_size);
        m_file.write(m_header_size + page_num * m_page_size + offset, size, buffer);
    }
    
    std::uint64_t Page_IO::getPageNum(std::uint64_t address) const {
        return (address - m_header_size) / m_page_size;
    }

    std::uint64_t Page_IO::tail() const
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (m_block_num) {
            // reserve space up to end of the step
            return m_address + (m_step_size - *m_block_num) * m_block_size;
        } else {
            // step not known, return end of the current block
            return m_address + m_block_size;
        }
    }
    
    std::uint32_t Page_IO::getPageSize() const {
        return m_page_size;        
    }
    
    std::pair<std::uint64_t, std::uint32_t> Page_IO::getNextPageNum(bool *is_first_page_ptr)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (!m_reserve_pool.empty()) {
            auto result = m_reserve_pool.next();
            if (is_first_page_ptr) {
                *is_first_page_ptr = isFirstPageInStep(result.first);
            }
            return result;
        }

        if (m_page_count == m_block_capacity) {
            allocateNextBlock();
        }
        if (is_first_page_ptr) {
            // first page of the first block in the step
            *is_first_page_ptr = isFirstPageInStep(m_first_page_num + m_page_count);
        }
        return { m_first_page_num + m_page_count, m_block_capacity - m_page_count };
    }
    
    std::uint64_t Page_IO::getEndPageNum(bool *is_first_page_ptr) const
    {
        assert(m_access_type == AccessType::READ_WRITE);
        if (is_first_page_ptr) {
            // first page of the first block in the step
            *is_first_page_ptr = isFirstPageInStep(m_first_page_num + m_page_count);
        }
        return m_first_page_num + m_page_count;
    }

    void Page_IO::setAtTail()
    {
        assert(m_access_type == AccessType::READ_WRITE);
        auto address = m_tail_function();
        if (address <= m_header_size) {
            address = m_header_size;
        } else {
            auto rel_address = address - m_header_size;
            auto rel_pages = (rel_address + m_page_size - 1) / m_page_size;
            address = m_header_size + rel_pages * m_page_size;
        }

        setAtPageNum(getPageNum(address));
    }

    void Page_IO::setAtPageNum(std::uint64_t page_num)
    {
        assert(m_access_type == AccessType::READ_WRITE);
        auto current_next_page_num = m_first_page_num + m_page_count;
        if (page_num <= current_next_page_num) {
            return;
        }

        auto block_id = (page_num * m_page_size) / m_block_size;
        m_address = m_header_size + block_id * m_block_size;
        m_page_count = static_cast<std::uint32_t>(page_num % m_block_capacity);
        if (m_page_count == 0) {
            m_address -= m_block_size;
            m_page_count = m_block_capacity;
            --block_id;
        }
        m_first_page_num = getPageNum(m_address);
        m_block_num = static_cast<std::uint32_t>(block_id % m_step_size);
    }
    
    Page_IO::StepIterator::StepIterator(const ExtSpace &ext_space)
        : m_next_it(ext_space.tryBegin())
    {
        if (m_next_it && !m_next_it->is_end()) {
            m_current_page_num = (**m_next_it).m_storage_page_num;
            m_current_rel_page_num = (**m_next_it).m_rel_page_num;
            ++(*m_next_it);
        }
    }

    bool Page_IO::StepIterator::operator!() const {
        return !m_next_it.get();
    }

    bool Page_IO::StepIterator::is_end() const {
        return !m_current_page_num.has_value();
    }

    std::uint64_t Page_IO::StepIterator::operator*() const {
        return *m_current_page_num;
    }

    Page_IO::StepIterator &Page_IO::StepIterator::operator++()
    {
        if (m_next_it && !m_next_it->is_end()) {
            m_current_page_num = (**m_next_it).m_storage_page_num;
            m_current_rel_page_num = (**m_next_it).m_rel_page_num;
            ++(*m_next_it);
        } else {
            m_current_page_num = std::nullopt;
            m_current_rel_page_num = std::nullopt;
        }
        return *this;
    }
    
    std::optional<std::size_t> Page_IO::StepIterator::tryGetStepPages() const
    {        
        if (m_next_it && !m_next_it->is_end()) {
            // step size may not be larger the the distance between the 2 consecutive ext-space entries
            // NOTE: the distance is measure between relative page numbers
            return (**m_next_it).m_rel_page_num - *m_current_rel_page_num;
        }
        return std::nullopt;
    }

    Page_IO::Reader::Reader(const Page_IO &page_io, const ExtSpace &ext_space,
        std::optional<std::uint64_t> end_page_num)
        : m_page_io(page_io)
        , m_step_it(ext_space)
        , m_end_page_num(std::min(end_page_num.value_or(std::numeric_limits<std::uint64_t>::max()), endPageNum()))
        , m_current_page_num(getFirstPageNum(ext_space))
    {
    }
    
    std::uint32_t Page_IO::Reader::next(std::vector<std::byte> &buf, std::uint64_t &start_page_num,
        std::size_t max_bytes)
    {
        std::size_t page_size = m_page_io.getPageSize();
        auto max_pages = max_bytes / page_size;
        if (buf.size() < max_pages * page_size) {
            buf.resize(max_pages * page_size);
        }

        start_page_num = m_current_page_num;
        auto to_read = std::min(std::uint64_t(max_pages), m_end_page_num - m_current_page_num);
        // align with the step size (if defined)
        if (!!m_step_it) {
            if (!m_step_it.is_end()) {
                auto step_pages = m_step_it.tryGetStepPages();
                if (step_pages) {
                    auto step_end_page = *m_step_it + *step_pages;
                    to_read = std::min(to_read, step_end_page - m_current_page_num);
                }
            }
        }
        
        if (to_read > 0) {
            m_page_io.read(m_current_page_num, buf.data(), static_cast<std::uint32_t>(to_read));
            m_current_page_num += to_read;
            // move on to the next step if end of the current step reached
            if (!!m_step_it) {
                auto step_pages = m_step_it.tryGetStepPages();
                if (step_pages) {
                    auto step_end_page = *m_step_it + *step_pages;
                    if (m_current_page_num >= step_end_page) {
                        ++m_step_it;
                        if (!m_step_it.is_end()) {
                            // position at the beginning of the next step
                            m_current_page_num = *m_step_it;
                        }
                    }
                }
            }
        }
        return to_read;
    }
    
    std::uint64_t Page_IO::Reader::endPageNum() const
    {
        // calculate end page number from actual file size
        m_page_io.m_file.refresh();
        auto file_size = m_page_io.m_file.size();
        if (file_size < m_page_io.m_header_size) {
            return 0;
        }
        return (file_size - m_page_io.m_header_size) / m_page_io.m_page_size;
    }
    
    std::uint64_t Page_IO::Reader::getFirstPageNum(const ExtSpace &ext_space) const
    {                
        if (!!ext_space) {
            auto it = ext_space.tryBegin();
            if (it && !it->is_end()) {
                return (**it).m_storage_page_num;
            }
        }
        return 0;        
    }
    
    void Page_IO::moveBy(std::uint32_t page_count)
    {
        if (!m_block_num) {
            THROWF(db0::InternalException) << "Page_IO::moveBy: step access not initialized";
        }

        auto old_block_num = *m_block_num;
        auto step_offset = old_block_num * m_block_capacity + m_page_count + page_count;
        auto step_capacity = m_step_size * m_block_capacity;
        if (step_offset > step_capacity) {
            THROWF(db0::InternalException) << "Page_IO::moveBy: attempt to move beyond the current step";
        }

        auto new_block_num = step_offset / m_block_capacity;
        auto new_page_count = step_offset % m_block_capacity;
        if (new_block_num == m_step_size) {
            new_block_num = m_step_size - 1;
            new_page_count = m_block_capacity;
        }

        auto block_diff = new_block_num - old_block_num;
        m_first_page_num += block_diff * m_block_capacity;
        m_address += block_diff * m_block_size;
        assert(m_address == m_header_size + m_first_page_num * m_page_size);
        m_block_num = new_block_num;
        m_page_count = new_page_count;
    }
    
    std::uint32_t Page_IO::getCurrentStepRemainingPages() const
    {
        if (!m_block_num) {
            THROWF(db0::InternalException) << "Page_IO::getCurrentStepRemainingPages: step access not initialized";
        }
        
        // end of the step reached
        if (*m_block_num >= m_step_size) {
            assert(*m_block_num == m_step_size);
            assert(m_page_count == 0);            
            return 0;
        }

        // current block excluding
        auto blocks_remaining = m_step_size - *m_block_num - 1;
        auto pages_remaining_in_block = m_block_capacity - m_page_count;
        return blocks_remaining * m_block_capacity + pages_remaining_in_block;
    }
    
}
