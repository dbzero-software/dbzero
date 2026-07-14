// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MemoryStorage.hpp"
#include <cstring>

namespace db0

{
    
    MemoryStorage::MemoryPage::MemoryPage(std::size_t size)
        : m_buffer(size)
    {
    }
    
    MemoryStorage::MemoryStorage(std::size_t page_size)
        : m_page_size(page_size)
    {
    }
    
    void MemoryStorage::read(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
        FlagSet<AccessOptions> flags) const
    {
        auto begin_page = address / m_page_size;
        auto end_page = begin_page + size / m_page_size;

        std::byte *read_buf = reinterpret_cast<std::byte *> (buffer);
        for (auto page_num = begin_page; page_num != end_page; ++page_num, read_buf += m_page_size) {
            auto it = m_pages.find({ page_num, state_num });
            if (it != m_pages.end()) {
                std::memcpy(read_buf, it->second.m_buffer.data(), m_page_size);
            } else {
                if (flags[AccessOptions::read]) {
                    THROWF(db0::IOException) << "MemoryStorage::read: page not found: " << page_num << ", state: " << state_num;
                }                
                std::memset(read_buf, 0, m_page_size);
            }
        }
    }   

    void MemoryStorage::write(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer)
    {
        auto begin_page = address / m_page_size;
        auto end_page = begin_page + size / m_page_size;
        
        std::byte *write_buf = reinterpret_cast<std::byte *>(buffer);
        // write as physical pages and register with the sparse index
        for (auto page_num = begin_page; page_num != end_page; ++page_num, write_buf += m_page_size) {
            MemoryPage mem_page(m_page_size);
            std::memcpy(mem_page.m_buffer.data(), write_buf, m_page_size);
            m_pages[{ page_num, state_num }] = std::move(mem_page);
        }
    }
    
    bool MemoryStorage::tryFindMutation(std::uint64_t page_num, std::uint64_t state_num, std::uint64_t &mutation_id) const
    {
        if (m_pages.empty()) {
            return false;
        }
        auto it = m_pages.lower_bound({page_num, state_num});
        if (it == m_pages.end()) {
            --it;
        }
        if (it != m_pages.begin() && (it->first.second > state_num || it->first.first != page_num)) {
            --it;
        }
        if (it->first.first == page_num && it->first.second <= state_num) {
            mutation_id = it->first.second;
            return true;
        }
        return false;
    }

}