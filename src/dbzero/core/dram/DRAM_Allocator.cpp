// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "DRAM_Allocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <cassert>

namespace db0

{

    DRAM_Allocator::DRAM_Allocator(std::size_t page_size)
        : m_page_size(page_size)
    {
    }
    
    DRAM_Allocator::DRAM_Allocator(const std::unordered_set<std::size_t> &allocs, std::size_t page_size)
        : m_page_size(page_size)
    {
        update(allocs);
    }
    
    void DRAM_Allocator::update(const std::unordered_set<std::size_t> &allocs)
    {
        if (allocs.empty()) {
            return;
        }

        if (!m_free_pages.empty()) {
            THROWF(db0::InternalException) 
                << "DRAM_Allocator: update called on non-empty allocator" << THROWF_END;
        }

        std::uint64_t max_page_id = FIRST_PAGE_ID;
        for (auto addr: allocs) {
            if (addr % m_page_size != 0) {
                THROWF(db0::InternalException) << "DRAM_Allocator: invalid alloc address (" << addr << ")" << THROWF_END;
            }
            auto page_id = addr / m_page_size;
            for (;max_page_id <= page_id; ++max_page_id) {
                if ((max_page_id != page_id) && allocs.find(max_page_id * m_page_size) == allocs.end()) {
                    m_free_pages.insert(max_page_id);
                }                
            }
        }
        m_next_page_id = max_page_id;
    }
    
    std::optional<Address> DRAM_Allocator::tryAlloc(std::size_t size, std::uint32_t slot_num,
        bool aligned, unsigned char realm_id, unsigned char)
    {
        assert(slot_num == 0);
        assert(!aligned && "DRAM_Allocator: aligned allocation not supported");        
        assert(size == m_page_size && "DRAM_Allocator: invalid alloc size requested");
        if (m_free_pages.empty()) {
            return Address::fromOffset(m_next_page_id++ * m_page_size);
        }
        auto it = m_free_pages.begin();
        auto result = *it * m_page_size;
        m_free_pages.erase(it);
        return Address::fromOffset(result);
    }
    
    void DRAM_Allocator::free(Address address)
    {
        auto page_id = address / m_page_size;
        if (page_id >= m_next_page_id) {
            THROWF(db0::InternalException) << "DRAM_Allocator: invalid free address (" << address << ")" << THROWF_END;
        }
        if (address % m_page_size != 0) {
            // we don't free inner addresses
            return;            
        }        
        auto it = m_free_pages.find(page_id);
        if (it != m_free_pages.end()) {
            THROWF(db0::InternalException) << "DRAM_Allocator: double free (" << address << ")" << THROWF_END;
        }
        // the last page being removed
        if (page_id + 1 == m_next_page_id) {            
            --m_next_page_id;
            for (;m_next_page_id > 1;--m_next_page_id) {
                auto it = m_free_pages.find(m_next_page_id);
                if (it == m_free_pages.end()) {
                    break;
                }
                m_free_pages.erase(it);                
            }
            return;
        }        
        m_free_pages.insert(page_id);
    }

    std::size_t DRAM_Allocator::getAllocSize(Address address) const
    {
        // address validity not checked here
        auto offset = address % m_page_size;
        return m_page_size - offset;    
    }
    
    bool DRAM_Allocator::isAllocated(Address address, std::size_t *size_of_result) const
    {
        auto page_id = address / m_page_size;
        if (page_id >= m_next_page_id) {
            return false;
        }
        if (address % m_page_size != 0) {
            // we don't check inner addresses
            return false;
        }
        auto it = m_free_pages.find(page_id);
        if (it != m_free_pages.end()) {
            return false;
        }
        if (size_of_result) {
            auto offset = address % m_page_size;
            *size_of_result = m_page_size - offset;    
        }
        return true;
    }
    
    Address DRAM_Allocator::firstAlloc() const {
        return Address::fromOffset(FIRST_PAGE_ID * m_page_size);
    }
        
    void DRAM_Allocator::commit() const
    {
    }

    void DRAM_Allocator::detach() const
    {
    }

}