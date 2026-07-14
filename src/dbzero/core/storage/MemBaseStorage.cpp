// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "MemBaseStorage.hpp"
#include <cstring>

namespace db0

{
    
    MemBaseStorage::MemBaseStorage(std::size_t page_size)
        : BaseStorage(AccessType::READ_WRITE)
        , m_page_size(page_size)
        , m_temp_buf(page_size)
    {
    }
    
    void MemBaseStorage::read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        FlagSet<AccessOptions> access_mode) const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        assert(state_num >= m_max_state_num && "MemBaseStorage::writeDiffs: state number must be >= max state number");
        assert(state_num > 0 && "MemBaseStorage::read: state number must be > 0");
        assert((address % m_page_size == 0) && "MemBaseStorage::read: address must be page-aligned");
        assert((size % m_page_size == 0) && "MemBaseStorage::read: size must be page-aligned");

        auto begin_page = address / m_page_size;
        auto end_page = begin_page + size / m_page_size;
        
        std::byte *read_buf = reinterpret_cast<std::byte *>(buffer);
        for (auto page_num = begin_page; page_num != end_page; ++page_num, read_buf += m_page_size) {
            auto &dp_data = getDataPage(page_num, access_mode);
            if (dp_data.empty()) {
                if (access_mode[AccessOptions::read]) {
                    THROWF(db0::IOException) << "MemBaseStorage::read: page not found: " << page_num << ", state: " << state_num;
                }
                 // if requested access is write-only then simply fill the misssing (new) page with 0
                std::memset(read_buf, 0, m_page_size);
                continue;
            }
            std::memcpy(read_buf, dp_data.data(), m_page_size);            
        }
    }
    
    void MemBaseStorage::validateRead(std::uint64_t address, StateNumType state_num, std::size_t size,
        void *buffer, FlagSet<AccessOptions> access_mode) const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        // NOTE: validation only supported for the latest state
        if (state_num >= m_max_state_num) {            
            assert(state_num > 0 && "MemBaseStorage::read: state number must be > 0");
            assert((address % m_page_size == 0) && "MemBaseStorage::read: address must be page-aligned");
            assert((size % m_page_size == 0) && "MemBaseStorage::read: size must be page-aligned");
            
            auto begin_page = address / m_page_size;
            auto end_page = begin_page + size / m_page_size;
            
            std::byte *read_buf = reinterpret_cast<std::byte *>(buffer);
            for (auto page_num = begin_page; page_num != end_page; ++page_num, read_buf += m_page_size) {
                auto &dp_data = getDataPage(page_num, access_mode);
                if (dp_data.empty()) {
                    if (access_mode[AccessOptions::read]) {
                        THROWF(db0::IOException) << "MemBaseStorage::read: page not found: " << page_num << ", state: " << state_num;
                    }
                    // if requested access is write-only then simply fill the misssing (new) page with 0
                    std::memset(m_temp_buf.data(), 0, m_page_size);
                } else {
                    std::memcpy(m_temp_buf.data(), dp_data.data(), m_page_size);             
                }
                // validate what we've just read
                if (std::memcmp(m_temp_buf.data(), read_buf, m_page_size) != 0) {
                    assert(false && "MemBaseStorage::validateRead: data mismatch");
                    std::cout << "MemBaseStorage::validateRead: data mismatch at page: " << page_num << ", state: " << state_num << std::endl;
                    std::cout << "Expected data (hex): " << std::endl;
                    db0::showBytes(std::cout, (std::byte *)(m_temp_buf.data()), m_page_size) << std::endl;
                    std::cout << "Actual data (hex): " << std::endl;
                    db0::showBytes(std::cout, read_buf, m_page_size) << std::endl;
                    THROWF(db0::IOException) << "MemBaseStorage::validateRead: data mismatch at page: " << page_num << ", state: " << state_num;                
                }
            }
        }
    }
    
    void MemBaseStorage::write(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        assert(state_num >= m_max_state_num && "MemBaseStorage::writeDiffs: state number must be >= max state number");
        m_max_state_num = state_num;
        
        assert((address % m_page_size == 0) && "BDevStorage::write: address must be page-aligned");
        assert((size % m_page_size == 0) && "BDevStorage::write: size must be page-aligned");
                
        auto begin_page = address / m_page_size;
        auto end_page = begin_page + size / m_page_size;
        
        std::byte *write_buf = reinterpret_cast<std::byte *>(buffer);
        
        for (auto page_num = begin_page; page_num != end_page; ++page_num, write_buf += m_page_size) {
            auto &dp_data = getDataPage(page_num, { AccessOptions::write });
            std::memcpy(dp_data.data(), write_buf, m_page_size);
        }
    }
    
    bool MemBaseStorage::tryWriteDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        const std::vector<std::uint16_t> &diffs, unsigned int)
    {
        writeDiffs(address, state_num, size, buffer, diffs, 0);
        return true;
    }
    
    void MemBaseStorage::writeDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        const std::vector<std::uint16_t> &diffs, unsigned int)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        assert(state_num >= m_max_state_num && "MemBaseStorage::writeDiffs: state number must be >= max state number");
        m_max_state_num = state_num;
        
        assert((address % m_page_size == 0) && "BDevStorage::writeDiffs: address must be page-aligned");
        assert(size == m_page_size && "BDevStorage::writeDiffs: size must be equal to page size");
        
        auto page_num = address / m_page_size;
        auto &dp_data = getDataPage(page_num, { AccessOptions::write, AccessOptions::read, AccessOptions::create });
        assert(!dp_data.empty() && "MemBaseStorage::writeDiffs: data page must exist");
        if (dp_data.empty()) {
            THROWF(db0::IOException) << "MemBaseStorage::writeDiffs: page not found: " << page_num << ", state: " << state_num;
        }
        
        applyDiffs(diffs, buffer, dp_data.data(), dp_data.data() + m_page_size);
    }
    
    StateNumType MemBaseStorage::findMutation(std::uint64_t, StateNumType) const {
        THROWF(db0::InternalException) << "MemBaseStorage::findMutation: operation not supported" << THROWF_END;
    }
    
    bool MemBaseStorage::tryFindMutation(std::uint64_t, StateNumType, StateNumType &) const {
        THROWF(db0::InternalException) << "MemBaseStorage::findMutation: operation not supported" << THROWF_END;
    }
    
    std::size_t MemBaseStorage::getPageSize() const {
        return m_page_size;
    }

    std::vector<std::byte> &MemBaseStorage::getDataPage(std::uint64_t page_num, FlagSet<AccessOptions> access_flags) const
    {
        auto it = m_data_pages.find(page_num);
        if (it != m_data_pages.end()) {
            return it->second;
        }

        if (!access_flags[AccessOptions::read] || access_flags[AccessOptions::create]) {
            m_data_pages[page_num] = std::vector<std::byte>(m_page_size, std::byte{0});            
            return m_data_pages[page_num];
        }
        
        if (!access_flags[AccessOptions::write]) {
            return m_dp_null;
        }

        THROWF(db0::IOException) << "MemBaseStorage::getDataPage: page not found: " << page_num << THROWF_END;  
    }

    bool MemBaseStorage::flush(ProcessTimer *) {
        return true;
    }
    
    void MemBaseStorage::close() {
        m_data_pages.clear();
    }

}
