#include "MemBaseStorage.hpp"
#include <cstring>

namespace db0

{
    
#ifndef NDEBUG

    MemBaseStorage::MemBaseStorage(std::size_t page_size)
        : BaseStorage(AccessType::READ_WRITE)
        , m_page_size(page_size)
    {
    }
    
    void MemBaseStorage::read(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        FlagSet<AccessOptions> access_mode) const
    {
        assert(state_num >= m_max_state_num && "MemBaseStorage::writeDiffs: state number must be >= max state number");
        assert(state_num > 0 && "BDevStorage::read: state number must be > 0");
        assert((address % m_page_size == 0) && "BDevStorage::read: address must be page-aligned");
        assert((size % m_page_size == 0) && "BDevStorage::read: size must be page-aligned");

        auto begin_page = address / m_page_size;
        auto end_page = begin_page + size / m_page_size;
        
        std::byte *read_buf = reinterpret_cast<std::byte *>(buffer);
        // lookup sparse index and read physical pages
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
    
    void MemBaseStorage::write(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer)
    {
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
    
    void MemBaseStorage::writeDiffs(std::uint64_t address, StateNumType state_num, std::size_t size, void *buffer,
        const std::vector<std::uint16_t> &diffs, unsigned int)
    {
        assert(state_num >= m_max_state_num && "MemBaseStorage::writeDiffs: state number must be >= max state number");
        m_max_state_num = state_num;
        
        assert((address % m_page_size == 0) && "BDevStorage::writeDiffs: address must be page-aligned");
        assert(size == m_page_size && "BDevStorage::writeDiffs: size must be equal to page size");
        
        auto page_num = address / m_page_size;
        auto &dp_data = getDataPage(page_num, { AccessOptions::write, AccessOptions::read, AccessOptions::create });
        assert(!dp_data.empty() && "MemBaseStorage::writeDiffs: data page must exist");
        
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
        if (page_num < m_data_pages.size()) {
            return m_data_pages[page_num];
        }

        if (!access_flags[AccessOptions::read] || access_flags[AccessOptions::create]) {
            while (page_num >= m_data_pages.size()) {
                m_data_pages.emplace_back(m_page_size, std::byte{0});
            }
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

#endif

}
