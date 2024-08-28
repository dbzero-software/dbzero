#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <iostream>
#include <cstring>

namespace db0

{
    
    DRAM_Prefix::MemoryPage::MemoryPage(BaseStorage &storage, std::uint64_t address, std::size_t size)
        : m_lock(std::make_shared<DP_Lock>(storage, address, size,
            FlagSet<AccessOptions> { AccessOptions::write, AccessOptions::create }, 0, 0, true))
        // pull from Storage0 temp instance
        , m_buffer(m_lock->getBuffer(address))
    {
    }
    
    DRAM_Prefix::DRAM_Prefix(std::size_t page_size)
        : Prefix("/sys/DRAM")
        , m_page_size(page_size)
        , m_dev_null(page_size)        
    {
    }
    
    DRAM_Prefix::~DRAM_Prefix() {
        close();
    }
    
    MemLock DRAM_Prefix::mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions>)
    {
        auto page_num = address / m_page_size;
        auto offset = address % m_page_size;
        if (size + offset > m_page_size) {
            THROWF(db0::InternalException) << "DRAM_Prefix: invalid range requested" << THROWF_END;
        }
        auto it = m_pages.find(page_num);
        if (it == m_pages.end()) {
            it = m_pages.emplace(page_num, MemoryPage(m_dev_null, address - offset, m_page_size)).first;
        }
        return { (std::byte*)it->second.m_buffer + offset, it->second.m_lock };
    }

    std::uint64_t DRAM_Prefix::getStateNum() const {
        return 0;
    }
    
    std::uint64_t DRAM_Prefix::commit() {
        return getStateNum();
    }
        
    std::size_t DRAM_Prefix::getPageSize() const {
        return m_page_size;
    }

    void DRAM_Prefix::flushDirty(SinkFunction sink) const
    {
        for (auto &page : m_pages) {
            if (page.second.m_lock->resetDirtyFlag()) {
                sink(page.first, page.second.m_buffer);
            }
        }
    }
    
    void DRAM_Prefix::update(std::size_t page_num, const void *bytes, bool mark_dirty)
    {
        auto it = m_pages.find(page_num);
        if (it == m_pages.end()) {
            it = m_pages.emplace(page_num, MemoryPage(m_dev_null, page_num * m_page_size, m_page_size)).first;
        }
        std::memcpy(it->second.m_buffer, bytes, m_page_size);
        if (mark_dirty) {
            it->second.m_lock->setDirty();
        }
    }
    
    bool DRAM_Prefix::empty() const {
        return m_pages.empty();
    }
    
    void DRAM_Prefix::close() 
    {
        for (auto &page: m_pages) {
            page.second.resetDirtyFlag();
        }
        m_pages.clear();
    }
    
    void DRAM_Prefix::MemoryPage::resetDirtyFlag() {
        m_lock->resetDirtyFlag();
    }
    
    void DRAM_Prefix::operator=(const DRAM_Prefix &other)
    {
        if (m_page_size != other.m_page_size) {
            THROWF(db0::InternalException) << "DRAM_Prefix: page size mismatch" << THROWF_END;
        }
        // update binary contents
        for (auto &page: other.m_pages) {
            update(page.first, page.second.m_buffer, false);
        }
        
        // remove pages not existing in the other prefix
        for (auto it = m_pages.begin(); it != m_pages.end();) {
            if (other.m_pages.find(it->first) == other.m_pages.end()) {
                it = m_pages.erase(it);
            } else {
                ++it;
            }
        }        
    }
    
    std::uint64_t DRAM_Prefix::refresh() {
        return 0;
    }

    std::uint64_t DRAM_Prefix::getLastUpdated() const {
        return 0;
    }
    
    std::shared_ptr<Prefix> DRAM_Prefix::getSnapshot(std::optional<std::uint64_t>) const {
        return const_cast<DRAM_Prefix *>(this)->shared_from_this();
    }
    
    BaseStorage &DRAM_Prefix::getStorage() const {
        return m_dev_null;
    }
    
}