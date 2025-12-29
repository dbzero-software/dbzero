// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <iostream>
#include <cstring>
#include <algorithm>
#include <string_view>

namespace db0

{

#ifndef NDEBUG
    // cummulated size (in bytes) of all DRAM_Prefix instances
    std::size_t DRAM_Prefix::dp_size = 0;
    // total number of resource locks / data pages
    std::size_t DRAM_Prefix::dp_count = 0;
#endif

    DRAM_Prefix::DRAM_Prefix(std::size_t page_size)
        : Prefix("/sys/DRAM")
        , m_page_size(page_size)
        , m_dev_null(page_size)
        , m_dirty_cache(page_size)
        , m_context { m_dirty_cache, m_dev_null }
    {
    }
    
    DRAM_Prefix::~DRAM_Prefix() {
        close();
    }
    
    DRAM_Prefix::MemoryPage::MemoryPage(StorageContext context, std::uint64_t address, std::size_t size)
        : m_lock(std::make_shared<DP_Lock>(context, address, size, FlagSet<AccessOptions> {}, 0, 0))
        // pull from Storage0 temp instance
        , m_buffer(m_lock->getBuffer(address))
    {        
#ifndef NDEBUG
        dp_size += m_lock->size();
        ++dp_count;
#endif
    }
    
    DRAM_Prefix::MemoryPage::MemoryPage(const MemoryPage &dp)
        : m_lock(dp.m_lock)
        , m_buffer(dp.m_buffer)
    {
#ifndef NDEBUG
        dp_size += m_lock->size();
        ++dp_count;
#endif
    }
    
    DRAM_Prefix::MemoryPage::MemoryPage(MemoryPage &&dp)
        : m_lock(std::move(dp.m_lock))
        , m_buffer(dp.m_buffer)
    {
#ifndef NDEBUG
        dp.m_lock = nullptr;
#endif
    }

#ifndef NDEBUG
    DRAM_Prefix::MemoryPage::~MemoryPage()
    {
        if (m_lock) {
            dp_size -= m_lock->size();
            --dp_count;
        }
    }
#endif
    
    MemLock DRAM_Prefix::mapRange(std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode)
    {
        auto page_num = address / m_page_size;
        auto offset = address % m_page_size;
        if (size + offset > m_page_size) {
            THROWF(db0::InternalException) << "DRAM_Prefix: invalid range requested (@" << address 
                << ", size = " << size << ")" << THROWF_END;
        }
        auto it = m_pages.find(page_num);
        if (it == m_pages.end()) {
            it = m_pages.emplace(page_num, MemoryPage(m_context, address - offset, m_page_size)).first;
        } else if (access_mode[AccessOptions::write]) {
            it->second.m_lock->setDirty();
        }
        return { (std::byte*)it->second.m_buffer + offset, it->second.m_lock };
    }
    
    StateNumType DRAM_Prefix::getStateNum(bool) const {
        return 0;
    }
    
    std::uint64_t DRAM_Prefix::commit(ProcessTimer *) {
        return getStateNum(false);
    }
    
    std::size_t DRAM_Prefix::getPageSize() const {
        return m_page_size;
    }
    
    void DRAM_Prefix::flushDirty(SinkFunction sink) const {
        m_dirty_cache.flushDirty(sink);
    }
    
    void *DRAM_Prefix::update(std::size_t page_num, bool mark_dirty)
    {
        auto it = m_pages.find(page_num);
        if (it == m_pages.end()) {
            it = m_pages.emplace(page_num, MemoryPage(m_context, page_num * m_page_size, m_page_size)).first;
        }        
        if (mark_dirty) {
            it->second.m_lock->setDirty();
        }
        return it->second.m_buffer;
    }
    
    bool DRAM_Prefix::empty() const {
        return m_pages.empty();
    }
    
    void DRAM_Prefix::close(ProcessTimer *) 
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
            auto buf = update(page.first, false);
            std::memcpy(buf, page.second.m_buffer, m_page_size);
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
    
    std::uint64_t DRAM_Prefix::getLastUpdated() const {
        return 0;
    }
    
    std::shared_ptr<Prefix> DRAM_Prefix::getSnapshot(std::optional<StateNumType>) const {
        return const_cast<DRAM_Prefix *>(this)->shared_from_this();
    }
    
    BaseStorage &DRAM_Prefix::getStorage() const {
        return m_dev_null;
    }

    std::size_t DRAM_Prefix::size() const {
        return m_pages.size() * m_page_size;
    }
    
#ifndef NDEBUG
    std::pair<std::size_t, std::size_t> DRAM_Prefix::getTotalMemoryUsage() {
        return { DRAM_Prefix::dp_size, DRAM_Prefix::dp_count };
    }

    std::size_t DRAM_Prefix::getContentHash() const
    {
        std::vector<std::size_t> page_nums;
        for (auto &page: m_pages) {
            page_nums.push_back(page.first);
        }
        std::sort(page_nums.begin(), page_nums.end());
        std::size_t hash = 0;
        for (auto page_num: page_nums) {
            auto &page = m_pages.at(page_num);
            hash += std::hash<std::string_view>()(std::string_view((char*)page.m_buffer, m_page_size));
        }
        return hash;
    }    
#endif

    std::size_t DRAM_Prefix::getDirtySize() const
    {
        assert(false);
        throw std::runtime_error("DRAM_Prefix::getDirtySize operation not supported");
    }

    std::size_t DRAM_Prefix::flushDirty(std::size_t) 
    {
        assert(false);
        throw std::runtime_error("DRAM_Prefix::flushDirty operation not supported");
    }
    
}