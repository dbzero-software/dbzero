// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "BaseStorage.hpp"

namespace db0
{

    BaseStorage::BaseStorage(AccessType access_type, StorageFlags flags)
        : m_access_type(access_type)
        , m_flags(flags)
    {
    }
    
    AccessType BaseStorage::getAccessType() const {
        return m_access_type;
    }
    
    void BaseStorage::getStats(std::function<void(const std::string &, std::uint64_t)>) const
    {
    }
    
    bool BaseStorage::beginRefresh() {
        return false;
    }
    
    std::uint64_t BaseStorage::completeRefresh(
        std::function<void(std::uint64_t updated_page_num, StateNumType state_num)>)
    {
        return 0;
    }
    
    std::uint64_t BaseStorage::refresh(
        std::function<void(std::uint64_t updated_page_num, StateNumType state_num)>)
    {
        if (beginRefresh()) {
            return completeRefresh();
        }
        return 0;
    }
    
    std::uint64_t BaseStorage::getLastUpdated() const {
        return 0;
    }

#ifndef NDEBUG
    void BaseStorage::getDRAM_IOMap(std::unordered_map<std::uint64_t, DRAM_PageInfo> &) const
    {
    }
    
    void BaseStorage::dramIOCheck(std::vector<DRAM_CheckResult> &) const
    {        
    }
#endif        

    void BaseStorage::beginCommit() {
    }
    
    void BaseStorage::endCommit() {  
    }
    
    void BaseStorage::fetchDP_ChangeLogs(StateNumType begin_state, std::optional<StateNumType> end_state,
        std::function<void(const DP_ChangeLogT &)> f) const
    {
        THROWF(db0::InternalException) << "Operation not supported: fetchChangeLog";
    }

    BDevStorage &BaseStorage::asFile() {
        THROWF(db0::InternalException) << "Storage is not file-based" << THROWF_END;
    }
    
}