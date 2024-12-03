#include "BaseStorage.hpp"

namespace db0
{

    BaseStorage::BaseStorage(AccessType access_type)
        : m_access_type(access_type)
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
        std::function<void(std::uint64_t updated_page_num, std::uint64_t state_num)>)
    {
        return 0;
    }
    
    std::uint64_t BaseStorage::refresh(
        std::function<void(std::uint64_t updated_page_num, std::uint64_t state_num)>)
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

}