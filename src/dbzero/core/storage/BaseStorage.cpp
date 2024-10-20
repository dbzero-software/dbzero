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

#ifndef NDEBUG
    void BaseStorage::getDRAM_IOMap(std::unordered_map<std::uint64_t, DRAM_PageInfo> &) const
    {
    }
    
    void BaseStorage::dramIOCheck(std::vector<DRAM_CheckResult> &) const
    {        
    }
#endif        

}