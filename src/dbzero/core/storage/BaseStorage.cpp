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

}