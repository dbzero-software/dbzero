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
    
}