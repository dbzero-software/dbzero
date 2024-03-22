#include "mptr.hpp"
#include "Memspace.hpp"

namespace db0

{

    std::size_t mptr::getPageSize() const
    {        
        return m_memspace.get().getPageSize();
    }

}