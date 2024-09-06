#include "Prefix.hpp"

namespace db0

{

    Prefix::Prefix(std::string name)
        : m_name(std::move(name))    
    {
    }
    
    const std::string &Prefix::getName() const {
        return m_name;
    }
    
    void Prefix::beginAtomic() {
        THROWF(db0::InternalException) << "Atomic operations not supported by this prefix implementation" << THROWF_END;
    }

    void Prefix::endAtomic() {
        THROWF(db0::InternalException) << "Atomic operations not supported by this prefix implementation" << THROWF_END;        
    }

    void Prefix::cancelAtomic() {
        THROWF(db0::InternalException) << "Atomic operations not supported by this prefix implementation" << THROWF_END;        
    }

    void Prefix::cleanup() const
    {
    }

}