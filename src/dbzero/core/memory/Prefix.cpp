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
    
}