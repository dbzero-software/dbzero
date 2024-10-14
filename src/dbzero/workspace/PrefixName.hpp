#pragma once

#include <string>

namespace db0

{

    // converts to a canonical prefix name
    class PrefixName
    {
    public:
        PrefixName(const char *name);
        PrefixName(const std::string &name);
        
        operator const char *() const;
        operator const std::string &() const;
        const std::string &get() const;
        
    private:
        const std::string m_name;
    };
    
}