#pragma once

#include <string>

namespace db0

{

    // converts to a canonical prefix name
    class PrefixName
    {
    public:
        PrefixName() = default;
        PrefixName(const char *name);
        PrefixName(const std::string &name);
        
        operator const char *() const;
        operator const std::string &() const;
        const std::string &get() const;
        const char *c_str() const;
        
        bool isValid() const;
        bool operator!() const;

    private:
        std::string m_name;  
        bool m_has_value = false;
    };
    
}