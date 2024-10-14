#include "PrefixName.hpp"

namespace db0

{

    const char *ltrim(const char *s) 
    {
        while (*s == '/' || *s == '\\') {
            ++s;
        }
        return s;
    }

    PrefixName::PrefixName(const char *name)
        : m_name(ltrim(name))
    {
    }

    PrefixName::PrefixName(const std::string &name)
        : PrefixName(name.c_str())
    {
    }

    PrefixName::operator const char *() const {
        return m_name.c_str();
    }

    PrefixName::operator const std::string &() const {
        return m_name;
    }

    const std::string &PrefixName::get() const {
        return m_name;
    }

}