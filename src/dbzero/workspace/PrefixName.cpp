#include "PrefixName.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    const char *ltrim(const char *s)
    {
        if (!s) {
            return "";
        }
        while (*s == '/' || *s == '\\') {
            ++s;
        }
        return s;
    }
    
    PrefixName::PrefixName(const char *name)
        : m_name(ltrim(name))
        , m_has_value(m_name.length() > 0)
    {
        if (name && !m_has_value) {
            THROWF(db0::InternalException) << "Invalid prefix name: '" << name;
        }
    }

    PrefixName::PrefixName(const std::string &name)
        : PrefixName(name.c_str())
    {
    }
    
    PrefixName::operator const char *() const
    {
        if (!m_has_value) {
            THROWF(db0::InternalException) << "Invalid prefx name object accessed";
        }
        return m_name.c_str();
    }
    
    const char *PrefixName::c_str() const
    {
        if (!m_has_value) {
            THROWF(db0::InternalException) << "Invalid prefx name object accessed";
        }
        return m_name.c_str();
    }

    PrefixName::operator const std::string &() const
    {
        if (!m_has_value) {
            THROWF(db0::InternalException) << "Invalid prefx name object accessed";
        }
        return m_name;
    }

    bool PrefixName::isValid() const {
        return m_has_value;
    }

    const std::string &PrefixName::get() const
    {
        if (!m_has_value) {
            THROWF(db0::InternalException) << "Invalid prefx name object accessed";
        }
        return m_name;
    }

    bool PrefixName::operator!() const {
        return !m_has_value;    
    }

}