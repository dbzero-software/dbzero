#include "Config.hpp"

namespace db0

{
    
    Config::Config(ObjectPtr py_config)
        : m_py_config(py_config)
    {
    }

    const Config::ObjectSharedPtr& Config::getRawConfig() const
    {
        return m_py_config;
    }
    
    // long specialization
    template <> std::optional<long> get<long>(typename LangToolkit::ObjectPtr py_dict, const std::string &key)
    {
        if (!py_dict) {
            return std::nullopt;
        }
        return LangToolkit::getLong(py_dict, key);
    }

    // bool specialization
    template <> std::optional<bool> get<bool>(typename LangToolkit::ObjectPtr py_dict, const std::string &key)
    {
        if (!py_dict) {
            return std::nullopt;
        }
        return LangToolkit::getBool(py_dict, key);
    }

    // string specialization
    template <> std::optional<std::string> get<std::string>(typename LangToolkit::ObjectPtr py_dict, const std::string &key)
    {
        if (!py_dict) {
            return std::nullopt;
        }
        return LangToolkit::getString(py_dict, key);
    }

}