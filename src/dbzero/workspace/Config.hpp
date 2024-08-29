#pragma once

#include <optional>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0

{

    using LangToolkit = db0::object_model::LangConfig::LangToolkit;
    template <typename T> std::optional<T> get(typename LangToolkit::ObjectPtr, const std::string &key);
    
    // Wraps a Python dict object and provides getters for configuration variables
    class Config
    {
    public:        
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        Config(ObjectPtr py_config);

        template <typename T> std::optional<T> get(const std::string &key) const {
            return db0::get<T>(m_py_config.get(), key);
        }

    private:
        ObjectSharedPtr m_py_config;
    };
    
}